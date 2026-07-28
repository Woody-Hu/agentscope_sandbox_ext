# -*- coding: utf-8 -*-
"""KataWorkspace — :class:`DockerWorkspace` forced onto a Kata
Containers Docker runtime.

Kata Containers (https://katacontainers.io) runs each container
inside a lightweight VM backed by a hardware hypervisor (Firecracker
or QEMU).  From Docker's perspective it is *just another runtime* —
the same image, the same ``docker run`` flags, the same container
API.  This backend therefore inherits the entire
:class:`DockerWorkspace` lifecycle (image build, bind-mount, gateway
bootstrap, teardown) and only overrides
:meth:`_create_and_start_container` to inject ``HostConfig.Runtime``.

The result is a workspace whose in-container syscalls are mediated by
a guest kernel, giving the agent VM-grade isolation (separate kernel,
hardware-enforced memory boundaries) while keeping the container
ergonomics the agentscope gateway expects.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

from agentscope._logging import logger
from agentscope.mcp import MCPClient
from agentscope.workspace import DockerWorkspace
from agentscope.workspace._docker._make_dockerfile import (
    CONTAINER_WORKDIR,
    DEFAULT_BASE_IMAGE,
    DEFAULT_GATEWAY_PORT,
    GATEWAY_HOME,
)

from .._base import SandboxedWorkspaceExtBase
from ._constants import KATA_DEFAULT_RUNTIME_NAME, KATA_RUNTIME_CANDIDATES


class KataWorkspace(SandboxedWorkspaceExtBase, DockerWorkspace):
    """Workspace backed by a Docker container running under Kata.

    Inherits the full :class:`DockerWorkspace` implementation and
    overrides :meth:`_create_and_start_container` to set
    ``HostConfig.Runtime`` to a Kata runtime name.

    See :class:`agentscope_sandbox_ext._gvisor._workspace.GVisorWorkspace`
    for the multiple-inheritance rationale — Kata follows the exact
    same pattern with a different runtime name.
    """

    sandbox_kind = "kata"
    _gateway_home = GATEWAY_HOME

    def __init__(
        self,
        *,
        workspace_id: str | None = None,
        base_image: str = DEFAULT_BASE_IMAGE,
        host_workdir: str | None = None,
        node_version: str | None = None,
        extra_pip: list[str] | None = None,
        gateway_port: int = DEFAULT_GATEWAY_PORT,
        env: dict[str, str] | None = None,
        default_mcps: list[MCPClient] | None = None,
        skill_paths: list[str] | None = None,
        runtime: str = KATA_DEFAULT_RUNTIME_NAME,
    ) -> None:
        """Construct a :class:`KataWorkspace`.

        Args:
            workspace_id (`str | None`, optional):
                Existing workspace identifier to adopt.
            base_image (`str`, defaults to `DEFAULT_BASE_IMAGE`):
                Base Docker image; must provide ``python3``.
            host_workdir (`str | None`, optional):
                Host directory bind-mounted to ``/workspace``.
            node_version (`str | None`, optional):
                Major Node.js version baked into the image.
            extra_pip (`list[str] | None`, optional):
                Extra Python packages installed into the gateway venv.
            gateway_port (`int`, defaults to `DEFAULT_GATEWAY_PORT`):
                TCP port the gateway listens on inside the container.
            env (`dict[str, str] | None`, optional):
                Environment variables set inside the container.
            default_mcps (`list[MCPClient] | None`, optional):
                MCPs registered on first init.
            skill_paths (`list[str] | None`, optional):
                Skill directories seeded into ``skills/`` on first init.
            runtime (`str`, defaults to :data:`KATA_DEFAULT_RUNTIME_NAME`):
                Docker runtime name.  Override only if your daemon
                registers Kata under a different key (e.g.
                ``kata-qemu``).
        """
        DockerWorkspace.__init__(
            self,
            workspace_id=workspace_id,
            base_image=base_image,
            host_workdir=host_workdir,
            node_version=node_version,
            extra_pip=extra_pip,
            gateway_port=gateway_port,
            env=env,
            default_mcps=default_mcps,
            skill_paths=skill_paths,
        )
        self._runtime = runtime

    # ── runtime probe ─────────────────────────────────────────────

    @classmethod
    async def verify_runtime_available(cls) -> None:
        """Raise :class:`RuntimeError` if no Kata runtime is registered.

        Probes ``docker info --format '{{json .Runtimes}}'`` and checks
        the parsed JSON for any of :data:`KATA_RUNTIME_CANDIDATES`.
        """
        probe = await asyncio.create_subprocess_exec(
            "docker",
            "info",
            "--format",
            "{{json .Runtimes}}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                probe.communicate(),
                timeout=10.0,
            )
        except asyncio.TimeoutError:
            probe.kill()
            await probe.communicate()
            raise RuntimeError(
                "'docker info' did not respond within 10s — is the "
                "Docker daemon running?",
            )
        if probe.returncode != 0:
            raise RuntimeError(
                f"'docker info' failed (exit {probe.returncode}): "
                f"{stderr.decode('utf-8', 'replace').strip()}",
            )
        import json

        try:
            runtimes = json.loads(stdout.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"could not parse 'docker info' runtime list: {exc}",
            ) from exc
        if not isinstance(runtimes, dict):
            raise RuntimeError(
                f"unexpected 'docker info' runtimes payload: {runtimes!r}",
            )
        present = [name for name in KATA_RUNTIME_CANDIDATES if name in runtimes]
        if not present:
            raise RuntimeError(
                "No Kata Containers runtime registered with Docker. "
                f"Expected one of {list(KATA_RUNTIME_CANDIDATES)}; found "
                f"{list(runtimes)}. Install Kata and register a runtime "
                "in /etc/docker/daemon.json. See "
                "https://katacontainers.io/docs/install/",
            )

    # ── overridden container creation ────────────────────────────

    async def _create_and_start_container(self) -> None:
        """Create + start the container under the Kata runtime.

        Mirrors :meth:`DockerWorkspace._create_and_start_container`
        exactly except that ``HostConfig.Runtime`` is set to
        :attr:`_runtime`.
        """
        config: dict[str, Any] = {
            "Image": self._image_tag,
            "Cmd": ["sleep", "infinity"],
            "WorkingDir": CONTAINER_WORKDIR,
            "Labels": {
                "agentscope.workspace": "true",
                "agentscope.workspace.id": self.workspace_id,
                "agentscope.sandbox_ext.kind": "kata",
            },
        }
        if self.env:
            config["Env"] = [f"{k}={v}" for k, v in self.env.items()]

        host_config: dict[str, Any] = {"Runtime": self._runtime}
        if self.host_workdir is not None:
            os.makedirs(self.host_workdir, exist_ok=True)
            host_config["Binds"] = [
                f"{os.path.abspath(self.host_workdir)}:{CONTAINER_WORKDIR}:rw",
            ]
        config["HostConfig"] = host_config

        logger.info(
            "KataWorkspace: creating container %s under runtime %r",
            self.workspace_id,
            self._runtime,
        )
        self._container = await self._client.containers.create_or_replace(
            name=f"as_ws_kata_{self.workspace_id}",
            config=config,
        )
        await self._container.start()

        from agentscope.workspace._docker._docker_backend import DockerBackend

        self._backend = DockerBackend(self._container, CONTAINER_WORKDIR)

    # ── metrics ───────────────────────────────────────────────────

    async def metrics(self) -> dict[str, Any]:
        """Return Kata-specific observability fields."""
        base = await super().metrics()
        base.update(
            {
                "runtime": self._runtime,
                "container_id": (
                    getattr(self._container, "_id", None)
                    if self._container is not None
                    else None
                ),
            },
        )
        return base

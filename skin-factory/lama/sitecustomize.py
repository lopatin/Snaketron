"""Deny outbound network access inside the isolated LaMa forge process."""

from __future__ import annotations

import os
import socket


def _offline(*_args, **_kwargs):
    raise PermissionError("network access is disabled in the Snaketron LaMa forge")


if os.environ.get("SNAKETRON_LAMA_OFFLINE") != "1":
    raise RuntimeError("the Snaketron LaMa runtime must be launched in offline mode")

OFFLINE_GUARD_INSTALLED = True
socket.create_connection = _offline
socket.getaddrinfo = _offline
socket.socket.bind = _offline
socket.socket.connect = _offline
socket.socket.connect_ex = _offline
socket.socket.sendto = _offline
if hasattr(socket.socket, "sendmsg"):
    socket.socket.sendmsg = _offline

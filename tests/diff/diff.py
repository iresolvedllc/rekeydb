#!/usr/bin/env python3
"""Compare replies from two RESP servers.

This is a bootstrap differential harness. The full conformance suite is the
KeyDB Tcl suite; this catches command-level divergence quickly while the Tcl
adapter is being built.

Usage:
    ./diff.py --a-port 6379 --b-port 6380 SET k v
    ./diff.py --a-port 6379 --b-port 6380 --trace traces/core.txt

Trace format: one command per line, split with shell rules. Comments start
with '#'.
"""

from __future__ import annotations

import argparse
import shlex
import socket
import sys


def encode(args: list[str]) -> bytes:
    out = [f"*{len(args)}\r\n".encode()]
    for arg in args:
        data = arg.encode() if isinstance(arg, str) else arg
        out.append(f"${len(data)}\r\n".encode())
        out.append(data)
        out.append(b"\r\n")
    return b"".join(out)


def read_line(stream) -> bytes:
    line = stream.readline()
    if not line.endswith(b"\r\n"):
        raise EOFError("connection closed while reading a line")
    return line


def read_reply(stream) -> bytes:
    prefix = stream.read(1)
    if not prefix:
        raise EOFError("connection closed")
    if prefix in b"+-:,#(_":
        return prefix + read_line(stream)
    if prefix in b"$=":
        header = read_line(stream)
        length = int(header[:-2])
        if length < 0:
            return prefix + header
        data = stream.read(length + 2)
        if len(data) != length + 2:
            raise EOFError("short bulk string")
        return prefix + header + data
    if prefix in b"*~>":
        header = read_line(stream)
        count = int(header[:-2])
        if count < 0:
            return prefix + header
        return prefix + header + b"".join(read_reply(stream) for _ in range(count))
    if prefix == b"%":
        header = read_line(stream)
        count = int(header[:-2])
        return prefix + header + b"".join(
            read_reply(stream) + read_reply(stream) for _ in range(count)
        )
    raise ValueError(f"unknown RESP prefix {prefix!r}")


def connect(port: int) -> socket.socket:
    sock = socket.create_connection(("127.0.0.1", port), timeout=5)
    sock.settimeout(5)
    return sock


def show(data: bytes) -> str:
    return data.decode("utf-8", errors="backslashreplace").replace("\r\n", "\\r\\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--a-port", type=int, required=True)
    parser.add_argument("--b-port", type=int, required=True)
    parser.add_argument("--trace")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    commands: list[list[str]] = []
    if args.trace:
        with open(args.trace, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.split("#", 1)[0].strip()
                if line:
                    commands.append(shlex.split(line))
    if args.command:
        command = args.command
        if command and command[0] == "--":
            command = command[1:]
        if command:
            commands.append(command)
    if not commands:
        parser.error("provide --trace or a command")

    a = connect(args.a_port)
    b = connect(args.b_port)
    failures = 0
    for command in commands:
        payload = encode(command)
        a.sendall(payload)
        b.sendall(payload)
        reply_a = read_reply(a.makefile("rb", buffering=0))
        reply_b = read_reply(b.makefile("rb", buffering=0))
        printable = " ".join(shlex.quote(part) for part in command)
        if reply_a == reply_b:
            print(f"ok   {printable}")
        else:
            failures += 1
            print(f"DIFF {printable}")
            print(f"  a: {show(reply_a)}")
            print(f"  b: {show(reply_b)}")
    a.close()
    b.close()
    if failures:
        print(f"{failures} divergence(s)", file=sys.stderr)
        return 1
    print(f"{len(commands)} command(s) matched")
    return 0


if __name__ == "__main__":
    sys.exit(main())

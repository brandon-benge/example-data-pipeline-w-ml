#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from platform_stacks import STACKS


ROOT = Path(__file__).resolve().parents[1]


def run_compose(args: list[str]) -> int:
    completed = subprocess.run(["docker", "compose", *args], cwd=ROOT)
    return completed.returncode


def stack_services(stack: str) -> list[str]:
    return list(STACKS[stack].services)


def is_bootstrap_service(service: str) -> bool:
    return "-bootstrap" in service or service == "ml-training"


def stack_bootstrap_services(stack: str) -> list[str]:
    return [service for service in stack_services(stack) if is_bootstrap_service(service)]


def stack_runtime_services(stack: str) -> list[str]:
    return [service for service in stack_services(stack) if not is_bootstrap_service(service)]


def stack_stoppable_runtime_services(stack: str) -> list[str]:
    preserved = set(STACKS[stack].stop_preserve_services)
    return [service for service in stack_runtime_services(stack) if service not in preserved]


def stack_stoppable_bootstrap_services(stack: str) -> list[str]:
    preserved = set(STACKS[stack].stop_preserve_services)
    return [service for service in stack_bootstrap_services(stack) if service not in preserved]


def cmd_list() -> int:
    for stack in STACKS.values():
        print(f"{stack.name}: {stack.description}")
        print(f"  services: {', '.join(stack.services)}")
    return 0


def cmd_up(stack: str) -> int:
    runtime_services = stack_runtime_services(stack)
    bootstrap_services = stack_bootstrap_services(stack)

    up_code = 0
    if runtime_services:
        up_code = run_compose(["up", "-d", *runtime_services])

    recreate_code = 0
    if bootstrap_services:
        recreate_code = run_compose(["up", "-d", "--force-recreate", *bootstrap_services])

    return up_code or recreate_code


def cmd_stop(stack: str) -> int:
    runtime_services = stack_stoppable_runtime_services(stack)
    bootstrap_services = stack_stoppable_bootstrap_services(stack)

    stop_code = 0
    if runtime_services:
        stop_code = run_compose(["stop", *runtime_services])

    rm_code = 0
    if bootstrap_services:
        rm_code = run_compose(["rm", "-f", *bootstrap_services])

    return stop_code or rm_code


def cmd_ps(stack: str, *, include_all: bool) -> int:
    args = ["ps"]
    if include_all:
        args.append("-a")
    args.extend(stack_services(stack))
    return run_compose(args)


def cmd_logs(stack: str, *, follow: bool) -> int:
    args = ["logs"]
    if follow:
        args.append("-f")
    args.extend(stack_services(stack))
    return run_compose(args)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start or stop a logical platform stack without bringing up the full platform.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("list", help="List the available logical stacks.")

    for command in ("up", "stop", "ps"):
        subparser = subparsers.add_parser(command, help=f"{command} a logical stack")
        subparser.add_argument("stack", choices=sorted(STACKS))
        if command == "ps":
            subparser.add_argument("-a", "--all", action="store_true", help="Show exited one-shot bootstrap containers too.")

    logs = subparsers.add_parser("logs", help="Show logs for all services in a logical stack")
    logs.add_argument("stack", choices=sorted(STACKS))
    logs.add_argument("-f", "--follow", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "list":
        return cmd_list()
    if args.command == "up":
        return cmd_up(args.stack)
    if args.command == "stop":
        return cmd_stop(args.stack)
    if args.command == "ps":
        return cmd_ps(args.stack, include_all=args.all)
    if args.command == "logs":
        return cmd_logs(args.stack, follow=args.follow)
    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    sys.exit(main())

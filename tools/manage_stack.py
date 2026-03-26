#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from platform_stacks import (
    STACKS,
    SERVICE_RESOURCE_KIND,
    WORKLOAD_NAMESPACES,
    canonical_namespace_for_service,
    namespace_candidates_for_service,
)


ROOT = Path(__file__).resolve().parents[1]
K8S_MANIFEST = ROOT / "k8s" / "platform.yaml"
K8S_SECRETS_MANIFEST = ROOT / "k8s" / "platform-secrets.example.yaml"
PVC_NAMESPACE_BY_NAME = {
    "postgres-data": WORKLOAD_NAMESPACES["infra"],
    "kafka-data": WORKLOAD_NAMESPACES["infra"],
    "minio-data": WORKLOAD_NAMESPACES["infra"],
    "superset-home": WORKLOAD_NAMESPACES["serve"],
    "kafka-connect-plugin-cache": WORKLOAD_NAMESPACES["ingest"],
}
BOOTSTRAP_JOB_NAMESPACE_BY_NAME = {
    "kafka-bootstrap": WORKLOAD_NAMESPACES["infra"],
    "schema-registry-bootstrap": WORKLOAD_NAMESPACES["infra"],
    "kafka-connect-source-bootstrap": WORKLOAD_NAMESPACES["ingest"],
    "minio-bootstrap": WORKLOAD_NAMESPACES["infra"],
    "iceberg-cdc-bootstrap": WORKLOAD_NAMESPACES["infra"],
    "kafka-connect-sinks-bootstrap": WORKLOAD_NAMESPACES["ingest"],
    "flink-bootstrap-bronze-events": WORKLOAD_NAMESPACES["process"],
    "flink-bootstrap-online-features": WORKLOAD_NAMESPACES["process"],
    "superset-bootstrap": WORKLOAD_NAMESPACES["serve"],
}


def run_kubectl(args: list[str]) -> int:
    completed = subprocess.run(["kubectl", *args], cwd=ROOT)
    return completed.returncode


def kubectl_output(args: list[str]) -> str:
    completed = subprocess.run(
        ["kubectl", *args],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def kubectl_success(args: list[str]) -> bool:
    completed = subprocess.run(
        ["kubectl", *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    return completed.returncode == 0


def terminating_workload_namespaces() -> list[str]:
    terminating: list[str] = []
    for namespace in WORKLOAD_NAMESPACES.values():
        completed = subprocess.run(
            [
                "kubectl",
                "get",
                "namespace",
                namespace,
                "-o",
                r'jsonpath={.status.phase}{"|"}{.metadata.deletionTimestamp}',
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            continue
        phase, _, deletion_timestamp = completed.stdout.strip().partition("|")
        if phase == "Terminating" or deletion_timestamp:
            terminating.append(namespace)
    return terminating


def resolve_service_namespace(service: str) -> str:
    kind = SERVICE_RESOURCE_KIND.get(service, "service")
    for namespace in namespace_candidates_for_service(service):
        if kubectl_success(["get", kind, service, "-n", namespace]):
            return namespace
    return canonical_namespace_for_service(service)


def stack_services(stack: str) -> list[str]:
    return list(STACKS[stack].services)


def is_bootstrap_service(service: str) -> bool:
    return "-bootstrap" in service


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
    terminating = terminating_workload_namespaces()
    if terminating:
        print("Cannot apply the platform while workload namespaces are terminating:")
        for namespace in terminating:
            print(f"  - {namespace}")
        print("Wait for namespace deletion to finish, then retry.")
        return 1

    exit_code = 0
    for namespace in WORKLOAD_NAMESPACES.values():
        print(f"Ensuring namespace '{namespace}' exists.")
        result = subprocess.run(
            [
                "/bin/zsh",
                "-lc",
                f"kubectl create namespace {namespace} --dry-run=client -o yaml | kubectl apply -f -",
            ],
            cwd=ROOT,
        ).returncode
        if result != 0:
            exit_code = result

    print(f"Applying Kubernetes secrets manifest for logical stack '{stack}'.")
    result = run_kubectl(["apply", "-f", str(K8S_SECRETS_MANIFEST)])
    if result != 0:
        exit_code = result

    # Jobs have immutable pod templates, so recreate bootstrap jobs before apply.
    for job_name, namespace in BOOTSTRAP_JOB_NAMESPACE_BY_NAME.items():
        print(f"Recreating bootstrap job template for job/{job_name} in namespace {namespace}.")
        result = run_kubectl(
            ["delete", "job", job_name, "-n", namespace, "--ignore-not-found=true"]
        )
        if result != 0:
            exit_code = result

    print(f"Applying Kubernetes platform manifest for logical stack '{stack}'.")
    result = run_kubectl(["apply", "-f", str(K8S_MANIFEST)])
    if result != 0:
        exit_code = result
    return exit_code


def cmd_stop(stack: str) -> int:
    services = stack_stoppable_runtime_services(stack) + stack_stoppable_bootstrap_services(stack)
    if not services:
        print(f"No removable services defined for stack '{stack}'.")
        return 0

    print(f"Stopping Kubernetes resources for logical stack '{stack}'.")
    exit_code = 0
    for service in services:
        kind = SERVICE_RESOURCE_KIND.get(service)
        if kind is None:
            print(f"Skipping unknown service kind for '{service}'.")
            continue
        namespace = resolve_service_namespace(service)
        result = run_kubectl(["delete", kind, service, "-n", namespace, "--ignore-not-found=true"])
        if result != 0:
            exit_code = result
    return exit_code


def cmd_destroy_all() -> int:
    print("Destroying the full Kubernetes platform, including PVCs.")
    exit_code = 0

    # Delete PVCs explicitly before namespace teardown so storage cleanup can start immediately.
    for pvc_name, namespace in PVC_NAMESPACE_BY_NAME.items():
        print(f"Deleting pvc/{pvc_name} in namespace {namespace}...")
        result = run_kubectl(["delete", "pvc", pvc_name, "-n", namespace, "--ignore-not-found=true", "--wait=false"])
        if result != 0:
            exit_code = result

    for manifest in (K8S_SECRETS_MANIFEST, K8S_MANIFEST):
        print(f"Deleting resources from {manifest.name}...")
        result = run_kubectl(["delete", "-f", str(manifest), "--ignore-not-found=true", "--wait=false"])
        if result != 0:
            exit_code = result

    print("Deletion has been requested. Kubernetes may continue terminating namespaces and volumes in the background.")
    return exit_code


def cmd_ps(stack: str, *, include_all: bool) -> int:
    exit_code = 0
    found = False
    for service in stack_services(stack):
        kind = SERVICE_RESOURCE_KIND.get(service)
        if kind is None:
            continue
        namespace = resolve_service_namespace(service)
        print(f"===== {namespace} {kind}/{service} =====")
        args = ["get", kind, service, "-n", namespace]
        if include_all:
            args.extend(["-o", "wide"])
        result = run_kubectl(args)
        if result == 0:
            found = True
        else:
            exit_code = result
        print()

    if not found and exit_code == 0:
        print(f"No resources defined for stack '{stack}'.")
        return 1
    return exit_code


def cmd_logs(stack: str, *, follow: bool) -> int:
    targets: list[tuple[str, str, str]] = []
    for service in stack_services(stack):
        kind = SERVICE_RESOURCE_KIND.get(service)
        namespace = resolve_service_namespace(service)
        if kind in {"deployment", "statefulset"}:
            try:
                pod = kubectl_output(
                    [
                        "get",
                        "pods",
                        "-n",
                        namespace,
                        "-l",
                        f"app.kubernetes.io/name={service}",
                        "-o",
                        "jsonpath={.items[0].metadata.name}",
                    ]
                )
            except subprocess.CalledProcessError:
                pod = ""
            if pod:
                targets.append((namespace, "pod", pod))
        elif kind == "job":
            targets.append((namespace, "job", service))
        elif kind == "cronjob":
            targets.append((namespace, "cronjob", service))

    if not targets:
        print(f"No loggable resources found for stack '{stack}'.")
        return 1

    if follow:
        namespace, target_kind, target_name = targets[0]
        if target_kind == "cronjob":
            print(f"Cannot stream logs directly from cronjob/{target_name}; inspect a spawned job instead.")
            return 1
        return run_kubectl(["logs", "-n", namespace, "-f", f"{target_kind}/{target_name}"])

    exit_code = 0
    for namespace, target_kind, target_name in targets:
        print(f"===== logs: {namespace} {target_kind}/{target_name} =====")
        if target_kind == "cronjob":
            print("CronJob does not hold logs directly; inspect a spawned job instead.\n")
            exit_code = 1
            continue
        result = run_kubectl(["logs", "-n", namespace, f"{target_kind}/{target_name}"])
        if result != 0:
            exit_code = result
        print()
    return exit_code


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start or stop a logical platform stack without bringing up the full platform.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("list", help="List the available logical stacks.")
    subparsers.add_parser("destroy", help="Delete the full platform, including namespaces and PVCs.")

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
    if args.command == "destroy":
        return cmd_destroy_all()
    if args.command == "ps":
        return cmd_ps(args.stack, include_all=args.all)
    if args.command == "logs":
        return cmd_logs(args.stack, follow=args.follow)
    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    sys.exit(main())

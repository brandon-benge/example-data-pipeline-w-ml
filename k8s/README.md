# Kubernetes Deployment

This repo now targets DigitalOcean Kubernetes (`kubectl`) instead of `docker compose` for platform orchestration.

## Assumptions

- You already have `kubectl` pointed at your DOKS cluster.
- The branch or tag referenced in [platform.yaml](./platform.yaml) is reachable from the cluster at runtime.
- This is still the same single-node demo architecture, just packaged as Kubernetes workloads.

## Images

All workloads now use upstream/public images directly.

Repo-owned logic is injected at runtime by:

- cloning the repo declared in `platform-config`
- mounting repo files into pods
- building the Kafka Connect Iceberg plugin in an init container
- downloading Flink and Spark runtime jars in init containers
- installing Python dependencies for Flink jobs in init containers

## Repo Source

The manifest clones the repo at pod startup using:

- `GIT_REPO_URL`
- `GIT_REPO_REF`

Those are currently set in [platform.yaml](./platform.yaml). If you need the cluster to run a different branch or fork, change those values first.

## Secrets

Create the workload namespaces and shared secrets first:

```bash
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: Namespace
metadata:
  name: data-platform-infra
---
apiVersion: v1
kind: Namespace
metadata:
  name: data-platform-ingest
---
apiVersion: v1
kind: Namespace
metadata:
  name: data-platform-process
---
apiVersion: v1
kind: Namespace
metadata:
  name: data-platform-serve
---
apiVersion: v1
kind: Namespace
metadata:
  name: data-platform-govern
EOF

kubectl apply -f k8s/platform-secrets.example.yaml
```

Replace the example values before you do that.

## Deploy

```bash
kubectl apply -f k8s/platform.yaml
```

## Access

When you use `bash tools/run_stack_workflow.sh --stop-at <stage>`, the workflow establishes these workstation port-forwards automatically after the platform is applied. Run them manually only when you are not using the staged workflow.

Use port-forwarding from your workstation for the primary interfaces:

```bash
kubectl -n data-platform-serve port-forward svc/trino 8080:8080
kubectl -n data-platform-process port-forward svc/flink-jobmanager 8082:8081
kubectl -n data-platform-serve port-forward svc/superset 8088:8088
```

Optional debugging endpoints:

```bash
kubectl -n data-platform-infra port-forward svc/schema-registry 8081:8081
kubectl -n data-platform-govern port-forward svc/metadata 9002:9002
```

Generator access from your workstation:

```bash
kubectl -n data-platform-infra port-forward svc/postgres 5432:5432
kubectl -n data-platform-infra port-forward svc/kafka 19092:19092
kubectl -n data-platform-infra port-forward svc/schema-registry 8081:8081
```

That keeps the host-side generator defaults working when you are seeding manually from the host:

- `POSTGRES_HOST=localhost`
- `POSTGRES_PORT=5432`
- `KAFKA_BOOTSTRAP_SERVERS=localhost:19092`
- `SCHEMA_REGISTRY_URL=http://localhost:8081`

## Important limitations

- The current manifest preserves the repo's laptop-scale topology. It is not a production-hardened DOKS design.
- Metadata artifacts still need a better shared-storage story if you want fully durable multi-pod writes.
- Bootstrap jobs rely on workload ordering stabilizing through Kubernetes readiness and may need reruns during first-time bring-up.
- Training, model registry, inference, experimentation, and Redis-backed online features are no longer part of this manifest and should live in a sibling ML platform repo.

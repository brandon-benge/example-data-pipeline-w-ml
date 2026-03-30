# Kubernetes Deployment

This repo now targets DigitalOcean Kubernetes (`kubectl`) instead of `docker compose` for platform orchestration.

## Assumptions

- You already have `kubectl` pointed at your DOKS cluster.
- You are logged in to the DigitalOcean registry used for the repo bundle image, or your cluster already has pull access to it.
- This is still the same single-node demo architecture, just packaged as Kubernetes workloads.

## Images

All workloads now use upstream/public images directly.

Repo-owned logic is injected at runtime by:

- pulling the packaged repo bundle image and unpacking it into the pod workspace
- mounting repo files into pods
- building the Kafka Connect Iceberg plugin in an init container
- downloading Flink and Spark runtime jars in init containers
- installing Python dependencies for Flink jobs in init containers

## Repo Source

The manifest now uses the repo bundle image built by [tools/package_repo.sh](../tools/package_repo.sh) and [tools/push_repo_bundle.sh](../tools/push_repo_bundle.sh). The default image reference currently used by the init containers is:

- `registry.digitalocean.com/registry-k8s-1-35-1-do-1-tor1-1774447535918/repo-bundle:latest`

When you run `bash tools/run_stack_workflow.sh --stop-at <stage>`, the workflow packages the current repo, pushes that tag, and then applies the platform manifest.

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

The manifest provisions Postgres, Kafka, MinIO, and the Kafka Connect plugin cache with the repo-managed `do-block-storage-retain` storage class, which uses the DigitalOcean block storage CSI driver with `reclaimPolicy: Retain`. Deleting those PVCs will not automatically delete their backing volumes; clean them up manually when that is intentional.

If your cluster already has an older `do-block-storage-retain` definition, delete and recreate that `StorageClass` before a direct `kubectl apply`, because Kubernetes does not allow in-place updates to `StorageClass.parameters` or `volumeBindingMode`. The staged workflow handles that recreation for you through `tools/manage_stack.py`.

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

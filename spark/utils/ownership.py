from __future__ import annotations

from spark.utils.common import PROJECT_ROOT, load_yaml


_OWNERSHIP = load_yaml(PROJECT_ROOT / "config" / "governance" / "ownership.yaml")
_CLASSIFICATION = load_yaml(PROJECT_ROOT / "config" / "governance" / "classification.yaml")


def owner_for_dataset(dataset_name: str) -> str:
    owners = _OWNERSHIP.get("owners", {})
    for owner_name, payload in owners.items():
        if dataset_name in payload.get("datasets", []):
            return owner_name
    return "platform_admin"


def sensitivity_for_dataset(dataset_name: str) -> str:
    restricted = set(_CLASSIFICATION.get("datasets", {}).get("restricted_paths", []))
    if dataset_name in restricted:
        return "restricted"
    if dataset_name.startswith("iceberg.gold."):
        return _CLASSIFICATION.get("datasets", {}).get("gold", {}).get("default", "internal")
    if dataset_name.startswith("iceberg.silver."):
        return _CLASSIFICATION.get("datasets", {}).get("silver", {}).get("default", "confidential")
    return _CLASSIFICATION.get("datasets", {}).get("bronze", {}).get("default", "internal")

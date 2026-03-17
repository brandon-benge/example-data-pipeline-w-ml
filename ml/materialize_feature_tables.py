from __future__ import annotations

import subprocess

from ml.features import PROJECT_ROOT


def main() -> None:
    print("Deprecated: feature tables are now built by dbt. Running `dbt build --select features`.")
    subprocess.run(
        ["docker", "compose", "exec", "dbt", "dbt", "build", "--select", "features"],
        cwd=PROJECT_ROOT,
        check=True,
    )


if __name__ == "__main__":
    main()

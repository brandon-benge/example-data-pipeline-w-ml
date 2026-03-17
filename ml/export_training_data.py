from __future__ import annotations

from ml.materialize_feature_tables import main


if __name__ == "__main__":
    print("Deprecated: feature tables are now built by dbt. Running `docker compose exec dbt dbt build --select features` via the compatibility wrapper.")
    main()

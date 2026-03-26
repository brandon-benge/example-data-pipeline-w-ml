import json

from superset.app import create_app
from superset.extensions import db


SQLALCHEMY_URI = "trino://demo@trino.data-platform-serve:8080/iceberg/gold"
app = create_app()


with app.app_context():
    from superset.models.core import Database

    existing = db.session.query(Database).filter_by(database_name="trino_iceberg").one_or_none()
    if existing is None:
        existing = Database(database_name="trino_iceberg")
        db.session.add(existing)
    existing.sqlalchemy_uri = SQLALCHEMY_URI
    existing.exposed_in_sqllab = True
    existing.allow_ctas = False
    existing.allow_cvas = False
    existing.allow_dml = False
    existing.extra = json.dumps(
        {
            "metadata_params": {},
            "engine_params": {},
            "schemas_allowed_for_csv_upload": [],
            "default_schema": "gold",
        }
    )
    db.session.commit()

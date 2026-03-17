ROW_LIMIT = 5000
SUPERSET_WEBSERVER_PORT = 8088
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"
WTF_CSRF_ENABLED = True
SECRET_KEY = "local-demo-secret-key"
FEATURE_FLAGS = {
    "DASHBOARD_RBAC": True
}

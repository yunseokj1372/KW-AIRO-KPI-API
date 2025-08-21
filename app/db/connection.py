import oracledb
from app.core.config import settings

def get_connection():
    return oracledb.connect(
        user=settings.db_user,
        password=settings.db_password,
        dsn=settings.db_dsn,
    )
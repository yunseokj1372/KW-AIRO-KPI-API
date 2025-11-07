import oracledb
from app.core.config import settings

def getConnection():
    return oracledb.connect(
        user=settings.db_user,
        password=settings.db_password,
        dsn=settings.db_dsn,
    )
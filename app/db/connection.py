import oracledb
from app.core.config import settings

def getConnection():
    """Get synchronous connection - will be used in async context via thread pools"""
    return oracledb.connect(
        user=settings.db_user,
        password=settings.db_password,
        dsn=settings.db_dsn,
    )
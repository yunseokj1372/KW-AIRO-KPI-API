import pydantic_settings

class Settings(pydantic_settings.BaseSettings):
    db_user: str
    db_password: str
    db_dsn: str
    secret_key: str

    class Config:
        env_file = ".env"

settings = Settings()
from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments-local@localhost/payments"
    rabbitmq_url: str = "amqp://payments:payments-local@localhost:5672/"
    api_key: SecretStr


settings = Settings()

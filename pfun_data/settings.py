import os
from pydantic_settings import BaseSettings, SettingsConfigDict

__all__ = [
    'PostgresDBConfig',
    'pg_config'
]


class PostgresDBConfig(BaseSettings):
    """Postgres database configuration for pfun_data operations."""
    model_config = SettingsConfigDict(
        env_file='.env', validate_default=True, extra='allow')
    _pg_conn_str_template: str = \
        "postgresql://postgres:{pg_password}@{pg_host}:{pg_port}/pfun"
    pg_host: str = os.getenv("POSTGRES_HOST")
    pg_port: int = os.getenv("POSTGRES_PORT")
    pg_password: str = os.getenv("POSTGRES_PASSWORD")

    @property
    def pg_conn_str(self) -> str:
        """Postgres connection string."""
        return self._pg_conn_str_template.format(
            pg_password=self.pg_password,
            pg_host=self.pg_host,
            pg_port=self.pg_port
        )


pg_config = PostgresDBConfig()

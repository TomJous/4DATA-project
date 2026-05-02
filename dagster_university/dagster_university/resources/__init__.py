from contextlib import contextmanager

from dagster import ConfigurableResource, EnvVar
from sqlalchemy import create_engine


class PostgresResource(ConfigurableResource):
    connection_string: str

    @contextmanager
    def get_connection(self):
        engine = create_engine(self.connection_string)
        with engine.begin() as conn:
            yield conn
        engine.dispose()


database_resource = PostgresResource(
    connection_string=EnvVar("POSTGRES_CONNECTION_STRING")
)

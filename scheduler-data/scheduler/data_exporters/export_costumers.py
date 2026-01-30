from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.data_preparation.shared.secrets import get_secret_value
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


def create_qb_table(table_name: str, schema: str):
    
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    create_query = f"""
    CREATE SCHEMA IF NOT EXISTS {schema};
    CREATE TABLE IF NOT EXISTS {table_name} (
        id TEXT PRIMARY KEY,
        payload JSONB NOT NULL,
        ingested_at_utc TIMESTAMPTZ NOT NULL,
        extract_window_start_utc TIMESTAMPTZ NOT NULL,
        extract_window_end_utc TIMESTAMPTZ NOT NULL,
        page_number INT NOT NULL,
        page_size INT NOT NULL,
        request_payload TEXT NOT NULL
    );
    """
    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        print(f"Checking/Creating table: {table_name}...")
        loader.execute(create_query)
        print(f"Verified {table_name}.")


@data_exporter
def postgres_data_export(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a PostgreSQL database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#postgresql
    """
    schema_name = get_secret_value('pg_schema')  # Specify the name of the schema to export data to
    table_name = 'qb_costumer'  # Specify the name of the table to export data to

    create_qb_table(table_name, schema_name)

    full_table_name = f'{schema}.{table_name}'

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    print('Beginning data exportation...')
    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            df,
            schema_name,
            table_name,
            index=False,
            if_exists='append',  # UPSERT defined below by updating if it exists instead
            unique_constaints=['id'],
            unique_conflict_method='UPDATE',
            allow_reversed_words=True
        )
    print('Data exported to the database.')

    number_exported = 0

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        number_exported = loader.execture('SELECT COUNT(*) FROM {full_table_name}')

    print('Total number of entries in the database: ', number_exported)

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.data_preparation.shared.secrets import get_secret_value
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


def create_qb_table(logger, table_name: str, schema: str):
    
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    create_query = f"""
    CREATE SCHEMA IF NOT EXISTS {schema};
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT PRIMARY KEY,
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
        logger.info(f"Checking/Creating table: {table_name}...")
        loader.execute(create_query)
        logger.info(f"Verified {table_name}.")


@data_exporter
def postgres_data_export(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a PostgreSQL database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#postgresql
    """
    logger = kwargs.get('logger')

    schema_name = get_secret_value('pg_schema')  # Specify the name of the schema to export data to
    table_name = 'qb_invoice'  # Specify the name of the table to export data to

    create_qb_table(logger, table_name, schema_name)

    full_table_name = f'{schema_name}.{table_name}'

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    logger.info('Beginning data exportation...')
    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            df,
            schema_name,
            table_name,
            index=False,
            if_exists='append',  # UPSERT defined below by updating if it exists instead
            unique_constraints=['id'],
            unique_conflict_method='UPDATE',
        )
        logger.info('Data exported to the database.')

        logger.info(f"\n--- VERIFYING DATA IN {table_name} ---")

        df_count = loader.load(f'SELECT COUNT(*) as total FROM {full_table_name}')

        logger.info(f"Total number of entries in the database: {df_count.iloc[0]['total']}")

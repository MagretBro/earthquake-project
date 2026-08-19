import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor

OWNER = 'magret_bro'
DAG_ID = 'fct_avg_day_earthquake'

# using table DAG
LAYER = 'raw'
SOURCE = 'earthquake'
SCHEMA = 'dm'
TARGET_TABLE = 'fct_avg_day_earthquake'

# DWH
PG_CONNECT = 'postgres_dwh'
LONG_DESCRIPTION = """This DAG calculates the average earthquake magnitude for each day."""
SHORT_DESCRIPTION = """This DAG calculates the average earthquake magnitude for each day."""

args = {
    'owner': OWNER,
    'start_date': pendulum.datetime(2023, 1, 1, tz="UTC"),
    'catchup': True,
    'retries': 3,
    'retry_delay': pendulum.duration(hours=1),
}

with DAG (
    dag_id=DAG_ID,
    schedule_interval='@daily',
    default_args=args,
    description=SHORT_DESCRIPTION,
    tags=['dm', 'pg'],
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(task_id='start')

    sensor_on_raw_layer = ExternalTaskSensor(
        task_id='sensor_on_raw_layer',
        external_dag_id='raw_from_s3_to_pg',
        allowed_states=['success'],
        mode='reschedule',
        poke_interval=60,
        timeout=60,
    )

    drop_stg_table_before = SQLExecuteQueryOperator(
            task_id="drop_stg_table_before",
            conn_id=PG_CONNECT,
            autocommit=True,
            sql=f"""
            DROP TABLE IF EXISTS stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
            """,
        )

    create_stg_table = SQLExecuteQueryOperator(
        task_id="create_stg_table",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        CREATE TABLE stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}" AS
        SELECT
            time::date AS date,
            avg(mag::float)
        FROM
            ods.fct_earthquake
        WHERE
            time::date = '{{{{ data_interval_start.format('YYYY-MM-DD') }}}}'
        GROUP BY 1
        """,
    )



    drop_from_target_table = SQLExecuteQueryOperator(
        task_id='drop_from_target_table',
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""DELETE FROM {SCHEMA}.{TARGET_TABLE} 
        WHERE date in (
            SELECT date FROM stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        ) 
        """,
    )

    insert_into_target_table = SQLExecuteQueryOperator(
        task_id="insert_into_target_table",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        INSERT INTO {SCHEMA}.{TARGET_TABLE}
        SELECT * FROM stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        """,
    )

    drop_stg_table_after = SQLExecuteQueryOperator(
        task_id="drop_stg_table_after",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DROP TABLE IF EXISTS stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        """,
    )


    end = EmptyOperator(
        task_id="end",
    )

    (
        start >>
        sensor_on_raw_layer >>
        drop_stg_table_before >>
        create_stg_table >>
        drop_from_target_table >>
        insert_into_target_table >>
        drop_stg_table_after >>
        end
    )






















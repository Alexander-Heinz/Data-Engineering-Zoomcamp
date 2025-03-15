from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.common.time import Duration

def create_green_trips_source(t_env):
    source_ddl = """
        CREATE TABLE green_trips (
            lpep_pickup_datetime TIMESTAMP(3),
            lpep_dropoff_datetime TIMESTAMP(3),
            PULocationID INT,
            DOLocationID INT,
            passenger_count INT,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            WATERMARK FOR lpep_dropoff_datetime AS lpep_dropoff_datetime - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-trips',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """
    t_env.execute_sql(source_ddl)
    return "green_trips"

def session_aggregation():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(3)

    # Set up the table environment in streaming mode
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Create the source table from the Kafka topic 'green-trips'
    source_table = create_green_trips_source(t_env)

    # Define a session window query using lpep_dropoff_datetime with a 5 minute gap.
    # This query groups by PULocationID and DOLocationID and counts the number of trips per session.
    session_query = f"""
        SELECT
            SESSION_START(lpep_dropoff_datetime, INTERVAL '5' MINUTE) AS session_start,
            SESSION_END(lpep_dropoff_datetime, INTERVAL '5' MINUTE) AS session_end,
            PULocationID,
            DOLocationID,
            COUNT(*) AS trip_count
        FROM TABLE(
            SESSION(TABLE {source_table}, DESCRIPTOR(lpep_dropoff_datetime), INTERVAL '5' MINUTE)
        )
        GROUP BY SESSION(lpep_dropoff_datetime, INTERVAL '5' MINUTE), PULocationID, DOLocationID
        ORDER BY trip_count DESC
    """

    # Execute the query and print the results.
    result = t_env.sql_query(session_query)
    result.execute().print()

if __name__ == '__main__':
    session_aggregation()

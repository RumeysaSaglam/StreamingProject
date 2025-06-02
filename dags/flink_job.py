from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.jdbc import JdbcSink

import json

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Kafka consumer
    kafka_props = {
        'bootstrap.servers': 'kafka_broker:19092',
        'group.id': 'flink_consumer_group'
    }

    kafka_source = FlinkKafkaConsumer(
        topics='random_names_topic',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    stream = env.add_source(kafka_source)

    # Data transform (parse and map)
    parsed = stream \
        .map(lambda value: json.loads(value), output_type=Types.MAP(Types.STRING(), Types.STRING())) \
        .map(lambda d: (d['name'], d.get('timestamp', '')), output_type=Types.TUPLE([Types.STRING(), Types.STRING()]))

    # Write to Postgres
    jdbc_url = "jdbc:postgresql://postgres:5432/streaming_db"
    jdbc_driver = "org.postgresql.Driver"
    jdbc_user = "your_user"
    jdbc_password = "your_password"

    insert_sql = "INSERT INTO names_table (name, timestamp) VALUES (?, ?)"

    parsed.add_sink(JdbcSink.sink(
        sql=insert_sql,
        drivername=jdbc_driver,
        db_url=jdbc_url,
        username=jdbc_user,
        password=jdbc_password,
        parameter_types=[Types.STRING(), Types.STRING()]
    ))

    env.execute("Kafka to Postgres Streaming with Flink")

if __name__ == '__main__':
    main()

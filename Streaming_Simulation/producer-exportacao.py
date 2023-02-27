import io, time
import random
import avro.schema
from avro.datafile import DataFileReader
from avro.io import DatumWriter, DatumReader
from confluent_kafka import Producer

PRODUCER = Producer({'bootstrap.servers':'localhost:9092'})

# Kafka topic
TOPIC = "my-topic"

SCHEMA_PATH = "exportacao.avsc"
SCHEMA = avro.schema.parse(open(SCHEMA_PATH).read())


# Path to user.avsc avro schema
reader = DataFileReader(open("test.avro", "rb"), DatumReader())
for record in reader:
    try:  
        record["Time"] = time.time()
        writer = DatumWriter(SCHEMA)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write(record, encoder)
        raw_bytes = bytes_writer.getvalue()
        PRODUCER.produce(TOPIC, raw_bytes)
    except BufferError as e:
        print('%% Local producer queue is full (%d messages awaiting delivery): try again\n',len(PRODUCER))
    
    PRODUCER.poll(0)
    print('%% Waiting for %d deliveries\n' % len(PRODUCER))
    PRODUCER.flush()

reader.close()
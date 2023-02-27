import io, sys, time
import avro.schema
import avro.io
from confluent_kafka import Consumer

CONSUMER = Consumer({'bootstrap.servers':'localhost:9092','group.id':'python-consumer','auto.offset.reset':'earliest'})
print('Kafka Consumer has been initiated...')

TOPIC = "my-topic"

SCHEMA_PATH = "exportacao.avsc"
SCHEMA = avro.schema.parse(open(SCHEMA_PATH).read())
times = 0.0
count = 0
CONSUMER.subscribe([TOPIC])
try:
    while True:
        msg = CONSUMER.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print( msg.error )
        else:
            bytes_reader = io.BytesIO(msg.value())
            decoder = avro.io.BinaryDecoder(bytes_reader)
            reader = avro.io.DatumReader(SCHEMA)
            record = reader.read(decoder)
            times += (time.time() - record["Time"])
            count += 1
            print(times/count)

except KeyboardInterrupt:
    sys.stderr.write('%% Aborted by user\n')

CONSUMER.close()


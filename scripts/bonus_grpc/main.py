import grpc
import requests
import threading
import io
import pubsub_api_pb2 as pb2
import pubsub_api_pb2_grpc as pb2_grpc
import avro.schema
import avro.io
import time
import certifi
import json

semaphore = threading.Semaphore(1)
latest_replay_id = None

sessionid = 'session'
instanceurl = 'https://mydomain.my.salesforce.com'
tenantid = 'yourtenantid'
authmetadata = (('accesstoken', sessionid),
('instanceurl', instanceurl),
('tenantid', tenantid))

with open(certifi.where(), 'rb') as f:
    secure_channel_credentials = grpc.ssl_channel_credentials(f.read())
    pubsub_url = 'api.pubsub.salesforce.com:7443'
    channel = grpc.secure_channel(pubsub_url, secure_channel_credentials)
    stub = pb2_grpc.PubSubStub(channel)
    
def fetchReqStream(topic):
    while True:
        semaphore.acquire()
        yield pb2.FetchRequest(
            topic_name = topic,
            replay_preset = pb2.ReplayPreset.LATEST,
            num_requested = 1)

def decode(schema, payload):
  schema = avro.schema.parse(schema)
  buf = io.BytesIO(payload)
  decoder = avro.io.BinaryDecoder(buf)
  reader = avro.io.DatumReader(schema)
  ret = reader.read(decoder)
  return ret


mysubtopic = "/data/AccountChangeEvent"
print('Subscribing to ' + mysubtopic)
substream = stub.Subscribe(fetchReqStream(mysubtopic),
         metadata=authmetadata)
try:
    for event in substream:
        print(event)
        if event.events:
            semaphore.release()
            print("Number of events received: ", len(event.events))
            payloadbytes = event.events[0].event.payload
            schemaid = event.events[0].event.schema_id
            schema = stub.GetSchema(
                pb2.SchemaRequest(schema_id=schemaid),
                metadata=authmetadata
            ).schema_json
            print('decoding')
            decoded = decode(schema, payloadbytes)
            print("Got an event!", json.dumps(decoded))
        else:
            print("No events found (subway)")
        latest_replay_id = event.latest_replay_id
except grpc.RpcError as e:
    print(f"gRPC Error: {e.code()} - {e.details()}")



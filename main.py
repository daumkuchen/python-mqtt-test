from paho.mqtt import client as mqtt
from time import sleep
import ssl
import json

# BROKER = 'localhost'
BROKER = 'xxxxx.ap-northeast-1.amazonaws.com'

# PORT = 1883
PORT = 8883

PATH_TO_ROOT = './crt/root.pem'
PATH_TO_CERT = './crt/certificate.pem.crt'
PATH_TO_KEY = './crt/private.pem.key'
CLIENT_ID = 'rasp-test'
TOPIC = 'test/hello'


def on_init():
  client = mqtt.Client(
    client_id=CLIENT_ID,
    protocol=mqtt.MQTTv311)

  client.tls_set(
    ca_certs=PATH_TO_ROOT,
    certfile=PATH_TO_CERT,
    keyfile=PATH_TO_KEY,
    tls_version = ssl.PROTOCOL_TLSv1_2)
  client.tls_insecure_set(True)

  client.connect(
    BROKER,
    port=PORT,
    keepalive=60)

  return client


def on_connect(client, userdata, flags, rc):
  print(f'Connected `{str(rc)}`')
  sub(client)
  # pub(client)


def on_disconnect(client, userdata, rc):
  print(f'Disconnect `{str(rc)}`')


def on_publish(client, userdata, mid):
  print(f'Publish `{str(mid)}`')


def on_message(client, userdata, msg):
  # print(f'Sub `{msg.payload.decode()}` from `{msg.topic}` Topic')

  dict_msg = json.loads(msg.payload)
  # print(f'Sub `{dict_msg}`')
  print(f'Sub `{dict_msg["message"]}`')


def sub(client: mqtt):
  client.subscribe(TOPIC)
  client.on_message = on_message


def pub(client: mqtt):
  client.on_publish = on_publish


def pub_send(client: mqtt):
  msg = json.dumps({ 'message': 'Hello, MQTT' })
  client.publish('test/hello', msg)


def main():
  client = on_init()

  client.on_connect = on_connect
  client.on_disconnect = on_disconnect

  # Sub
  client.loop_forever()

  # Pub
  # client.loop_start()
  # while True:
  #   pub_send(client)
  #   sleep(1)


if __name__ == '__main__':
  main()
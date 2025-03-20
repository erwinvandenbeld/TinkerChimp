import argparse
from time import sleep

from awsiot import mqtt5_client_builder
from awscrt import mqtt5, http
import threading
from concurrent.futures import Future
from shaker import Shaker
import urllib3
import json
import boto3
from contextlib import closing
import os
from tempfile import gettempdir

TIMEOUT = 20
topic_filter = "chimp/topic"
received_count = 0
received_all_event = threading.Event()
future_stopped = Future()
future_connection_success = Future()
shaker = None
cert_filepath='../../../certs/iot-certificate.pem.crt'
pri_key_filepath='../../../certs/iot-private.pem.key'

def on_publish_received(publish_packet_data):
    publish_packet = publish_packet_data.publish_packet
    print("Received message from topic'{}':{}".format(publish_packet.topic, publish_packet.payload))

    shaker.shake()

    global received_count
    received_count += 1

def on_lifecycle_stopped(lifecycle_stopped_data: mqtt5.LifecycleStoppedData):
    print("Lifecycle Stopped")
    global future_stopped
    future_stopped.set_result(lifecycle_stopped_data)


def on_lifecycle_connection_success(lifecycle_connect_success_data: mqtt5.LifecycleConnectSuccessData):
    print("Lifecycle Connection Success")
    global future_connection_success
    future_connection_success.set_result(lifecycle_connect_success_data)


def on_lifecycle_connection_failure(lifecycle_connection_failure: mqtt5.LifecycleConnectFailureData):
    print("Lifecycle Connection Failure")
    print("Connection failed with exception:{}".format(lifecycle_connection_failure.exception))


def read_arguments() -> dict[str, str]:
    parser = argparse.ArgumentParser(description='Start listening to iot core.')
    parser.add_argument('--topic', type=str, required=False, default='chimp/topic',
                        help='The IOT core topic to listen to.')
    parser.add_argument('--endpoint', type=str, required=True,
                        help='The IOT core endpoint to connect to.')
    parser.add_argument('--no-gpio', action=argparse.BooleanOptionalAction)

    return parser.parse_args()

def get_boto_session():
    print("Getting STS token")
    resp = http.request("GET","https://<TODO>.credentials.iot.eu-west-1.amazonaws.com/role-aliases/iot-chim-ara-flapflap-role-alias/credentials")
    print(resp.status)
    print(resp.data)
    if resp.status != 200:
        raise Exception("Failed to retrieve STS token: {}".format(resp.reason))
    global creds 
    creds = json.loads(resp.data)['credentials']
    # return boto3.session.Session(aws_access_key_id=creds['accessKeyId'], aws_secret_access_key=creds['secretAccessKey'], aws_session_token=creds['sessionToken'], region_name=region)
    return boto3.Session(
        aws_access_key_id=creds['accessKeyId'],
        aws_secret_access_key=creds['secretAccessKey'],
        aws_session_token=creds['sessionToken']
        )


retries = urllib3.Retry(connect=5, read=2, redirect=5)
http = urllib3.PoolManager(
    cert_file=cert_filepath,
    key_file=pri_key_filepath,
    cert_reqs="CERT_REQUIRED",
    headers={
        'x-amzn-iot-thingname': 'Flapflap'
    },
    retries=retries
)

if __name__ == '__main__':


    print("\nStarting MQTT5 PubSub Sample\n")
    arguments = read_arguments()
    message_topic = arguments.topic
    shaker = Shaker(arguments.no_gpio == None)

    sess = get_boto_session()
    polly = sess.client('polly', region_name='eu-west-1')
    response = polly.synthesize_speech(
        OutputFormat='mp3',
        Text='Hello, this is a test from the AWS SDK for Python',
        TextType='text',
        VoiceId='Brian'
    )
    print(response)

    # Access the audio stream from the response
    if "AudioStream" in response:
        # Note: Closing the stream is important because the service throttles on the
        # number of parallel connections. Here we are using contextlib.closing to
        # ensure the close method of the stream object will be called automatically
        # at the end of the with statement's scope.
        with closing(response["AudioStream"]) as stream:
                output = os.path.join(gettempdir(), "iniIoTmessage.mp3")
                print("Writing audio stream to file {}".format(output))
                try:
                    # Open a file for writing the output as a binary stream
                    with open(output, "wb") as file:
                        file.write(stream.read())
                        
                except IOError as error:
                    # Could not write to file, exit gracefully
                    print(error)
                    sys.exit(-1)


    print("I got here")

    client = mqtt5_client_builder.mtls_from_path(
        endpoint=arguments.endpoint,
        port=8883,
        cert_filepath=cert_filepath,
        pri_key_filepath=pri_key_filepath,
        on_publish_received=on_publish_received,
        on_lifecycle_stopped=on_lifecycle_stopped,
        on_lifecycle_connection_success=on_lifecycle_connection_success,
        on_lifecycle_connection_failure=on_lifecycle_connection_failure,
    )
    print("MQTT5 Client Created")

    print("Connecting to endpoint with client ID")

    client.start()
    lifecycle_connect_success_data = future_connection_success.result(TIMEOUT)
    connack_packet = lifecycle_connect_success_data.connack_packet
    negotiated_settings = lifecycle_connect_success_data.negotiated_settings
    # Subscribe

    print("Subscribing to topic '{}'...".format(message_topic))
    subscribe_future = client.subscribe(subscribe_packet=mqtt5.SubscribePacket(
        subscriptions=[mqtt5.Subscription(
            topic_filter=message_topic,
            qos=mqtt5.QoS.AT_LEAST_ONCE)]
    ))
    suback = subscribe_future.result(TIMEOUT)
    print("Subscribed with {}".format(suback.reason_codes))

    received_all_event.wait()
    print("{} message(s) received.".format(received_count))

    # Unsubscribe

    print("Unsubscribing from topic '{}'".format(message_topic))
    unsubscribe_future = client.unsubscribe(unsubscribe_packet=mqtt5.UnsubscribePacket(
        topic_filters=[message_topic]))
    unsuback = unsubscribe_future.result(TIMEOUT)
    print("Unsubscribed with {}".format(unsuback.reason_codes))

    print("Stopping Client")
    client.stop()

    future_stopped.result(TIMEOUT)
    print("Client Stopped!")

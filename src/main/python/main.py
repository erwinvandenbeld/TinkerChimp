import argparse
from time import sleep

from awsiot import mqtt5_client_builder
from awscrt import mqtt5, http
import threading
from concurrent.futures import Future
from gpiozero import LED

TIMEOUT = 100
topic_filter = "chimp/topic"
received_count = 0
received_all_event = threading.Event()
future_stopped = Future()
future_connection_success = Future()


def on_publish_received(publish_packet_data):
    publish_packet = publish_packet_data.publish_packet
    print("Received message from topic'{}':{}".format(publish_packet.topic, publish_packet.payload))

    for i in range(4):
        led.blink()

    global received_count
    received_count += 1
    # FIXME Deceide when to stop
    if received_count == 4:
        received_all_event.set()


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

    return parser.parse_args()


if __name__ == '__main__':
    led = LED(17)

    print("\nStarting MQTT5 PubSub Sample\n")
    arguments = read_arguments()
    message_topic = arguments.topic

    client = mqtt5_client_builder.mtls_from_path(
        endpoint=arguments.endpoint,
        port=8883,
        cert_filepath='../../../certs/iot-certificate.pem.crt',
        pri_key_filepath='../../../certs/iot-private.pem.key',
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

    received_all_event.wait(TIMEOUT)
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

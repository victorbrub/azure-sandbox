"""
Example: Azure Event Hub Operations

This example demonstrates how to use Azure Event Hub utilities
to send and receive events.
"""

from utils.azure_eventhub import AzureEventHubProducer, AzureEventHubConsumer
from utils.config import get_config_manager
from utils.logging import configure_logging, get_logger, OperationLogger
import time
import json

# Configure logging
configure_logging(log_level="INFO")
logger = get_logger(__name__)

# Load configuration
config = get_config_manager(config_file="config.yml", env_file=".env")


def producer_example():
    """Example of producing events to Event Hub."""
    
    logger.info("=== Event Hub Producer Example ===")
    
    # Initialize producer
    namespace = config.get("azure.eventhub.namespace", required=True)
    eventhub_name = config.get("azure.eventhub.eventhub_name", required=True)
    connection_string = config.get("azure.eventhub.connection_string")
    
    producer = AzureEventHubProducer(
        namespace=namespace,
        eventhub_name=eventhub_name,
        connection_string=connection_string
    )
    
    # Example 1: Send a single event
    logger.info("\n=== Sending single event ===")
    event_data = {
        "event_type": "sensor_reading",
        "sensor_id": "sensor_001",
        "temperature": 22.5,
        "humidity": 65.3,
        "timestamp": time.time()
    }
    
    with OperationLogger(logger, "send_event"):
        producer.send_event(event_data, partition_key="sensor_001")
        logger.info("Event sent successfully")
    
    # Example 2: Send a batch of events
    logger.info("\n=== Sending batch of events ===")
    events = []
    for i in range(10):
        event = {
            "event_type": "sensor_reading",
            "sensor_id": f"sensor_{i:03d}",
            "temperature": 20 + (i * 0.5),
            "humidity": 60 + (i * 0.3),
            "timestamp": time.time()
        }
        events.append(event)
    
    with OperationLogger(logger, "send_batch", batch_size=len(events)):
        producer.send_batch(events, partition_key="batch_001")
        logger.info(f"Sent batch of {len(events)} events")
    
    producer.close()


def consumer_example():
    """Example of consuming events from Event Hub."""
    
    logger.info("\n\n=== Event Hub Consumer Example ===")
    
    # Initialize consumer
    namespace = config.get("azure.eventhub.namespace", required=True)
    eventhub_name = config.get("azure.eventhub.eventhub_name", required=True)
    connection_string = config.get("azure.eventhub.connection_string")
    
    consumer = AzureEventHubConsumer(
        namespace=namespace,
        eventhub_name=eventhub_name,
        consumer_group="$Default",
        connection_string=connection_string
    )
    
    # Example 1: Receive events with callback
    logger.info("\n=== Receiving events with callback ===")
    
    event_count = 0
    max_events = 10
    
    def on_event(event):
        """Callback function to process received events."""
        nonlocal event_count
        event_count += 1
        logger.info(f"Received event {event_count}: {event}")
        
        if event_count >= max_events:
            raise KeyboardInterrupt("Max events reached")
    
    try:
        with OperationLogger(logger, "receive_events"):
            consumer.receive_events(
                on_event=on_event,
                max_wait_time=30.0,  # Wait up to 30 seconds
                starting_position="-1"  # Start from latest
            )
    except KeyboardInterrupt:
        logger.info(f"Stopped after receiving {event_count} events")
    
    # Example 2: Receive a batch of events
    logger.info("\n=== Receiving batch of events ===")
    with OperationLogger(logger, "receive_batch"):
        batch = consumer.receive_batch(
            max_batch_size=5,
            max_wait_time=10.0
        )
        logger.info(f"Received batch of {len(batch)} events")
        for event in batch:
            logger.info(f"  Event: {event}")
    
    consumer.close()


def main():
    """Main example function."""
    
    # Run producer example
    producer_example()
    
    # Wait a bit for events to be available
    time.sleep(2)
    
    # Run consumer example
    consumer_example()


if __name__ == "__main__":
    main()

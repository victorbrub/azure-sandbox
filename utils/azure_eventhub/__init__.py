"""Azure Event Hub Utilities

Provides utilities for producing and consuming messages from Azure Event Hubs,
which is a highly scalable data streaming platform.
"""

from azure.eventhub import EventHubProducerClient, EventHubConsumerClient, EventData
from azure.eventhub.exceptions import EventHubError
from azure.identity import DefaultAzureCredential
from typing import List, Dict, Optional, Any, Callable
import logging
import json

logger = logging.getLogger(__name__)


class AzureEventHubProducer:
    """Producer client for sending events to Azure Event Hub."""
    
    def __init__(self, namespace: str, eventhub_name: str, 
                 connection_string: Optional[str] = None):
        """
        Initialize Event Hub Producer client.
        
        Args:
            namespace: Event Hub namespace (e.g., mynamespace.servicebus.windows.net)
            eventhub_name: Name of the Event Hub
            connection_string: Optional connection string (uses Azure AD if not provided)
        """
        self.namespace = namespace
        self.eventhub_name = eventhub_name
        
        if connection_string:
            self.producer = EventHubProducerClient.from_connection_string(
                conn_str=connection_string,
                eventhub_name=eventhub_name
            )
        else:
            credential = DefaultAzureCredential()
            self.producer = EventHubProducerClient(
                fully_qualified_namespace=namespace,
                eventhub_name=eventhub_name,
                credential=credential
            )
        
        logger.info(f"Initialized Event Hub producer for: {eventhub_name}")
    
    def send_event(self, event_data: Any, partition_key: Optional[str] = None) -> None:
        """
        Send a single event to Event Hub.
        
        Args:
            event_data: Event data (will be JSON serialized if dict)
            partition_key: Optional partition key for routing
        """
        try:
            if isinstance(event_data, dict):
                event_data = json.dumps(event_data)
            
            event = EventData(event_data)
            
            with self.producer:
                event_batch = self.producer.create_batch(partition_key=partition_key)
                event_batch.add(event)
                self.producer.send_batch(event_batch)
            
            logger.info(f"Sent event to Event Hub: {self.eventhub_name}")
        except Exception as e:
            logger.error(f"Failed to send event: {e}")
            raise
    
    def send_batch(self, events: List[Any], partition_key: Optional[str] = None) -> None:
        """
        Send a batch of events to Event Hub.
        
        Args:
            events: List of event data
            partition_key: Optional partition key for routing
        """
        try:
            with self.producer:
                event_batch = self.producer.create_batch(partition_key=partition_key)
                
                for event_data in events:
                    if isinstance(event_data, dict):
                        event_data = json.dumps(event_data)
                    event_batch.add(EventData(event_data))
                
                self.producer.send_batch(event_batch)
            
            logger.info(f"Sent batch of {len(events)} events to Event Hub: {self.eventhub_name}")
        except Exception as e:
            logger.error(f"Failed to send batch: {e}")
            raise
    
    def close(self) -> None:
        """Close the producer client."""
        try:
            self.producer.close()
            logger.info("Closed Event Hub producer")
        except Exception as e:
            logger.error(f"Failed to close producer: {e}")
            raise


class AzureEventHubConsumer:
    """Consumer client for receiving events from Azure Event Hub."""
    
    def __init__(self, namespace: str, eventhub_name: str, consumer_group: str = "$Default",
                 connection_string: Optional[str] = None):
        """
        Initialize Event Hub Consumer client.
        
        Args:
            namespace: Event Hub namespace
            eventhub_name: Name of the Event Hub
            consumer_group: Consumer group name (defaults to $Default)
            connection_string: Optional connection string (uses Azure AD if not provided)
        """
        self.namespace = namespace
        self.eventhub_name = eventhub_name
        self.consumer_group = consumer_group
        
        if connection_string:
            self.consumer = EventHubConsumerClient.from_connection_string(
                conn_str=connection_string,
                consumer_group=consumer_group,
                eventhub_name=eventhub_name
            )
        else:
            credential = DefaultAzureCredential()
            self.consumer = EventHubConsumerClient(
                fully_qualified_namespace=namespace,
                eventhub_name=eventhub_name,
                consumer_group=consumer_group,
                credential=credential
            )
        
        logger.info(f"Initialized Event Hub consumer for: {eventhub_name}, group: {consumer_group}")
    
    def receive_events(self, on_event: Callable[[Any], None], 
                      max_wait_time: Optional[float] = None,
                      starting_position: str = "-1") -> None:
        """
        Receive events from Event Hub with a callback function.
        
        Args:
            on_event: Callback function to process each event
            max_wait_time: Maximum time to wait for events (None = indefinite)
            starting_position: Starting position ("-1" = end, "0" = beginning)
        """
        def on_event_wrapper(partition_context, event):
            try:
                if event:
                    event_data = event.body_as_str()
                    try:
                        event_data = json.loads(event_data)
                    except json.JSONDecodeError:
                        pass
                    
                    on_event(event_data)
                    partition_context.update_checkpoint(event)
            except Exception as e:
                logger.error(f"Error processing event: {e}")
        
        try:
            with self.consumer:
                self.consumer.receive(
                    on_event=on_event_wrapper,
                    max_wait_time=max_wait_time,
                    starting_position=starting_position
                )
            logger.info("Started receiving events")
        except KeyboardInterrupt:
            logger.info("Stopped receiving events")
        except Exception as e:
            logger.error(f"Failed to receive events: {e}")
            raise
    
    def receive_batch(self, max_batch_size: int = 100, 
                     max_wait_time: float = 60.0) -> List[Any]:
        """
        Receive a batch of events from Event Hub.
        
        Args:
            max_batch_size: Maximum number of events to receive
            max_wait_time: Maximum time to wait for events
            
        Returns:
            List of events
        """
        events = []
        
        def on_event(partition_context, event):
            if event:
                event_data = event.body_as_str()
                try:
                    event_data = json.loads(event_data)
                except json.JSONDecodeError:
                    pass
                events.append(event_data)
                partition_context.update_checkpoint(event)
                
                if len(events) >= max_batch_size:
                    raise StopIteration()
        
        try:
            with self.consumer:
                self.consumer.receive(
                    on_event=on_event,
                    max_wait_time=max_wait_time,
                    starting_position="-1"
                )
            logger.info(f"Received batch of {len(events)} events")
            return events
        except StopIteration:
            logger.info(f"Received batch of {len(events)} events (max reached)")
            return events
        except Exception as e:
            logger.error(f"Failed to receive batch: {e}")
            raise
    
    def close(self) -> None:
        """Close the consumer client."""
        try:
            self.consumer.close()
            logger.info("Closed Event Hub consumer")
        except Exception as e:
            logger.error(f"Failed to close consumer: {e}")
            raise


class EventHubCheckpointStore:
    """Helper class for managing checkpoints in Blob Storage."""
    
    def __init__(self, storage_connection_string: str, container_name: str):
        """
        Initialize checkpoint store.
        
        Args:
            storage_connection_string: Blob Storage connection string
            container_name: Container name for storing checkpoints
        """
        from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
        
        self.checkpoint_store = BlobCheckpointStore.from_connection_string(
            storage_connection_string,
            container_name
        )
        logger.info(f"Initialized checkpoint store with container: {container_name}")
    
    def get_store(self):
        """Get the checkpoint store instance."""
        return self.checkpoint_store

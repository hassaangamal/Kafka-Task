# Optimized Consumer
from confluent_kafka import Consumer, KafkaError
import os
import base64
import csv
from PIL import Image
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor
import threading

consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'image-consumer-group',
    'auto.offset.reset': 'earliest',
    # Performance optimization
    'fetch.min.bytes': 1024 * 1024,  # 1MB minimum fetch
    'max.partition.fetch.bytes': 1048576,  # 1MB per partition
    'enable.auto.commit': False,     # Manual commit for better control
    # Session timeout
    'session.timeout.ms': 30000,     # 30 seconds
    'max.poll.interval.ms': 300000,  # 5 minutes
}

class BatchingConsumer:
    def __init__(self, config, topics, output_directory, csv_file, batch_size=10):
        self.consumer = Consumer(config)
        self.consumer.subscribe(topics)
        self.output_directory = output_directory
        self.csv_file = csv_file
        self.batch_size = batch_size
        self.shutdown_flag = threading.Event()
        self.csv_lock = threading.Lock()
        
        os.makedirs(output_directory, exist_ok=True)

    def process_message(self, msg):
        try:
            payload = eval(msg.value().decode('utf-8'))
            serialized_image = payload["image"]
            annotation = payload["annotation"]
            
            # Deserialize and save image
            image_data = base64.b64decode(serialized_image)
            image = Image.open(BytesIO(image_data))
            image_path = os.path.join(self.output_directory, annotation)
            image.save(image_path)
            
            # Write to CSV with thread safety
            with self.csv_lock:
                with open(self.csv_file, mode="a", newline="", encoding="utf-8") as file:
                    csv_writer = csv.writer(file)
                    csv_writer.writerow([image_path, annotation])
            
            return True
        except Exception as e:
            print(f"Error processing message: {e}")
            return False

    def run(self):
        # Initialize CSV file with headers
        with open(self.csv_file, mode="w", newline="", encoding="utf-8") as file:
            csv_writer = csv.writer(file)
            csv_writer.writerow(["Image Path", "Annotation"])
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            while not self.shutdown_flag.is_set():
                messages = []
                
                # Collect batch of messages
                while len(messages) < self.batch_size:
                    msg = self.consumer.poll(0.1)
                    if msg is None:
                        break
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        print(f"Consumer error: {msg.error()}")
                        continue
                    messages.append(msg)
                
                if messages:
                    # Process batch in parallel
                    futures = [executor.submit(self.process_message, msg) for msg in messages]
                    
                    # Wait for all processing to complete
                    all_processed = all(future.result() for future in futures)
                    
                    if all_processed:
                        self.consumer.commit()

    def close(self):
        self.shutdown_flag.set()
        self.consumer.close()

def main():
    consumer = BatchingConsumer(
        consumer_config,
        ["my-topic"],
        r"E:\GZ Headway\Kafka\output_images",
        "image_data.csv"
    )
    
    try:
        consumer.run()
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()

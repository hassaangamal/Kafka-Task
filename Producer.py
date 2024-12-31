
from confluent_kafka import Producer
import os
import base64
from concurrent.futures import ThreadPoolExecutor
import queue
import threading

# Enhanced producer configuration
producer_config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'image-producer',
    # Batch settings
    'batch.size': 2097152,  # 2MB batch size
    'linger.ms': 5,         # Wait up to 5ms to batch messages
    # Compression
    'compression.type': 'lz4',  # Use lz4 compression
    'acks': '1',            # Leader acknowledgment only
    # Retries configuration
    'retries': 3,
    'retry.backoff.ms': 100,
}

class BatchingProducer:
    def __init__(self, config, topic, batch_size=10):
        self.producer = Producer(config)
        self.topic = topic
        self.batch_size = batch_size
        self.message_queue = queue.Queue()
        self.shutdown_flag = threading.Event()
        self.batch_thread = threading.Thread(target=self._batch_sender)
        self.batch_thread.start()

    def _batch_sender(self):
        while not self.shutdown_flag.is_set() or not self.message_queue.empty():
            batch = []
            try:
                while len(batch) < self.batch_size:
                    try:
                        msg = self.message_queue.get(timeout=0.1)
                        batch.append(msg)
                    except queue.Empty:
                        break
                
                if batch:
                    for msg in batch:
                        self.producer.produce(
                            self.topic,
                            key=msg['key'],
                            value=msg['value'],
                            callback=self._delivery_report
                        )
                    self.producer.flush()
            except Exception as e:
                print(f"Error in batch sending: {e}")

    def send(self, key, value):
        self.message_queue.put({'key': key, 'value': value})

    def _delivery_report(self, err, msg):
        if err:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def close(self):
        self.shutdown_flag.set()
        self.batch_thread.join()
        self.producer.flush()

def process_image(image_path):
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')

def main():
    topic = "my-topic"
    producer = BatchingProducer(producer_config, topic)
    
    image_directory = r"images"
    
    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        
        for filename in os.listdir(image_directory):
            if filename.endswith((".png", ".jpg", ".jpeg")):
                image_path = os.path.join(image_directory, filename)
                futures.append(executor.submit(process_image, image_path))
                
        for filename, future in zip(os.listdir(image_directory), futures):
            if filename.endswith((".png", ".jpg", ".jpeg")):
                try:
                    serialized_image = future.result()
                    payload = {
                        "image": serialized_image,
                        "annotation": filename
                    }
                    producer.send(
                        key=filename.encode('utf-8'),
                        value=str(payload).encode('utf-8')
                    )
                except Exception as e:
                    print(f"Error processing {filename}: {e}")

    producer.close()

if __name__ == "__main__":
    main()
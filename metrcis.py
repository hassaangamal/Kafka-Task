import time
import csv
import os
import base64
from confluent_kafka import Producer, Consumer, KafkaError
from concurrent.futures import ThreadPoolExecutor
import statistics
import json

class BatchingProducer:
    def __init__(self, config, topic, batch_size=10):
        # Remove invalid configuration
        valid_config = {
            'bootstrap.servers': config['bootstrap.servers'],
            'client.id': 'image-producer',
            'batch.size': config.get('batch.size', 16384),
            'linger.ms': config.get('linger.ms', 0),
            'compression.type': config.get('compression.type', 'none'),
            'acks': config.get('acks', '1'),
            'retries': config.get('retries', 3),
            'retry.backoff.ms': config.get('retry.backoff.ms', 100),
        }
        
        self.producer = Producer(valid_config)
        self.topic = topic
        self.batch_size = batch_size
        self.messages_sent = 0
        self.total_latency = 0

    def send(self, key, value):
        start_time = time.time()
        try:
            self.producer.produce(
                self.topic,
                key=key,
                value=value,
                callback=self._delivery_report
            )
            self.producer.poll(0)  # Trigger delivery reports
            
            end_time = time.time()
            self.total_latency += (end_time - start_time)
            self.messages_sent += 1
            
            # Flush after batch size is reached
            if self.messages_sent % self.batch_size == 0:
                self.producer.flush()
                
        except Exception as e:
            print(f"Error sending message: {e}")

    def _delivery_report(self, err, msg):
        if err:
            print(f'Message delivery failed: {err}')
        else:
            pass  # Successful delivery

    def get_metrics(self):
        return {
            'messages_sent': self.messages_sent,
            'avg_latency': (self.total_latency / self.messages_sent) if self.messages_sent > 0 else 0
        }

    def close(self):
        self.producer.flush()

def process_image(image_path):
    try:
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode('utf-8')
    except Exception as e:
        print(f"Error processing image {image_path}: {e}")
        return None

def test_configuration(producer_config, consumer_config, image_directory, batch_size=10, num_threads=4):
    start_time = time.time()
    producer = BatchingProducer(producer_config, "my-topic", batch_size)
    
    results = {
        'batch_size': batch_size,
        'num_threads': num_threads,
        'compression': producer_config.get('compression.type', 'none'),
        'linger_ms': producer_config.get('linger.ms', 0),
        'acks': producer_config.get('acks', '1')
    }
    
    try:
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            
            # Process images in parallel
            for filename in os.listdir(image_directory):
                if filename.endswith((".png", ".jpg", ".jpeg")):
                    image_path = os.path.join(image_directory, filename)
                    futures.append(executor.submit(process_image, image_path))
            
            # Send messages
            for filename, future in zip(os.listdir(image_directory), futures):
                if filename.endswith((".png", ".jpg", ".jpeg")):
                    try:
                        serialized_image = future.result()
                        if serialized_image:
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
        
        end_time = time.time()
        duration = end_time - start_time
        
        producer_metrics = producer.get_metrics()
        
        results.update({
            'throughput': producer_metrics['messages_sent'] / duration,
            'avg_latency_ms': producer_metrics['avg_latency'] * 1000,  # Convert to ms
            'total_duration_s': duration
        })
        
    finally:
        producer.close()
    
    return results

def main():
    image_directory = r"E:\GZ Headway\Kafka\images"
    
    # Test configurations
    configurations = [
        # Base configuration
        {
            'producer': {
                'bootstrap.servers': 'localhost:9092',
                'batch.size': 16384,
                'linger.ms': 0,
                'compression.type': 'none',
                'acks': '1'
            },
            'batch_size': 1,
            'threads': 1
        },
        # Optimized configuration
        {
            'producer': {
                'bootstrap.servers': 'localhost:9092',
                'batch.size': 1048576,  # 1MB
                'linger.ms': 5,
                'compression.type': 'snappy',
                'acks': '1'
            },
            'batch_size': 10,
            'threads': 4
        },
        # High throughput configuration
        {
            'producer': {
                'bootstrap.servers': 'localhost:9092',
                'batch.size': 2097152,  # 2MB
                'linger.ms': 10,
                'compression.type': 'lz4',
                'acks': '1'
            },
            'batch_size': 20,
            'threads': 8
        },
        # 
        {
        'producer': {
            'bootstrap.servers': 'localhost:9092',
            'batch.size': 524288,  # 512 KB
            'linger.ms': 3,
            'compression.type': 'snappy',
            'acks': '1'
        },
        'batch_size': 10,
        'threads': 4
    },    # Extreme throughput
    {
        'producer': {
            'bootstrap.servers': 'localhost:9092',
            'batch.size': 2097152,  # 2 MB
            'linger.ms': 10,
            'compression.type': 'gzip',
            'acks': '1'
        },
        'batch_size': 50,
        'threads': 16
    }
    ]
    
    results = []
    
    for config in configurations:
        print(f"Testing configuration: {json.dumps(config, indent=2)}")
        result = test_configuration(
            config['producer'],
            None,  # We're focusing on producer metrics for now
            image_directory,
            config['batch_size'],
            config['threads']
        )
        results.append(result)
    
    # Save results to CSV
    if results:
        headers = results[0].keys()
        with open('kafka_performance_results.csv', 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(results)
        print("Results saved to kafka_performance_results.csv")

if __name__ == "__main__":
    main()
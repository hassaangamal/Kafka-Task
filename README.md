# Kafka Image Processing Optimization Project

This project demonstrates various optimization techniques for processing and sending images through Apache Kafka using Docker and Python. The implementation includes different configurations to analyze and compare performance metrics.

## System Architecture

The system consists of:
- A Kafka broker running in Docker
- Confluent Control Center for monitoring
- Python-based producer with batching capabilities
- Multi-threaded image processing pipeline

## Prerequisites

- Docker and Docker Compose
- Python 3.x
- Required Python packages:
  - confluent-kafka
  - concurrent.futures (standard library)

## Project Structure

```
├── docker-compose.yml
├── metrcis.py
├── Consumer.py
├── Producer.py
└── images/
    └── [your image files]
```

## Configuration

### Docker Environment

The Docker setup includes:
- Single Kafka broker with KRaft (no ZooKeeper)
- Exposed ports:
  - 9092: Kafka broker
  - 9021: Control Center UI
- Configured for development with minimal replication

### Producer Configurations Tested

Five different configurations were tested with varying parameters:

1. **Base Configuration**
   - Batch Size: 16KB
   - No compression
   - Single thread
   - No message lingering

2. **Optimized Configuration**
   - Batch Size: 1MB
   - Snappy compression
   - 4 threads
   - 5ms linger time

3. **High Throughput Configuration**
   - Batch Size: 2MB
   - LZ4 compression
   - 8 threads
   - 10ms linger time

4. **Balanced Configuration**
   - Batch Size: 512KB
   - Snappy compression
   - 4 threads
   - 3ms linger time

5. **Extreme Throughput Configuration**
   - Batch Size: 2MB
   - GZIP compression
   - 16 threads
   - 10ms linger time

## Performance Results

| Configuration | Throughput (msg/s) | Latency (ms) | Duration (s) |
|--------------|-------------------|--------------|--------------|
| Base         | 17.85            | 0.026        | 8.63         |
| Optimized    | 417.56           | 0.004        | 0.37         |
| High         | 666.34           | 0.006        | 0.23         |
| Balanced     | 346.52           | 0.006        | 0.44         |
| Extreme      | 135.83           | 0.005        | 1.13         |

### Key Findings

1. The High Throughput configuration achieved the best performance with:
   - Highest throughput: 666.34 messages/second
   - Competitive latency: 0.006ms
   - Fastest total duration: 0.23 seconds

2. Compression Impact:
   - LZ4 compression provided the best balance of speed and compression
   - GZIP showed poorest performance despite highest thread count
   - Snappy proved effective for balanced configurations

3. Threading Impact:
   - Increasing threads from 1 to 8 showed significant improvements
   - Diminishing returns observed at 16 threads
   - 4-8 threads appeared optimal for this workload

## Steps to Reproduce

### Prerequisites

- Docker and Docker Compose installed.
- Python 3.x and required libraries installed (`confluent_kafka`, `statistics`, etc.).

### Setup

1. Clone the repository.
2. Navigate to the project directory.
3. Start the Kafka environment:
   ```bash
   docker-compose up -d
   ```

### Run the Benchmark

1. Place your test images in the `images` directory.
2. Execute the script:
   ```bash
   python measure_script.py
   ```
3. View the performance results in `kafka_performance_results.csv`.

## Insights and Observations

- Larger batch sizes generally improve throughput but can slightly increase latency.
- Compression methods like `snappy` and `lz4` strike a balance between throughput and latency.
- Increasing `linger.ms` allows the producer to batch messages effectively, improving throughput.
- Threading significantly enhances performance for higher workloads.

## Future Improvements

- Include consumer metrics for end-to-end performance analysis.
- Test configurations with varying replication factors and partitions.

## Conclusion

The optimized configuration achieved a throughput of 666.34 messages per second with minimal latency, demonstrating the importance of fine-tuning producer settings in Kafka.

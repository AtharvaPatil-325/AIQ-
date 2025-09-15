import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Properties;

public class DataIngestion {
    public static void main(String[] args) {
        // Initialize satellite feed
        SatelliteFeed feed = new SatelliteFeed("Sentinel-2");

        // Kafka producer configuration
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, "all"); // Ensure message delivery

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(kafkaProps);

        try {
            while (feed.hasNextFrame()) {
                Frame data = feed.getNextFrame();

                // Create a Kafka record
                ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                    "satellite-topic", // Kafka topic
                    data.getId(),      // Key (frame ID)
                    data.toBytes()     // Value (frame data as bytes)
                );

                // Send data asynchronously and handle callback
                producer.send(record, (RecordMetadata metadata, Exception exception) -> {
                    if (exception != null) {
                        System.err.println("Error sending frame " + data.getId() + ": " + exception.getMessage());
                    } else {
                        System.out.println("Successfully ingested frame " + data.getId() +
                            " to partition " + metadata.partition() +
                            " at offset " + metadata.offset());
                    }
                });
            }
        } catch (Exception e) {
            System.err.println("Error during data ingestion: " + e.getMessage());
        } finally {
            // Close the producer to release resources
            producer.close();
        }
    }
}
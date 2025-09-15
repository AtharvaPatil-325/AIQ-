import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Properties;

public class DataIngestion {
    public static void main(String[] args) {
        SatelliteFeed feed = new SatelliteFeed("Sentinel-2");

        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, "all");

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(kafkaProps);

        try {
            while (feed.hasNextFrame()) {
                Frame data = feed.getNextFrame();

                ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                    "satellite-topic",
                    data.getId(),
                    data.toBytes()
                );

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
            producer.close();
        }
    }
}
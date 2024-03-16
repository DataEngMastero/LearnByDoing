package org.example;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Sending Data to Kafka");

        // Kafka Producer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // Set Producer Properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400");

        // Create Kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int j = 0; j < 10; j++) {
            for(int i = 0; i < 30; i++) {
                // Create Producer Record
                ProducerRecord<String, String> record = new ProducerRecord<>("java_demo", "hello world " + i);

                // Send data to Producer
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // Executes everytime record is send successfully or an exception occurs
                        if(e == null) {
                            log.info("Record was successfully sent! Received New Metadata \n" +
                                    "Topic : " + metadata.topic() + "\n" +
                                    "Partition : " + metadata.partition() + "\n" +
                                    "Offset : " + metadata.offset() + "\n" +
                                    "Timestamp : " + metadata.timestamp() + "\n"
                            );
                        } else {
                            log.error("Error while producing ", e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }




        // Flush and Close
        producer.flush();
        producer.close();

    }
}

package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Sending Data to Kafka");

        // Kafka Producer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // Set Producer Properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Create Kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Create Producer Record
        ProducerRecord<String, String> record = new ProducerRecord<>("java_demo", "hello word");

        // Send data to Producer
        producer.send(record);

        // Flush and Close
        producer.flush();
        producer.close();

    }
}

package com.contactsunny.poc.SimpleKafkaProducer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;

@SpringBootApplication
public class SimpleKafkaProducerApplication implements CommandLineRunner {

    @Value("${kafka.topic.thetechcheck}")
    private String theTechCheckTopicName;

    @Value("${kafka.bootstrap.servers}")
    private String kafkaBootstrapServers;

    @Value("${zookeeper.groupId}")
    private String zookeeperGroupId;

    @Value("${zookeeper.host}")
    String zookeeperHost;

    private static final Logger logger = Logger.getLogger(SimpleKafkaProducerApplication.class);

    public static void main( String[] args ) {
        SpringApplication.run(SimpleKafkaProducerApplication.class, args);
    }

    @Override
    public void run(String... args) {

        /*
         * Defining producer properties.
         */
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBootstrapServers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        /*
        Creating a Kafka Producer object with the configuration above.
         */
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        /*
        Creating a loop which iterates 10 times, from 0 to 9, and sending a
        simple message to Kafka.
         */
        for (int index = 0; index < 10; index++) {
            sendKafkaMessage("The index is now: " + index, producer, theTechCheckTopicName);
        }

    }

    /**
     * Function to send a message to Kafka
     * @param payload
     * @param producer
     * @param topic
     */
    private static void sendKafkaMessage(String payload,
             KafkaProducer<String, String> producer,
             String topic)
    {
        logger.info("Sending Kafka message: " + payload);
        producer.send(new ProducerRecord<>(topic, payload));
    }
}

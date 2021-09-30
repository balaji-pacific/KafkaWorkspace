package com.kafka.simple.program;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * Kafka Consumer Demo
 * @author Balaji
 */
public class Consumer {

    public static String BOOTSTRAP_SERVER = "localhost:9092";
    public static String GROUP_ID = "kafka-example-1";
    public static String TOPICS = "MyTopic";


    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(Consumer.class);

        //Creating Properties for Consumer
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //Subscribe Consumer
        consumer.subscribe(Arrays.asList(TOPICS));

        while(true){
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            logger.info("Count Value "+consumerRecords.count());
            for(ConsumerRecord<String, String> consumerRecord : consumerRecords){
                logger.info("Key : " + consumerRecord.key() + "Value : " + consumerRecord.value());
                logger.info("Topic : " + consumerRecord.topic() + "Partition : " + consumerRecord.partition());
            }
        }
    }
}

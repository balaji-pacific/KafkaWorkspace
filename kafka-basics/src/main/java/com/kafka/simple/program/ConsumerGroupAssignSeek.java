package com.kafka.simple.program;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * Kafka Consumer Demo
 * @author Balaji
 */
public class ConsumerGroupAssignSeek {

    public static String BOOTSTRAP_SERVER = "localhost:9092";
    public static String TOPICS = "MyTopic";


    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerGroupAssignSeek.class);

        //Creating Properties for Consumer
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //Assign
        TopicPartition topicPartitionReadFrom = new TopicPartition(TOPICS, 0);
        long offsetToReadFrom = 150L;
        consumer.assign(Arrays.asList(topicPartitionReadFrom));

        consumer.seek(topicPartitionReadFrom, offsetToReadFrom);

        int numberOfMessageRead = 5;
        int numberOfMessageReadSoFar = 0;
        boolean keepReading = true;


        while(keepReading){
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> consumerRecord : consumerRecords){
                numberOfMessageReadSoFar +=1;
                logger.info(" Key : " + consumerRecord.key() + " Value : " + consumerRecord.value() + "\n");
                logger.info(" Topic : " + consumerRecord.topic() + " Partition : " + consumerRecord.partition() + "\n");
            }
            if(numberOfMessageReadSoFar >= numberOfMessageRead){
                keepReading = false;
                break;
            }
        }
        logger.info("Exiting the loop");
    }
}

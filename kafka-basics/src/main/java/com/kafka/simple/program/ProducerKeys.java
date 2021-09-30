package com.kafka.simple.program;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerKeys {

    public static final String BOOTSTRAP_SERVERS = "localhost:9092";



    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();

        Logger logger = LoggerFactory.getLogger(ProducerKeys.class);

        ProducerRecord<String, String> record = null;

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(int i=0; i<10;i++){

            String topic_name  = "MyTopic";
            String value = "Hello World " + String.valueOf(i);
            String key = "id_" + Integer.toString(i);

            record = new ProducerRecord<>(topic_name, key, value);
            logger.info("Key : " + key);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null){
                        logger.info("\nRecieved New Data. \n" +
                                "Topic : " + recordMetadata.topic() + "\n" +
                                "Timestamp : " + recordMetadata.hasTimestamp() + "\n" +
                                "Offset : " + recordMetadata.offset() + "\n" +
                                "Partition : " + recordMetadata.partition());
                    }else{
                        logger.error("Exception : " + e);
                    }
                }
            }).get();
        }
        producer.flush();
        producer.close();
    }
}
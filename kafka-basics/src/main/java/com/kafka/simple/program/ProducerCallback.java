package com.kafka.simple.program;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerCallback {

    public static final String BOOTSTRAP_SERVERS = "localhost:9092";



    public static void main(String[] args) {
        Properties properties = new Properties();

        Logger logger = LoggerFactory.getLogger(ProducerCallback.class);

        ProducerRecord<String, String> record = null;

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(int i=0; i<10;i++){
            record = new ProducerRecord<>("MyTopic","Hello World  "+ String.valueOf(i));
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
            });
        }
        producer.flush();
        producer.close();
    }
}
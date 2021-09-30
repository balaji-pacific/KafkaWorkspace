package com.kafka.simple.program.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String API_KEY = "zMiqkOHoaqSuk7GGvgtxIndzR";
    public static final String API_KEY_SECRET = "1UO3cZsB8Yzs57woz2CFspToLOA6ymR82Mfs4i1c1OWb2dJCNI";
    public static final String ACCESS_TOKEN = "248150763-cw6SWXpxUQA3bB8ODNziIrWqdNL5M1KswlgjxKNf";
    public static final String ACCESS_TOKEN_SECRET = "bZJa79DRwmdnNDA0j7M2SqPtpznfDHvq9jIiEoYdtJ6aa";

    public TwitterProducer(){

    }

    public static void main(String[] args) {

        new TwitterProducer().run();

    }

    public void run(){
        //Create twitter client

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100);
        Client hosebirdClient = createTwitterClient(msgQueue);
        // Attempts to establish a connection.
        hosebirdClient.connect();

        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down the application");
            logger.info("Stopping client");
            hosebirdClient.stop();
            logger.info("Shutting down the producer");
            kafkaProducer.close();
        }));

        // on a different thread, or multiple different threads....
            while (!hosebirdClient.isDone()) {
                String msg = null;
                try {
                    msg = msgQueue.poll(5, TimeUnit.SECONDS);
                }catch(InterruptedException ex){
                    ex.printStackTrace();
                    hosebirdClient.stop();
                    logger.error(ex.toString());
                }
                if(null!=msg){
                    logger.info(msg);
                    kafkaProducer.send(new ProducerRecord<>("TwitterTweet", null, msg), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if(e!=null){
                                e.printStackTrace();
                                logger.error("Exception happened : " + e);
                            }
                        }
                    });
                }
            }
            logger.info("End of the application");
        //kafka producer


        //loop to send message to kafka
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("kafka","java","america","suntv","tesla","apple","microsoft","ibm");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(TwitterProducer.API_KEY, TwitterProducer.API_KEY_SECRET, TwitterProducer.ACCESS_TOKEN, TwitterProducer.ACCESS_TOKEN_SECRET);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();

        return hosebirdClient;
    }

    /**
     * Kafka Producer
     * @return
     */
    public KafkaProducer<String, String> createKafkaProducer(){
        Properties properties = new Properties();

        ProducerRecord<String, String> record = null;

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,String.valueOf(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");

        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));


        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        return producer;
    }

}

package com.webbfontaine.kafka.twitter;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public class TwitterProducer {

    private static final String API_KEY = "HCMmkEXH6douaiDYSZVg5KNeV";
    private static final String API_SECRET = "ZUA1qjCuGUsmMWSm7WhAq0CBOfg1Q3gnNb5bwvBWxkQI6NBlWk";
    private static final String ACCESS_TOKEN = "2611730924-RNkNKJdfvNdTr9v7GiBC2TG9TbKlBdZuik9fxLR";
    private static final String ACCESS_TOKEN_SECRET = "TyT6qOpUukQhpq67unCLzFG8euZkG7mxpcNDHw8h2ohN2";

    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String TOPIC_NAME = "twitter_tweets";

   private static final List<String> TERMS = List.of("kafka", "api", "java", "Armenia", "Obama", "Putin");

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        log.info("Setup Twitter producer");

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);

        Client client = createTwitterClient(msgQueue);
        client.connect();

        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Stopping application...");
            client.stop();
            kafkaProducer.close();
            log.info("stopping client and producer!");
        }));

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error("Error during poll", e);
                client.stop();
            }
            if (msg != null) {
                log.info("message: {}", msg);
                kafkaProducer.send(new ProducerRecord<>(TOPIC_NAME, 3, null, msg), (metadata, e) -> {
                    if (null != e) {
                        log.error("Smth wrong happened: ", e);
                    }
                });
            }
        }
        log.info("End of application");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(TERMS);

        Authentication hosebirdAuth = new OAuth1(API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET);

        ClientBuilder builder = new ClientBuilder()
            .name("Hosebird-Client-01")
            .hosts(hosebirdHosts)
            .authentication(hosebirdAuth)
            .endpoint(hosebirdEndpoint)
            .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    public KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
//        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
//        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
//        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // default value of producer timeout is: [delivery.timeout.ms = 120000]

        //high throughput producer
//        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
//        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
//        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); //32 KB

        return new KafkaProducer<>(properties);
    }
}

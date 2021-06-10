package com.webbfontaine.elasticsearch;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

@Slf4j
public class ElasticSearchConsumer {
    // Full access "https://pazuq4mwys:g2usvbxk77@kafka-learning-9321846693.eu-central-1.bonsaisearch.net:443"
    private static final String BONSAI_HOSTNAME = "kafka-learning-9321846693.eu-central-1.bonsaisearch.net";
    private static final String USERNAME = "pazuq4mwys";
    private static final String PASSWORD = "g2usvbxk77";
    private static final int BONSAI_PORT = 443;
    private static final String BONSAI_SCHEMA = "https";

    private static final String LOCAL_HOSTNAME = "localhost";
    private static final int LOCAL_PORT = 9200;
    private static final String LOCAL_SCHEMA = "http";
    private static final String INDEX = "twitter";

    private static final JsonParser JSON_PARSER = new JsonParser();

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = createBonsaiClient();

        String topic = "twitter_tweets";

        KafkaConsumer<String, String> consumer = createConsumer(topic);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            Integer recordsCount = records.count();

            log.info("Received: {} records", recordsCount);

            BulkRequest bulkRequest = new BulkRequest();
            for (ConsumerRecord<String, String> record : records) {
                //kafka 2 strategies
                //generic ID
                //String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                try {
                    //tweeter feed specific id
                    String id = extractIdFromTweet(record.value());

                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    String jsonString = record.value();

                    IndexRequest indexRequest = new IndexRequest(INDEX)
                        .id(id)
                        .source(jsonString, XContentType.JSON);

                    bulkRequest.add(indexRequest);
                } catch (NullPointerException ex) {
                    log.warn("Skipping data ", ex);
                }
            }

            if (recordsCount > 0) {
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                log.info("Committing offset...");
                consumer.commitSync();
                log.info("Offset has been committing...");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

//        client.close();
    }

    private static String extractIdFromTweet(String tweetJson) {
        return JSON_PARSER.parse(tweetJson)
            .getAsJsonObject()
            .get("id_str")
            .getAsString();
    }

    private static RestHighLevelClient createBonsaiClient() {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(USERNAME, PASSWORD));
        RestClientBuilder builder = RestClient.builder(new HttpHost(BONSAI_HOSTNAME, BONSAI_PORT, BONSAI_SCHEMA))
            .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        return new RestHighLevelClient(builder);
    }

    private static RestHighLevelClient createLocalClient() {
        return new RestHighLevelClient(RestClient.builder(new HttpHost(LOCAL_HOSTNAME, LOCAL_PORT, LOCAL_SCHEMA)));
    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "kafka-demo-elastic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));

        return consumer;
    }
}

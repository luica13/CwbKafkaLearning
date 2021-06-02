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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import org.apache.kafka.clients.consumer.KafkaConsumer;

@Slf4j
public class ElasticSearchConsumer {
    // Full access "https://pazuq4mwys:g2usvbxk77@kafka-learning-9321846693.eu-central-1.bonsaisearch.net:443"
    private static final String HOSTNAME = "kafka-learning-9321846693.eu-central-1.bonsaisearch.net";
    private static final String USERNAME = "pazuq4mwys";
    private static final String PASSWORD = "g2usvbxk77";
    private static final int PORT = 443;
    private static final String SCHEMA = "https";
    private static final String INDEX = "twitter";

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = createClient();

        String topic = "twitter_tweets";

        KafkaConsumer<String, String> consumer = createConsumer(topic);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                //kafka 2 strategies
                //generic ID
//                String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                //tweeter feed specific id
                String id = extractIdFromTweet(record.value());

                log.info("Key: " + record.key() + ", Value: " + record.value());
                log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                String jsonString = record.value();

                IndexRequest indexRequest = new IndexRequest(INDEX).id(id).source(jsonString, XContentType.JSON);
                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                String resultId = indexResponse.getId();
                log.info("Index id: {}", resultId);

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

//        client.close();
    }

    private static JsonParser jsonParser = new JsonParser();

    private static String extractIdFromTweet(String tweetJson) {
        return jsonParser.parse(tweetJson)
            .getAsJsonObject()
            .get("id_str")
            .getAsString();

    }

    private static RestHighLevelClient createClient() {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(USERNAME, PASSWORD));

        RestClientBuilder builder = RestClient.builder(new HttpHost(HOSTNAME, PORT, SCHEMA))
            .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        return new RestHighLevelClient(builder);
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

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));

        return consumer;
    }
}

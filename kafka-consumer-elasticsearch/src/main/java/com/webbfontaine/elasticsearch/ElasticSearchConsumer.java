package com.webbfontaine.elasticsearch;

import java.io.IOException;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

@Slf4j
public class ElasticSearchConsumer {
    // Full access "https://pazuq4mwys:g2usvbxk77@kafka-learning-9321846693.eu-central-1.bonsaisearch.net:443"
    private static final String HOSTNAME = "kafka-learning-9321846693.eu-central-1.bonsaisearch.net";
    private static final String USERNAME = "pazuq4mwys";
    private static final String PASSWORD = "g2usvbxk77";
    private static final int PORT = 443;
    private static final String SCHEMA = "https";
    private static final String INDEX = "twitter";
    private static final String JSON_DATA = """
        {
            "foo": "bar"
        }
        """;

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = createClient();

        IndexRequest indexRequest = new IndexRequest(INDEX).source(JSON_DATA, XContentType.JSON);

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        String id = indexResponse.getId();
        log.info("Index id: {}", id);

        client.close();
    }

    private static RestHighLevelClient createClient() {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(USERNAME, PASSWORD));

        RestClientBuilder builder = RestClient.builder(new HttpHost(HOSTNAME, PORT, SCHEMA))
            .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        return new RestHighLevelClient(builder);
    }
}

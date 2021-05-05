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

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = createClient();

        String json = "{ \"foo\": \"bla bla\" }";

        IndexRequest indexRequest = new IndexRequest("twitter")
            .source(json, XContentType.JSON);

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        String id = indexResponse.getId();

        log.info("Index id: {}", id);

        client.close();
    }

    private static RestHighLevelClient createClient() {
//         Full access "https://pazuq4mwys:g2usvbxk77@kafka-learning-9321846693.eu-central-1.bonsaisearch.net:443"
        String hostname = "kafka-learning-9321846693.eu-central-1.bonsaisearch.net";
        String username = "pazuq4mwys";
        String password = "g2usvbxk77";

        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
            .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        return new RestHighLevelClient(builder);
    }
}

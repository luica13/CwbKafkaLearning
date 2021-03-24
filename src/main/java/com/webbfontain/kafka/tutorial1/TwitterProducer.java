package com.webbfontain.kafka.tutorial1;

import java.util.List;
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

@Slf4j
public class TwitterProducer {

    private static final String API_KEY = "HCMmkEXH6douaiDYSZVg5KNeV";
    private static final String API_SECRET = "ZUA1qjCuGUsmMWSm7WhAq0CBOfg1Q3gnNb5bwvBWxkQI6NBlWk";
    private static final String ACCESS_TOKEN = "2611730924-RNkNKJdfvNdTr9v7GiBC2TG9TbKlBdZuik9fxLR";
    private static final String ACCESS_TOKEN_SECRET = "TyT6qOpUukQhpq67unCLzFG8euZkG7mxpcNDHw8h2ohN2";

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        log.info("Setup Twitter producer");

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);

        Client client = createTwitterClient(msgQueue);
        client.connect();

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
            }
        }
        log.info("End of application");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        List<String> terms = List.of("kafka", "api", "java");
        hosebirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET);

        ClientBuilder builder = new ClientBuilder()
            .name("Hosebird-Client-01")
            .hosts(hosebirdHosts)
            .authentication(hosebirdAuth)
            .endpoint(hosebirdEndpoint)
            .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }
}

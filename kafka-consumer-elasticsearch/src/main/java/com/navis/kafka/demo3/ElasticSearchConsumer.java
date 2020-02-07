package com.navis.kafka.demo3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    public static RestHighLevelClient createClient() {
//        https://5bsvkh3g8a:hdkzrsff3c@kafka-course-5814904393.ap-southeast-2.bonsaisearch.net:443
        String hostName = "kafka-course-5814904393.ap-southeast-2.bonsaisearch.net";
        String userName = "5bsvkh3g8a";
        String password = "hdkzrsff3c";

        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(userName, password));
        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostName, 443, "https")
        ).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });

        return new RestHighLevelClient(builder);
    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {
        Properties properties = new Properties();
        String bootstrapServer = "localhost:9092";
        String groupId = "elasticsearch-consumer-app-4";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");


        //create a Kafka Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    private static String extractIdFromTweet(String tweetJson) {
        return new JsonParser().
                parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();

    }


    public static void main(String[] args) throws IOException {
//        String jsonString = "{" +
//                "\"user\":\"Tapas\"," +
//                "\"message\":\"Learning Elasticsearch\"" +
//                "}";
        RestHighLevelClient client = createClient();

        KafkaConsumer<String, String> consumer = createConsumer("twitter-tweets");
        //poll for new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            LOGGER.info("Received " + records.count() + " records..");
            for (ConsumerRecord<String, String> record : records) {
                /**To make the consumer idempotent, we need to send an Id along with the message
                 *Two ways
                 * 1.Kafka generic Id
                 * String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                 * 2. twitter feed specific id.
                 **/
                try {
                    String id = extractIdFromTweet(record.value());
                    IndexRequest indexRequest = new IndexRequest("twitter-test")
                            .id(id)
                            .source(record.value(), XContentType.JSON);
                    IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

                    LOGGER.info("id:: " + indexResponse.getId());
                } catch (NullPointerException e) {
                    LOGGER.warn("Skipping bad data:: " + record.value());
                }
            }
            LOGGER.info("Committing Offsets...");
            consumer.commitSync();
            LOGGER.info("Offsets have been committed");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }
}

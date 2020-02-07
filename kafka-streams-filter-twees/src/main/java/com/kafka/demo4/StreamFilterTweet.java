package com.kafka.demo4;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamFilterTweet {

    public static void main(String[] args) {

        String bootstrapServer = "localhost:9092";
        String applicationId = "demo-kafka-streams";

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());


        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> inputTopic = streamsBuilder.stream("twitter-tweets");
        inputTopic.filter((k, jsonTweet) -> extractFollowersOfUserFromTweet(jsonTweet) > 10000
        ).to("popular-tweets");

        KafkaStreams kafkaStreams = new KafkaStreams(
                streamsBuilder.build(),
                properties
        );

        kafkaStreams.start();
    }

    private static Integer extractFollowersOfUserFromTweet(String jsonTweet) {
        JsonParser jsonParser = new JsonParser();

        try {
            return jsonParser.parse(jsonTweet)
                    .getAsJsonObject()
                    .get("user").getAsJsonObject()
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch (Exception e) {
            return 0;
        }
    }
}

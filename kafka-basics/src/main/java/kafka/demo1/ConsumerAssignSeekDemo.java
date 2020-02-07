package kafka.demo1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerAssignSeekDemo {
    public static final Logger LOGGER = LoggerFactory.getLogger(ConsumerAssignSeekDemo.class);

    public static void main(String[] args) {

        Properties properties = new Properties();
        String bootstrapServer = "localhost:9092";
        String topic = "new-topic";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create a Kafka Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //assign and seek are mostly used to replay data or fetch a specific message
        //assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        consumer.assign(Collections.singleton(partitionToReadFrom));
        Long offsetToReadFrom = 587L;
        //seek
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        boolean keepReading = true;
        int numberOfMessagesToRead = 5;
        int numberOfMessagesReadSoFar = 0;
        //poll for new data
        while (keepReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesReadSoFar++;
                LOGGER.info("Key:  " + record.key()
                        + "\n Value:: " + record.value()
                        + "\n Partition:: " + record.partition()
                        + "\n Offset:: " + record.offset());
                if (numberOfMessagesReadSoFar == numberOfMessagesToRead) {
                    keepReading = false;
                    break;
                }
            }
        }
        LOGGER.info("Exiting the application");
    }
}

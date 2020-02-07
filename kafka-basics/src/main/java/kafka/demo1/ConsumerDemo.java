package kafka.demo1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    public static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {

        Properties properties = new Properties();
        String bootstrapServer = "localhost:9092";
        String groupId = "connectapp";
        String topic = "popular-tweets";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        //create a Kafka Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        //poll for new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                LOGGER.info("Key:  " + record.key()
                        + "\n Value:: " + record.value()
                        + "\n Partition:: " + record.partition()
                        + "\n Offset:: " + record.offset());
                consumer.commitAsync();
            }
        }

    }
}

package kafka.demo1;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerDemoWithKey {

    public static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerDemoWithKey.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String bootstrapServer = "localhost:9092";
        final Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        //

        for (int i = 11; i <= 40; i++) {
            String topic = "new-topic";
            String value = "Tapas" + i;
            String key = "id_"+ i;

            LOGGER.info("Key:: "+ key);
            ProducerRecord<String, String> record = new ProducerRecord(topic, key, value);
//            final int finalI = i;
            kafkaProducer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        LOGGER.info("MetaData received. \n" +
                                "Topic:: " + recordMetadata.topic() + "\n" +
                                "Partition:: " + recordMetadata.partition() + "\n" +
                                "Offset:: " + recordMetadata.offset() + "\n"
//                                +"Message:: test-" +     finalI
                        );
                    } else {
                        LOGGER.error("Error. Could not produce: " + e);
                    }
                }
            });
        }

        kafkaProducer.flush();
        kafkaProducer.close();
    }

}


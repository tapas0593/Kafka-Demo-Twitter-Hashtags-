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

public class KafkaProducerDemoWithCallBack {

    public static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerDemoWithCallBack.class);

    public static void main(String[] args) {
        String bootstrapServer = "localhost:9092";
        final Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);


        for (int i = 6; i <= 7; i++) {
            ProducerRecord<String, String> record = new ProducerRecord("new-topic", "tapas-" + Integer.toString(i));
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


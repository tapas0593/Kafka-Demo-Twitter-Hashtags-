package kafka.demo1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerThreadDemo {
    public static final Logger LOGGER = LoggerFactory.getLogger(ConsumerThreadDemo.class);

    public static void main(String[] args) {

        String bootstrapServer = "localhost:9092";
        String groupId = "connectapp";
        String topic = "popular-tweets";
        CountDownLatch latch = new CountDownLatch(1);
        LOGGER.info("Creating Consumer Thread");
        Runnable myConsumerThread = new ConsumerRunnable(bootstrapServer,groupId, topic, latch);
        Thread myThread = new Thread(myConsumerThread);
        myThread.start();

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook( new Thread( () -> {
            LOGGER.info("Caught Shut Down Hook");
            ((ConsumerRunnable) myConsumerThread).shutDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                LOGGER.info("Application has exited");
            }

        }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            LOGGER.info("Application is closing");
        }
    }

    public static class ConsumerRunnable implements Runnable {

        private Logger LOGGER =LoggerFactory.getLogger(ConsumerRunnable.class);
        private KafkaConsumer<String, String> consumer;
        private CountDownLatch latch;

        public ConsumerRunnable(String bootstrapServer,
                                String groupId,
                                String topic,
                                CountDownLatch latch) {
            this.latch = latch;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            //create a Kafka Consumer
            consumer = new KafkaConsumer<String, String>(properties);

            //subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {
                //poll for new data
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        LOGGER.info("Key:  " + record.key()
                                + "\n Value:: " + record.value()
                                + "\n Partition:: " + record.partition()
                                + "\n Offset:: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                LOGGER.info("Received Shut Down Signal");
            } finally {
                consumer.close();
                //tell the main code that we are done with consumer
                latch.countDown();
            }
        }

        public void shutDown() {
            //method to interrupt consumer.poll()
            //throws WakeUpException
        consumer.wakeup();
        }
    }
}

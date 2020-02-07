package kafka.demo2;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterProducer.class);
    private String consumerKey = "M7o2GPGOMtIqGuyCe3i7BBdbN";
    private String consumerSecret = "4FRzr0MF4Z7QHaSiKs1QKXy0QGUCG0RwUNiL7EABhxEy5yPMMV";
    private String accessToken = "479216074-ggI413AAr9oNlzjaf0oz5SoK5OyX4yDiq9K2mdVT";
    private String accessSecret = "iD96q07M1u9YyqhPxGb8n8DtpfG0fYGks7xIenBHj2Ou6";

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    private void run() {
        LOGGER.info("Setup Starts");
        //setup a blocking queue with a capacity to store messages
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);

        //create a twitter client
        Client client = createTwitterClient(msgQueue);

        // Attempts to establish a connection.
        client.connect();
        //create a kafka producer
        KafkaProducer<String, String> kafkaProducer = getKafkaProducer();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Received ShutDown Hook.....\n Shutting down Client from Kafka.....");
            client.stop();
            LOGGER.info("Closing kafka Producer.....");
            kafkaProducer.close();
            LOGGER.info("DONE.....");
        }));
        //loop to send tweets to kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                LOGGER.info("Line 71" + msg);
                try {
                    LOGGER.info("################# Message started flowing ...........................................");
                    kafkaProducer.send(new ProducerRecord<>("twitter-tweets1", msg), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e != null) {
                                LOGGER.error("Error:::" + e);
                            } else {
                                LOGGER.info("MetaData received. \n" +
                                        "Topic:: " + recordMetadata.topic() + "\n" +
                                        "Partition:: " + recordMetadata.partition() + "\n" +
                                        "Offset:: " + recordMetadata.offset() + "\n");
                            }
                        }
                    });
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        LOGGER.info("End of Application");
    }

    /**
     * This method builds a twitter client
     *
     * @param msgQueue message queue
     * @return Client
     */
    public Client createTwitterClient(BlockingQueue<String> msgQueue) {


        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();

        List<String> termsFollowed = Arrays.asList("tech");
        endpoint.trackTerms(termsFollowed);

        //authentication
        Authentication authentication = new OAuth1(consumerKey, consumerSecret, accessToken, accessSecret);


        //creating a client with the above defined authentication, host, name, processor and endpoints
        ClientBuilder clientBuilder = new ClientBuilder()
                .authentication(authentication)
                .hosts(hosts)
                .name("HoseBird Client-1")
                .processor(new StringDelimitedProcessor(msgQueue))
                .endpoint(endpoint);

        Client client = clientBuilder.build();
        return client;
    }

    public KafkaProducer<String, String> getKafkaProducer() {
        String bootstrapServer = "localhost:9092";
        //producer properties
        final Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //safe producer
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        //high throughput producer(in expense of a bit of latency and CPU Usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));

        return new KafkaProducer<String, String>(properties);
    }

}

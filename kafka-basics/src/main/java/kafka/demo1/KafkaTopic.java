package kafka.demo1;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;


public class KafkaTopic {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTopic.class);


    public static void main(String[] args) {
        Properties properties = new Properties();
        String bootstrapServer = "localhost:9092";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        AdminClient adminClient = AdminClient.create(properties);
        listAllTopics(adminClient);
        checkTopic("twitter_status_connect", adminClient);

    }

    private static ListTopicsResult listAllTopics(AdminClient adminClient) {
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        try {
            LOGGER.info(String.valueOf(listTopicsResult.names().get()));
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return listTopicsResult;
    }

    public static boolean checkTopic(String inTopicName, AdminClient adminClient) {
        ListTopicsResult listTopicsResult = listAllTopics(adminClient);
        Set<String> topicNamesList = null;
        try {
            topicNamesList = listTopicsResult.names().get();
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("Error:: " + e);
        }
        if (topicNamesList == null) {
            LOGGER.warn("No topics present in Kafka");
        } else {
            if (topicNamesList.contains(inTopicName)) {
                LOGGER.warn("Topic " + inTopicName + " is already present in Kafka.");
                return true;
            }
        }
        LOGGER.warn("Topic " + inTopicName + " is not present in Kafka.");
        return false;
    }

}

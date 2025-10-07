package main.java.sbx;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.Properties;

public class KafkaTopicListerWithSSL {
    public static void main(String[] args) {

        String bootstrapServers = "stpimc-00kfp002.innodev.local:9092,stpimc-00kfp003.innodev.local:9092,stpimc-00kfp004.innodev.local:9092";

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "kafka-topic-lister");
        props.put("security.protocol", "SSL");
        props.put("ssl.truststore.location", "C:\\Users\\vshalaev\\Documents\\PROJECT\\Kafka\\Offset_explorer\\kafka.truststore.jks");
        props.put("ssl.truststore.password", "truststore-password");
        props.put("ssl.keystore.location", "C:\\Users\\vshalaev\\Documents\\PROJECT\\Kafka\\Offset_explorer\\kafka.keystore.jks");
        props.put("ssl.keystore.password", "wAUyWEpL<>tWU9VxcAipcZP");
//        props.put("ssl.key.password", "key-password");

        try (AdminClient admin = AdminClient.create(props)) {
            admin.listTopics().names().get().forEach(System.out::println);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
package main.java.sbx;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

public class KafkaConsumerGroupApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerGroupApp.class);


    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092, localhost:39092, localhost:49092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // String must be one of: latest, earliest, none
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group-id");
        properties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "my-instance-id");


        try(Consumer<String, String> consumer= new KafkaConsumer<String, String>(properties)){

            consumer.subscribe(Pattern.compile("sandbox"), new MyConsumerRebalanceListener());

            ConsumerRecords<String, String> records =  consumer.poll(Duration.ofSeconds(30));

            StreamSupport.stream(records.spliterator(), false)
                    .forEach(rec -> {
                                LOGGER.info(" key - {}", rec);
                                LOGGER.info(" Record: key {}, value {}", rec.key(), rec.value());
                            }
                    );

        }

    }

}


class MyConsumerRebalanceListener implements ConsumerRebalanceListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerGroupApp.class);

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions){
        LOGGER.info("Partitions revoked: {}", partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions){
        LOGGER.info("Partitions assigned: {}", partitions);
    }


}





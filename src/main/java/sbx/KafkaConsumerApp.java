package main.java.sbx;


import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;


import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.StreamSupport;

public class KafkaConsumerApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerApp.class);


    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092, localhost:39092, localhost:49092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // String must be one of: latest, earliest, none


        try(Consumer<String, String> consumer= new KafkaConsumer<String, String>(properties)){

//            List <TopicPartition> topicPartitions = new ArrayList<>(Arrays.asList(new TopicPartition("sandbox", 0),
//                    new TopicPartition("sandbox", 0),
//                    new TopicPartition("sandbox", 0)));

            consumer.assign(
                    Arrays.asList(
                            new TopicPartition("sandbox", 0),
                            new TopicPartition("sandbox", 1),
                            new TopicPartition("sandbox", 2))
            );


//            ConsumerRecords<String, String> records =  consumer.poll(50);
            ConsumerRecords<String, String> records =  consumer.poll(Duration.ofSeconds(20));

            StreamSupport.stream(records.spliterator(), false)
                    .forEach(rec -> {
                                LOGGER.info(" key - {}", rec);
                                LOGGER.info(" Record: key {}, value {}", rec.key(), rec.value());
                    }
                    );

        }

    }



}

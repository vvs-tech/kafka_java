package main.java.sbx.tmp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Tmp {

    private static final Logger LOGGER = LoggerFactory.getLogger(Tmp.class);


    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();



        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092, localhost:39092, localhost:49092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.put(ProducerConfig.ACKS_CONFIG, "0");

        try(Producer<String, String> producer= new KafkaProducer<String, String>(properties)){
            producer.send(new ProducerRecord<>("sandbox", "Hello java 8"));
            RecordMetadata metadata =  producer.send(new ProducerRecord<>("sandbox", "Hello java 9")).get();

            LOGGER.info("==========================================");
            LOGGER.info("Metadata {}", metadata);
            LOGGER.info("==========================================");


        }

//        test1

    }
}

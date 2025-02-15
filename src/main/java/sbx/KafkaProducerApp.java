package main.java.sbx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerApp.class);


    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092, localhost:39092, localhost:49092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.put(ProducerConfig.ACKS_CONFIG, "0");

        try(Producer<String, String> producer= new KafkaProducer<String, String>(properties)){

//          Отправить сообщение в топик sandbox
            producer.send(new ProducerRecord<>("sandbox", "Hello java 7"));


//          Отправить сообщение в топик sandbox и получить метаданные сообщения
            RecordMetadata metadata =  producer.send(new ProducerRecord<>("sandbox", "Hello java 9")).get();
            LOGGER.info("=============== Получить метаданные сообщения ===========================");
            LOGGER.info("Metadata {}", metadata);
            LOGGER.info("================ Получить метаданные сообщения ==========================");
//==========================================
//Metadata sandbox-1@1    //в логе в полученной метадате указан топик sandbox, партиция 1, и  смещение offset 1
//==========================================

            RecordMetadata metadataKey =  producer.send(new ProducerRecord<>("sandbox", "key-1","Hello java key-1 Msg: 1")).get();
            LOGGER.info("================== Получить метаданные сообщения с указанным ключом ========================");
            LOGGER.info("Metadata {}", metadataKey);
            LOGGER.info("================== Получить метаданные сообщения с указанным ключом  ========================");


        }

    }
}

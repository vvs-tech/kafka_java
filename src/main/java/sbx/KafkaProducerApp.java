package main.java.sbx;


import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;


import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaProducerApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerApp.class);


    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092, localhost:39092, localhost:49092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.put(ProducerConfig.ACKS_CONFIG, "0");
//        0  - не ждать подтверждения
//        1  - ждать подтверждения записи только от лидирующей партиции
//        -1 - ждать подтверждения записи от всех реплик партиции

        try(Producer<String, String> producer= new KafkaProducer<String, String>(properties)){

            // Отправить сообщение в топик sandbox
            producer.send(new ProducerRecord<>("sandbox", "Hello java 7"));


            // Отправить сообщение в топик sandbox и получить метаданные сообщения
            RecordMetadata metadata =  producer.send(new ProducerRecord<>("sandbox", "Metadata 1")).get();
            LOGGER.info("=============== Получить метаданные сообщения ===========================");
            LOGGER.info("Metadata {}", metadata);
            LOGGER.info("================ Получить метаданные сообщения ==========================");
            //==========================================
            //Metadata sandbox-0@1    //в логе в полученной метадате указан топик sandbox, партиция 0, и  смещение offset 1
            //==========================================

            // Метод send с Callback
            RecordMetadata metadata2 =  producer.send(new ProducerRecord<>("sandbox", "Metadata 2 Callback"), (md, exception) -> {
                LOGGER.info("Callback: metadata: {}, exception == null: {}", md, exception);
            }).get();
            LOGGER.info("=============== Получить метаданные сообщения/ Метод send с Callback ===========================");
            LOGGER.info("Metadata {}", metadata2);
            LOGGER.info("================ Получить метаданные сообщения/Метод send с Callback ==========================");


            RecordMetadata metadataKey =  producer.send(new ProducerRecord<>("sandbox", "key-1","Metadata key-1 Msg: 2")).get();
            LOGGER.info("================== Получить метаданные сообщения с указанным ключом ========================");
            LOGGER.info("Metadata {}", metadataKey);
            LOGGER.info("================== Получить метаданные сообщения с указанным ключом  ========================");


            RecordMetadata metadataKeyPartition =  producer.send(new ProducerRecord<>("sandbox", 2, "key-1","Metadata Partition 2, key-1 Msg: 3")).get();
            LOGGER.info("================== Получить метаданные сообщения с указанным ключом и партицией========================");
            LOGGER.info("Metadata {}", metadataKeyPartition);
            LOGGER.info("================== Получить метаданные сообщения с указанным ключом и партицией  ========================");


            List <Header> headers1 = new ArrayList<>();
            headers1.add(new RecordHeader("Foo", "Bar".getBytes()));
            RecordMetadata metadataKeyPartitionHeader =  producer.send(new ProducerRecord<>("sandbox", 2, "key-1","Metadata Partition 2, key-1 Msg: 3, Header 1",
                    headers1)).get();
            LOGGER.info("================== Получить метаданные сообщения с указанным ключом, партицией и заголовком ========================");
            LOGGER.info("Metadata {}", metadataKeyPartitionHeader);
            LOGGER.info("================== Получить метаданные сообщения с указанным ключом, партицией и заголовком ========================");


            RecordMetadata metadataTimeStampKeyPartition =  producer.send(new ProducerRecord<>("sandbox", 2, System.currentTimeMillis(),
                    "key-2","Metadata Partition 2, TimeStamp 1, key-2 Msg: 4")).get();
            LOGGER.info("================== Получить метаданные сообщения с указанным ключом, партицией и TimeStamp ========================");
            LOGGER.info("Metadata {}", metadataTimeStampKeyPartition);
            LOGGER.info("================== Получить метаданные сообщения с указанным ключом, партицией и TimeStamp  ========================");


            List <Header> headers2 = new ArrayList<>();
            headers2.add(new RecordHeader("Foo", "Bar".getBytes()));
            RecordMetadata metadataTimeStampKeyPartitionHeader =  producer.send(new ProducerRecord<>("sandbox", 2, System.currentTimeMillis(),
                    "key-2","Metadata Partition 2, TimeStamp 2, key-2 Msg: 5, Header 3",
                    headers2)).get();
            LOGGER.info("================== Получить метаданные сообщения с указанным ключом, партицией, TimeStamp и заголовком ========================");
            LOGGER.info("Metadata {}", metadataTimeStampKeyPartitionHeader);
            LOGGER.info("================== Получить метаданные сообщения с указанным ключом, партицией, TimeStamp и заголовком ========================");



        }

    }
}

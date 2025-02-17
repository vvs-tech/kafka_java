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

public class KafkaProducerIdempotenceApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerIdempotenceApp.class);


    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092, localhost:39092, localhost:49092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "0");
//       ACKS -   0  - не ждать подтверждения (Если посмотреть Callback, получим сообщение 'sandbox-2@-1',где указ топик sandbox, партиция 2, а вот offset неизвестен, т.е. его значение -1. Происходит это из-за
//                      того, что мы не ждем подтверждения о записи в партицию, соответственно offset на момент отправки неизвестен)
//       ACKS -   1  - ждать подтверждения записи только от лидирующей партиции
//       ACKS -   -1 - ждать подтверждения записи от всех реплик партиции


        try(Producer<String, String> producer= new KafkaProducer<String, String>(properties)) {

            // Отправить сообщение в топик sandbox
            producer.send(new ProducerRecord<>("sandbox", "Ident msg 1"), (md, exception) -> {
                LOGGER.info("Callback: metadata: {}, exception == null: {}", md, exception);
            });

        }



    }

}

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

public class KafkaProducerTransactionApp {
//    Транзакционная отправка сообщений

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerApp.class);


    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092, localhost:39092, localhost:49092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-transaction-id-1");  //Идентификатор транзакции должен быть уникальным в рамках кластера
        // Теперь если отправить сообщение без транзакции, то получим TimeOutException, т.к. ожидается возникновение транзакции


        try(Producer<String, String> producer= new KafkaProducer<String, String>(properties)){

            producer.initTransactions(); // Инициализация транзакции

//            producer.send(new ProducerRecord<>("sandbox", "non Transaction Msg 0"));   // Этот код не сработает, т.к. в свойствах задана транзакционная отправка. Поэтому отправка сообщения без транзакции не выполнится

            producer.beginTransaction(); // Начало транзакции
            producer.send(new ProducerRecord<>("sandbox", "Transaction 1, Msg 3"));
            producer.send(new ProducerRecord<>("sandbox", "Transaction 1, Msg 4"));
            producer.commitTransaction(); // Коммит транзакции


        }



    }



}

package main.java.sbx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;


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

//            producer.send(new ProducerRecord<>("sandbox", "non Transaction Msg 0"));   // Этот код не сработает, т.к. проинициализирована транзакционная отправка. Поэтому отправка сообщения без транзакции не выполнится

            producer.beginTransaction(); // Начало транзакции
            producer.send(new ProducerRecord<>("sandbox", "Transaction 1, Msg 7"));
            producer.send(new ProducerRecord<>("sandbox", "Transaction 1, Msg 8"));
            producer.commitTransaction(); // Коммит транзакции

            // Сообщения в рамках транзакции можно писать в разные партиции
            // Несмотря на то,что для 2го и 3го сообщения указаны одинаковые ключи, сообщения будут записаны в разные партиции, т.к. партиции для этих сообщений указаны разные
            producer.beginTransaction(); // Начало транзакции
            producer.send(new ProducerRecord<>("sandbox",0, "transact-key-1", "Transaction 2, Msg 20"));
            producer.send(new ProducerRecord<>("sandbox", 1,"transact-key-2","Transaction 2, Msg 21"));
            producer.send(new ProducerRecord<>("sandbox", 2,"transact-key-2","Transaction 2, Msg 31"));
            producer.commitTransaction(); // Коммит транзакции


        }



    }



}

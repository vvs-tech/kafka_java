package main.java.sbx;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.stream.StreamSupport;

public class KafkaConsumerSeekApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerSeekApp.class);


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


            // 1. Получить сообщения из конкретной партиции с указанного смещения
            consumer.seek(new TopicPartition("sandbox", 2), 0);

            // 2. Получить сообщения из конкретной партиции с начального смещения
//            consumer.seekToBeginning(Arrays.asList(new TopicPartition("sandbox", 1)));


            // 3. Перейти на последнее смещение партиции
//            consumer.seekToEnd(Arrays.asList(new TopicPartition("sandbox", 1) ));


            // 4. Поиск смещения по метке времени
//            Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(Collections.singletonMap(new TopicPartition("sandbox", 1), 1740066484902L));  // Значение Long заменить на значение метки времени конкретного сообщения партиции
//            consumer.seek(new TopicPartition("sandbox", 1), offsets.get(new TopicPartition("sandbox", 1)).offset());



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

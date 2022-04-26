package io.fahmi.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerShutdownDemo {
    private static Logger log = LoggerFactory.getLogger(ConsumerShutdownDemo.class.getSimpleName());

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "topic1-consumer-group1");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutting down");
            consumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));

        consumer.subscribe(Collections.singleton("topic1"));

        try {
            while (true) {
                log.info("pooling");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("key: {}, value: {}, partition: {}", record.key(), record.value(), record.partition());
                }
            }
        } catch (WakeupException e) {
            log.info("Wakeup exception!");
        } catch (Exception e){
            log.error("unexpected error");
        }finally {
            consumer.close();
        }

    }
}

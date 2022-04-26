package io.fahmi.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerCallbackKeyDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerCallbackKeyDemo.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        log.info("Hello world");

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>("topic1", "key" + i,"hello world " + i);
            int finalI = i;
            producer.send(producerRecord, (metadata, exception) -> {
                log.info("iteration: {}, topic: {}, offset: {}, partition: {}", finalI, metadata.topic(), metadata.offset(), metadata.partition());
            });

            Thread.sleep(1000);
        }


        producer.flush();

        // flush included in close
        producer.close();
    }
}

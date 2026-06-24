package org.szesto;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Future;

public class MessageWorker {
    private final Properties properties;

    public MessageWorker(Properties properties) {
        this.properties = properties;
    }

    public Future<RecordMetadata> sendMessage(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        return producer.send(record, (metadata, exception) -> {
            // log exception
        });
    }

    public RecordMetadata sendMessageBlocking(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        try {
            return sendMessage(producer, record).get();
        } catch (Exception e) {
            // check for kafka specific exceptions
            throw new RuntimeException(e);
        }
    }

    public void sendMessages(String topic) {
        InputWorker inputWorker = new InputWorker();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            String message = InputWorker.readStdin();
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, "key", message);
            RecordMetadata meta = sendMessageBlocking(producer, record);
        }
    }

    public static void main(String[] args) {
    }
}

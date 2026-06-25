package org.szesto;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.Future;

public class MessageWorker {

    public MessageWorker() {
    }

    public static RecordMetadata sendMessageBlocking(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        try {
            return producer.send(record).get();

        } catch (Exception e) {
            // check for kafka specific exceptions
            throw new RuntimeException(e);
        }
    }

    public static Future<RecordMetadata> sendMessageNonBlocking(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        return producer.send(record, (metadata, exception) -> {
            // log exception
        });
    }

    public static ProducerRecord<String, String> makeRecord(String topic, String key, String message) {
        return new ProducerRecord<>(topic, key, message);
    }

    public static Properties loadProperties(String propertiesFile) throws IOException {
        Properties props = new Properties();

        // load properties with classloader (resources)
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(propertiesFile)) {
            props.load(is);
        }

        // load properties from input file
        try (FileInputStream fis = new FileInputStream(propertiesFile)) {
            props.load(fis);
        }

        return props;
    }

    public static KafkaProducer<String, String> createProducer(Properties props) {

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return new KafkaProducer<>(props);
    }

    public static void main(String... args) throws IOException {
        final String path = "./message.json"; // args[1];

        final String propertiesFile = "producer.properties";

        final Properties props = loadProperties(propertiesFile);

        final String topic = props.getProperty("topic", "");

        // producer is thread safe and will be passed to worker threads
        try (KafkaProducer<String, String> producer = MessageWorker.createProducer(props)) {

            // apply reading strategy
            final String buf = InputWorker.readFile(path);

            final ProducerRecord<String, String> record = makeRecord(topic, "key", buf);

            // apply threading strategy
            final RecordMetadata meta = sendMessageBlocking(producer, record);

            System.out.println(meta.offset());
        }
    }
}

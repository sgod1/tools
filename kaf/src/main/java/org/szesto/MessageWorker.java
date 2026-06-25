package org.szesto;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Future;

public class MessageWorker {

    private static final Logger logger = LoggerFactory.getLogger(MessageWorker.class);

    public MessageWorker() {
    }

    public static RecordMetadata sendMessageBlocking(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        try {
            return producer.send(record).get();

        } catch (Exception e) {
            // check for kafka specific exceptions
            logger.error("Error while sending message", e);

            throw new RuntimeException(e);
        }
    }

    public static Future<RecordMetadata> sendMessageNonBlocking(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        return producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Callback notify: Error while sending message", exception);
            }
            else {
                logger.info("Callback notify: Message sent to topic {} partition {} offset {}", metadata.topic(), metadata.partition(), metadata.offset());
            }
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
        return new KafkaProducer<>(props);
    }

    public static void main(String... args) throws IOException {
        final String messageFile = "message.json";

        final String propertiesFile = "producer.properties";

        logger.info("Kafka properties file: {}, message file: {}", propertiesFile, messageFile);

        final Properties props = loadProperties(propertiesFile);

        final String topic = props.getProperty("topic", "");
        if (topic.isEmpty()) {
            throw new IllegalArgumentException("Topic is not defined in producer.properties file");
        }

        // producer is thread safe and will be passed to worker threads
        try (KafkaProducer<String, String> producer = MessageWorker.createProducer(props)) {

            // apply reading strategy
            final String buf = InputWorker.readFile(messageFile);

            final ProducerRecord<String, String> record = makeRecord(topic, null, buf);

            // apply threading strategy
            final RecordMetadata meta = sendMessageBlocking(producer, record);

            logger.info("Message sent to topic {} partition {} offset {}", meta.topic(), meta.partition(), meta.offset());
        }
    }
}

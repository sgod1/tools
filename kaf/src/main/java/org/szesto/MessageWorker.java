package org.szesto;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

public class MessageWorker {

    private static final Logger logger = LoggerFactory.getLogger(MessageWorker.class);

    public MessageWorker() {
    }

    public static RecordMetadata sendMessageBlocking(Semaphore sem, CountDownLatch latch, KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        try {
            sem.acquire();
            return producer.send(record).get();

        } catch (Exception e) {
            // check for kafka specific exceptions
            logger.error("Error while sending message", e);

            throw new RuntimeException(e);
        }
        finally {
            sem.release();
            latch.countDown();
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

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // load properties from input file
        try (FileInputStream fis = new FileInputStream(InputWorker.absolutePath(propertiesFile).toString())) {
            props.load(fis);
        }

        return props;
    }

    public static KafkaProducer<String, String> createProducer(Properties props) {
        return new KafkaProducer<>(props);
    }

    public static void checkInput(String propertiesFile, String messageFile) {

        logger.info("Kafka properties file: {}, message file: {}", propertiesFile, messageFile);

        if (InputWorker.fileMissing(propertiesFile)) {
            throw new IllegalArgumentException("File '" + propertiesFile + "' not found");
        }

        if (InputWorker.fileMissing(messageFile)) {
            throw new IllegalArgumentException("File '" + messageFile + "' not found");
        }
    }

    public static boolean messageLoop(String messageFile,  String topic, ExecutorService executor, KafkaProducer<String, String> producer) throws IOException {

        // one file or collection of files
        final String buf = InputWorker.readFile(messageFile);

        // batches of m messages

        // send m messages over time t
        // send m messages over time t for the duration d
        // send m messages

        final int maxMessages = 100;

        CountDownLatch latch = new CountDownLatch(maxMessages);
        Semaphore sem = new Semaphore(50);

        Map<Integer, Future<RecordMetadata>> futures = new HashMap<>();

        for (int mcount = 0; mcount < maxMessages; mcount++) {
            final ProducerRecord<String, String> record = makeRecord(topic, null, buf);

            Future<RecordMetadata> future = executor.submit(() -> sendMessageBlocking(sem, latch, producer, record));
            futures.put(future.hashCode(), future);
        }

        boolean complete = false;

        while (!complete) {
            try {
                complete = latch.await(100L, TimeUnit.MILLISECONDS);

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            Iterator<Integer> iter = futures.keySet().iterator();

            while (iter.hasNext()) {
                int k = iter.next();

                if (futures.get(k).isDone()) {
                    try {
                        RecordMetadata meta = futures.get(k).get();
                        logger.info("Message sent to topic {} partition {} offset {}", meta.topic(), meta.partition(), meta.offset());

                    } catch (ExecutionException | InterruptedException e) {
                        logger.error("Error sending message", e);

                        // recycle producer? return retry = true
                    }

                    iter.remove();
                }
            }
        }

        // no retry
        return false;
    }

    public static void main(String... args) throws IOException {

        final String messageFile = "message.json";
        final String propertiesFile = "producer.properties";

        checkInput(propertiesFile, messageFile);

        final Properties props = loadProperties(propertiesFile);

        final String topic = props.getProperty("topic", "");
        if (topic.isEmpty()) {
            throw new IllegalArgumentException("Topic is not defined in producer.properties file");
        }

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {

            boolean retry = true;

            while (retry) {

                try (KafkaProducer<String, String> producer = MessageWorker.createProducer(props)) {
                    retry = MessageWorker.messageLoop(messageFile, topic, executor, producer);
                }
            }
        }
    }
}

package org.szesto;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public class MessageWorker {

    private static final Logger logger = LoggerFactory.getLogger(MessageWorker.class);

    public static enum BatchSendReturnCode {
        SUCCESS,
        CONTINUE_ON_ERROR,
        CLOSE_PRODUCER,
        FATAL_ERROR;

        public static boolean successOrRecover(BatchSendReturnCode rc) {
            return rc != CLOSE_PRODUCER && rc != FATAL_ERROR;
        }
    }

    public MessageWorker() {
    }

    public static RecordMetadata sendMessageBlocking(Semaphore sem, KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        try {
            sem.acquire();

            // random value between 0 and 1000
            long dt = ThreadLocalRandom.current().nextInt(1200, 1500);
            TimeUnit.MILLISECONDS.sleep(dt);

            return producer.send(record).get();

        } catch (Exception e) {
            // check for kafka specific exceptions
            logger.error("Error while sending message", e);

            throw new RuntimeException(e);
        }
        finally {
            sem.release();
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
        checkPropertiesFile(propertiesFile);

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

    public static KafkaConsumer<String, String> createConsumer(Properties props) {

        final String consumerGroup = props.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        if (consumerGroup.isEmpty()) {
            throw new IllegalArgumentException("Consumer group.id is not defined in the consumer.properties file");
        }

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return new KafkaConsumer<>(props);
    }

    public static void checkPropertiesFile(String propertiesFile) {
        logger.info("Kafka properties file: {}", propertiesFile);

        if (InputWorker.fileMissing(propertiesFile)) {
            throw new IllegalArgumentException("File '" + propertiesFile + "' not found");
        }
    }

    public static void checkMessageFile(String messageFile) {
        if (InputWorker.fileMissing(messageFile)) {
            throw new IllegalArgumentException("File '" + messageFile + "' not found");
        }
    }

    public static BatchSendReturnCode checkFutures(Map<Integer, Future<RecordMetadata>> futures) {

        BatchSendReturnCode brc = BatchSendReturnCode.SUCCESS;

        Iterator<Integer> iter = futures.keySet().iterator();

        while (iter.hasNext()) {
            int k = iter.next();

            if (futures.get(k).isDone()) {
                try {
                    RecordMetadata meta = futures.get(k).get();
                    if (logger.isDebugEnabled()) logger.debug("Message sent to topic {} partition {} offset {}", meta.topic(), meta.partition(), meta.offset());

                } catch (ExecutionException | InterruptedException e) {
                    Throwable cause = e.getCause();

                    logger.error("Error sending message, root cause: ", e);

                    switch (cause) {
                        case UnsupportedVersionException unsupportedVersionException -> brc = BatchSendReturnCode.FATAL_ERROR;
                        case AuthorizationException authorizationException -> brc = BatchSendReturnCode.FATAL_ERROR;
                        case OutOfOrderSequenceException outOfOrderSequenceException -> {
                            if (BatchSendReturnCode.successOrRecover(brc)) {
                                brc = BatchSendReturnCode.CLOSE_PRODUCER;
                            }
                        }
                        case null, default -> {
                            if (BatchSendReturnCode.successOrRecover(brc)) {
                                brc = BatchSendReturnCode.CONTINUE_ON_ERROR;
                            }
                        }
                    }
                }

                iter.remove();
            }
        }

        return brc;
    }

    public static boolean messageConsumerLoop(String topic, KafkaConsumer<String, String> consumer, ConsumeParams params) throws IOException {

        consumer.subscribe(Collections.singletonList(topic));

        int numRecords = 0;

        boolean receiveMoreMessages = true;

        try {
            while (receiveMoreMessages) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                numRecords += records.count();

                if (params.maxRecords() > 0 && numRecords >= params.maxRecords()) {
                    receiveMoreMessages = false;
                }

                if (! records.isEmpty()) logger.info("Received {} records", records.count());

                try {
                    consumer.commitSync();
                } catch (Exception e) {
                    Throwable cause = e.getCause();
                    logger.error("Error committing offsets, root cause: ", cause);
                }
            }

            return true;

        } catch (Exception e) {
            Throwable cause = e.getCause();
            logger.error("Error receiving message, root cause: ", e);
            return false;
        }
    }

    public static BatchSendReturnCode messageProducerLoop(String messageFile, String topic, ExecutorService executor, KafkaProducer<String, String> producer, SendParams sendParams) throws IOException {

        // one file or collection of files
        final String buf = InputWorker.readFile(messageFile);

        // rate: messages per second
        final int maxMessages = sendParams.rate() > 0 ? sendParams.rate() : 1;

        // batches of m messages
        final int batches = sendParams.batches() > 0 ? sendParams.batches() : 1;

        Map<Integer, Future<RecordMetadata>> futures = new HashMap<>();

        CountDownLatch latch = new CountDownLatch(1);
        Semaphore sem = new Semaphore(1000);

        BatchSendReturnCode brc = BatchSendReturnCode.SUCCESS;

        for (int b = 0; b < batches && BatchSendReturnCode.successOrRecover(brc); b++) {

            if (b > 0) {
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    // throw new RuntimeException(e);
                }
            }

            // rate: m/s
            for (int mcount = 0; mcount < maxMessages; mcount++) {

                final ProducerRecord<String, String> record = makeRecord(topic, null, buf);

                Future<RecordMetadata> future = executor.submit(() -> sendMessageBlocking(sem, producer, record));
                futures.put(future.hashCode(), future);
            }

            logger.info("Sent {} messages in batch {}, outstanding futures {}", maxMessages, b, futures.size());

            brc = checkFutures(futures);
        }

        logger.info("Finished sending, outstanding futures {}", futures.size());

        boolean complete = false;

        while (!complete) {
            try {
                complete = latch.await(100L, TimeUnit.MILLISECONDS);

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            BatchSendReturnCode brc1 = checkFutures(futures);

            if (BatchSendReturnCode.successOrRecover(brc)) {
                brc = brc1;
            }

            if (futures.isEmpty()) {
                complete = true;
            }
        }

        return brc;
    }

    public static ConsumeParams parseConsumerInput(String ...args) {
        // arg1 - role, arg2 - maxRecords, arg3 - maxDurationMinutes

        final int maxRecords = args.length >= 2 ? Integer.parseInt(args[1]) : 0;
        final int maxDurationMinutes = args.length >= 3 ? Integer.parseInt(args[2]) : 0;

        return new ConsumeParams(maxRecords, Duration.ofMinutes(maxDurationMinutes));
    }

    public static SendParams parseProducerInput(String ...args) {
        // arg1 - role, arg2 - rate, arg3 - batches

        final int rate = args.length >= 2 ? Integer.parseInt(args[1]) : 0;
        final int batches = args.length >= 3 ? Integer.parseInt(args[2]) : 0;

        return new SendParams(rate, batches);
    }

    public static ExecutorService executorsServiceFactory(Properties properties) {
        final String executorsFactory = properties.getProperty("executors.factory", "virtual");

        logger.info("Using executors factory: {}", executorsFactory);

        if (executorsFactory.equals("virtual")) {
            return Executors.newVirtualThreadPerTaskExecutor();
        } else if (executorsFactory.equals("cached")) {
            return Executors.newCachedThreadPool();
        } else {
            final int threadPoolSize = Integer.parseInt(properties.getProperty("executors.fixedThreadPoolSize", "50"));
            logger.info("Using fixed thread pool size: {}", threadPoolSize);

            return Executors.newFixedThreadPool(threadPoolSize);
        }
    }

    public static void main(String ...args) throws IOException {

        final String messageFile = "message.json";

        final String role = args.length >= 1 ? args[0] : "producer";

        final String propertiesFile = role.toLowerCase().startsWith("prod") ? "producer.properties" : "consumer.properties";

        final Properties props = loadProperties(propertiesFile);

        final String topic = props.getProperty("topic", "");
        if (topic.isEmpty()) {
            throw new IllegalArgumentException("Topic is not defined in the properties file");
        }

        if (role.toLowerCase().startsWith("prod")) {
            SendParams sendParams = parseProducerInput(args);

            checkMessageFile(messageFile);

            try (ExecutorService executor = executorsServiceFactory(props)) {
                BatchSendReturnCode brc = BatchSendReturnCode.SUCCESS;
                do {
                    try (KafkaProducer<String, String> producer = MessageWorker.createProducer(props)) {
                        brc = MessageWorker.messageProducerLoop(messageFile, topic, executor, producer, sendParams);
                    }
                } while (brc == BatchSendReturnCode.CLOSE_PRODUCER);
            }

        } else {
            ConsumeParams consumeParams = parseConsumerInput(args);

            try (KafkaConsumer<String, String> consumer = createConsumer(props)) {
                boolean rc = MessageWorker.messageConsumerLoop(topic, consumer, consumeParams);
            }
        }
    }
}

package kz.qazmarka.h2k.kafka.compat;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;

import kz.qazmarka.h2k.kafka.ensure.admin.KafkaTopicAdmin;
import kz.qazmarka.h2k.kafka.ensure.admin.KafkaTopicAdminClient;
import kz.qazmarka.h2k.kafka.serializer.RowKeySliceSerializer;
import kz.qazmarka.h2k.util.RowKeySlice;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * E2E проверка совместимости клиента Kafka 3.3.2 с брокером Kafka 2.3.1 (Confluent 5.3.8).
 */
@Tag("e2e")
@Testcontainers(disabledWithoutDocker = true)
@EnabledIfEnvironmentVariable(named = "H2K_E2E_KAFKA", matches = "(?i)true")
class Kafka233CompatibilityE2ETest {

    private static final DockerImageName KAFKA_IMAGE =
            DockerImageName.parse("confluentinc/cp-kafka:5.3.8")
                    .asCompatibleSubstituteFor("confluentinc/cp-kafka");

    @Container
    static final KafkaContainer KAFKA = createKafkaContainer();

    @Test
    void producerAndAdminAreCompatibleWithKafka23() throws Exception {
        String topic = "h2k_e2e_" + UUID.randomUUID().toString().substring(0, 8);
        createTopic(topic);

        byte[] value = "payload-1".getBytes(StandardCharsets.UTF_8);
        RowKeySlice key = RowKeySlice.whole("row-1".getBytes(StandardCharsets.UTF_8));
        sendRecord(topic, key, value);

        List<byte[]> values = pollValues(topic, 1);
        assertEquals(1, values.size(), "Не получили сообщение из Kafka 2.3.1");
        assertArrayEquals(value, values.get(0));
    }

    @Test
    void adminCanDescribeAndIncreasePartitions() throws Exception {
        String topic = "h2k_e2e_admin_" + UUID.randomUUID().toString().substring(0, 6);
        createTopic(topic, 1, TopicConfig.CLEANUP_POLICY_COMPACT, "60000");

        try (AdminClient admin = newAdminClient()) {
            KafkaTopicAdmin topicAdmin = new KafkaTopicAdminClient(admin);
            topicAdmin.increasePartitions(topic, 3, TimeUnit.SECONDS.toMillis(30));

            TopicDescription description = admin.describeTopics(Collections.singleton(topic))
                    .topicNameValues()
                    .get(topic)
                    .get(30, TimeUnit.SECONDS);
            List<TopicPartitionInfo> partitions = description.partitions();
            assertEquals(3, partitions.size(), "Должны получить 3 партиции после увеличения");

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            Config config = admin.describeConfigs(Collections.singleton(resource))
                    .values()
                    .get(resource)
                    .get(30, TimeUnit.SECONDS);
            assertEquals(TopicConfig.CLEANUP_POLICY_COMPACT,
                    config.get(TopicConfig.CLEANUP_POLICY_CONFIG).value());
            assertEquals("60000", config.get(TopicConfig.RETENTION_MS_CONFIG).value());
        }
    }

    @Test
    void producerDeliversBatchInOrder() throws Exception {
        String topic = "h2k_e2e_batch_" + UUID.randomUUID().toString().substring(0, 6);
        createTopic(topic);

        List<String> payloads = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            payloads.add("payload-" + i);
        }

        try (KafkaProducer<RowKeySlice, byte[]> producer = newProducer()) {
            for (int i = 0; i < payloads.size(); i++) {
                RowKeySlice key = RowKeySlice.whole(("row-" + i).getBytes(StandardCharsets.UTF_8));
                byte[] value = payloads.get(i).getBytes(StandardCharsets.UTF_8);
                producer.send(new ProducerRecord<>(topic, key, value)).get(30, TimeUnit.SECONDS);
            }
        }

        List<byte[]> values = pollValues(topic, payloads.size());
        assertEquals(payloads.size(), values.size(), "Не все сообщения дочитаны");
        for (int i = 0; i < payloads.size(); i++) {
            assertArrayEquals(payloads.get(i).getBytes(StandardCharsets.UTF_8), values.get(i));
        }
    }

    @Test
    void creatingExistingTopicFailsWithTopicExists() throws Exception {
        String topic = "h2k_e2e_exists_" + UUID.randomUUID().toString().substring(0, 6);
        createTopic(topic);
        ExecutionException ex = assertThrows(ExecutionException.class, () -> createTopic(topic));
        assertEquals(TopicExistsException.class, ex.getCause().getClass(),
                "Ожидаем TopicExistsException при повторном создании топика");
    }

    @Test
    void consumerGroupRebalanceDistributesPartitions() throws Exception {
        String topic = "h2k_e2e_rebalance_" + UUID.randomUUID().toString().substring(0, 6);
        createTopic(topic, 2, TopicConfig.CLEANUP_POLICY_DELETE, null);
        String groupId = "h2k-e2e-group-" + UUID.randomUUID();

        try (KafkaConsumer<byte[], byte[]> first = newConsumer(groupId);
             KafkaConsumer<byte[], byte[]> second = newConsumer(groupId)) {

            first.subscribe(Collections.singleton(topic));
            waitForAssignment(first, 2, "Первый consumer должен получить все партиции");

            second.subscribe(Collections.singleton(topic));
            waitForBalancedAssignment(first, 1, second, 1,
                    "После ребаланса оба consumer'а должны разделить партиции");

            assertEquals(1, first.assignment().size(), "Первый consumer должен иметь одну партицию");
            assertEquals(1, second.assignment().size(), "Второй consumer должен иметь одну партицию");
            first.assignment().forEach(tp -> assertEquals(topic, tp.topic()));
            second.assignment().forEach(tp -> assertEquals(topic, tp.topic()));
        }
    }

    @Test
    void producerWorksWithRowKeySliceOffset() throws Exception {
        String topic = "h2k_e2e_slice_" + UUID.randomUUID().toString().substring(0, 6);
        createTopic(topic);
        byte[] backingKey = "xxxxROWKEY".getBytes(StandardCharsets.UTF_8);
        RowKeySlice slice = new RowKeySlice(backingKey, 4, 6); // ROWKEY
        byte[] value = "slice-payload".getBytes(StandardCharsets.UTF_8);

        sendRecord(topic, slice, value);
        List<byte[]> values = pollValues(topic, 1);
        assertEquals(1, values.size(), "Ожидаем одно сообщение");
        assertArrayEquals(value, values.get(0));
    }

    @Test
    void producerFailsWithInvalidBootstrap() {
        Properties props = baseProducerProps();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        props.put(ProducerConfig.RETRIES_CONFIG, "0");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "1500");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "h2k-e2e-invalid");

        try (KafkaProducer<RowKeySlice, byte[]> producer = new KafkaProducer<>(props)) {
            RowKeySlice key = RowKeySlice.whole("row-invalid".getBytes(StandardCharsets.UTF_8));
            byte[] value = "invalid".getBytes(StandardCharsets.UTF_8);
            try {
                producer.send(new ProducerRecord<>("missing-topic", key, value)).get(2, TimeUnit.SECONDS);
                fail("Ожидали ошибку подключения к несуществующему брокеру");
            } catch (ExecutionException | TimeoutException ex) {
                assertNotNull(ex.getCause(), "Должна быть причина ошибки");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("Ожидание результата отправки прервано");
            }
        }
    }

    @Test
    void producerRecoversAfterBrokerRestart() throws Exception {
        String topic = "h2k_e2e_restart_" + UUID.randomUUID().toString().substring(0, 6);
        createTopic(topic);

        sendRecord(topic, RowKeySlice.whole("row-before".getBytes(StandardCharsets.UTF_8)),
                "before".getBytes(StandardCharsets.UTF_8));
        assertEquals(1, pollValues(topic, 1).size(), "Сообщение до рестарта должно сохраниться");

        String bootstrapBeforeRestart = bootstrapServers();
        KAFKA.stop();

        Properties failingProps = baseProducerProps(bootstrapBeforeRestart);
        failingProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000");
        failingProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "2000");
        try (KafkaProducer<RowKeySlice, byte[]> failingProducer = new KafkaProducer<>(failingProps)) {
            RowKeySlice key = RowKeySlice.whole("row-during".getBytes(StandardCharsets.UTF_8));
            byte[] value = "during".getBytes(StandardCharsets.UTF_8);
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> failingProducer.send(new ProducerRecord<>(topic, key, value)).get(),
                    "Отправка при выключенном брокере должна падать");
            assertNotNull(failure.getCause(), "Причина ошибки после остановки брокера должна присутствовать");
        }

        KAFKA.start();
        waitForKafkaReady();
        ensureTopicExists(topic);

        sendRecord(topic, RowKeySlice.whole("row-after".getBytes(StandardCharsets.UTF_8)),
                "after".getBytes(StandardCharsets.UTF_8));

        List<byte[]> valuesAfterRestart = pollValues(topic, 1);
        assertEquals(1, valuesAfterRestart.size(), "После рестарта должны читать новые сообщения");
        assertArrayEquals("after".getBytes(StandardCharsets.UTF_8), valuesAfterRestart.get(0),
                "Сообщение после рестарта должно приниматься");
    }

    private static void createTopic(String topic) throws Exception {
        createTopic(topic, 1, TopicConfig.CLEANUP_POLICY_DELETE, null);
    }

    private static void createTopic(String topic,
                                    int partitions,
                                    String cleanupPolicy,
                                    String retentionMs) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "h2k-e2e-admin");
        try (AdminClient admin = AdminClient.create(props)) {
            KafkaTopicAdmin topicAdmin = new KafkaTopicAdminClient(admin);
            Map<String, String> config = new HashMap<>();
            config.put(TopicConfig.CLEANUP_POLICY_CONFIG, cleanupPolicy);
            if (retentionMs != null) {
                config.put(TopicConfig.RETENTION_MS_CONFIG, retentionMs);
            }
            NewTopic newTopic = new NewTopic(topic, partitions, (short) 1)
                    .configs(config);
            topicAdmin.createTopic(newTopic, TimeUnit.SECONDS.toMillis(30));
        }
    }

    private static void ensureTopicExists(String topic) throws Exception {
        try {
            createTopic(topic, 1, TopicConfig.CLEANUP_POLICY_DELETE, null);
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw e;
            }
        }
    }

    private static AdminClient newAdminClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "h2k-e2e-admin-" + UUID.randomUUID());
        return AdminClient.create(props);
    }

    private static KafkaProducer<RowKeySlice, byte[]> newProducer() {
        return new KafkaProducer<>(baseProducerProps());
    }

    private static Properties baseProducerProps() {
        return baseProducerProps(bootstrapServers());
    }

    private static Properties baseProducerProps(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, RowKeySliceSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "180000");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "50");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "65536");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "h2k-e2e-producer");
        return props;
    }

    private static void sendRecord(String topic, RowKeySlice key, byte[] value) throws Exception {
        try (KafkaProducer<RowKeySlice, byte[]> producer = newProducer()) {
            producer.send(new ProducerRecord<>(topic, key, value)).get(30, TimeUnit.SECONDS);
        }
    }

    private static List<byte[]> pollValues(String topic, int expected) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "h2k-e2e-consumer" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1500");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "9000");

        List<byte[]> values = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singleton(topic));
            long deadline = System.currentTimeMillis() + 20_000;
            while (System.currentTimeMillis() < deadline && values.size() < expected) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], byte[]> consumerRecord : records) {
                    values.add(consumerRecord.value());
                }
            }
        }
        return values;
    }

    private static KafkaConsumer<byte[], byte[]> newConsumer(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1500");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "9000");
        return new KafkaConsumer<>(props);
    }

    private static void waitForAssignment(KafkaConsumer<byte[], byte[]> consumer,
                                          int expectedPartitions,
                                          String message) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            consumer.poll(Duration.ofMillis(250));
            if (consumer.assignment().size() == expectedPartitions) {
                return;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(200));
            if (Thread.interrupted()) {
                Thread.currentThread().interrupt();
                fail("Ожидание назначения партиций прервано");
            }
        }
        fail(message + ": assignment=" + consumer.assignment());
    }

    private static void waitForBalancedAssignment(KafkaConsumer<byte[], byte[]> first,
                                                  int expectedFirst,
                                                  KafkaConsumer<byte[], byte[]> second,
                                                  int expectedSecond,
                                                  String message) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            first.poll(Duration.ofMillis(250));
            second.poll(Duration.ofMillis(250));
            if (first.assignment().size() == expectedFirst && second.assignment().size() == expectedSecond) {
                return;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(200));
            if (Thread.interrupted()) {
                Thread.currentThread().interrupt();
                fail("Ожидание распределения партиций прервано");
            }
        }
        fail(message + ": first=" + first.assignment() + ", second=" + second.assignment());
    }

    private static void waitForKafkaReady() {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            try (AdminClient admin = newAdminClient()) {
                admin.listTopics().names().get(5, TimeUnit.SECONDS);
                return;
            } catch (Exception ignored) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(500));
                if (Thread.interrupted()) {
                    Thread.currentThread().interrupt();
                    fail("Ожидание брокера прервано");
                }
            }
        }
        fail("Брокер Kafka не поднялся после рестарта");
    }

    private static String bootstrapServers() {
        if (!KAFKA.isRunning()) {
            fail("Контейнер Kafka остановлен, bootstrap servers недоступен");
        }
        return KAFKA.getBootstrapServers();
    }

    private static KafkaContainer createKafkaContainer() {
        KafkaContainer container = new KafkaContainer(KAFKA_IMAGE);
        container.withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
        container.withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1");
        container.withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");
        container.withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
        return container;
    }
}

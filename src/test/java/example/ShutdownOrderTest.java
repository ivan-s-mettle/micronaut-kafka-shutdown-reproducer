package example;

import io.micronaut.context.ApplicationContext;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class ShutdownOrderTest {

    private static final int RECORD_COUNT = 2000;

    private static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.9.6");
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    @BeforeAll
    static void startContainers() throws Exception {
        KAFKA.start();
        POSTGRES.start();

        try (Admin admin = admin()) {
            admin.createTopics(List.of(new NewTopic(RecordListener.TOPIC, 1, (short) 1))).all().get();
        }
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))) {
            for (int i = 0; i < RECORD_COUNT; i++) {
                producer.send(new ProducerRecord<>(RecordListener.TOPIC, "key-" + i, "value-" + i));
            }
        }
        try (Connection connection = postgres(); Statement statement = connection.createStatement()) {
            statement.execute("create table processed_record (record_offset bigint primary key)");
        }
    }

    @AfterAll
    static void stopContainers() {
        POSTGRES.stop();
        KAFKA.stop();
    }

    @Test
    void recordsCommittedByTheConsumerAreProcessed() throws Exception {
        ApplicationContext context = ApplicationContext.run(Map.of(
                "kafka.bootstrap.servers", KAFKA.getBootstrapServers(),
                "datasources.default.url", POSTGRES.getJdbcUrl(),
                "datasources.default.username", POSTGRES.getUsername(),
                "datasources.default.password", POSTGRES.getPassword(),
                "datasources.default.driver-class-name", "org.postgresql.Driver"));

        // Stop the context while the listener is part-way through the topic
        await().atMost(Duration.ofSeconds(60)).until(() -> countProcessed(Long.MAX_VALUE) >= 100);
        System.out.println(">>> Stopping the application context");
        context.stop();
        System.out.println(">>> Application context stopped");

        long committedOffset = committedOffset();
        long processedBelowCommitted = countProcessed(committedOffset);
        long lost = committedOffset - processedBelowCommitted;
        System.out.printf(">>> Micronaut %s: committed offset=%d, processed below it=%d, lost=%d%n",
                ApplicationContext.class.getPackage().getImplementationVersion(), committedOffset, processedBelowCommitted, lost);

        assertThat(lost)
                .as("records whose offset was committed but that were never processed")
                .isZero();
    }

    private static long committedOffset() throws Exception {
        try (Admin admin = admin()) {
            Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets(RecordListener.GROUP_ID)
                    .partitionsToOffsetAndMetadata().get();
            OffsetAndMetadata offset = offsets.get(new TopicPartition(RecordListener.TOPIC, 0));
            return offset == null ? 0 : offset.offset();
        }
    }

    private static long countProcessed(long belowOffset) throws SQLException {
        try (Connection connection = postgres();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(
                     "select count(*) from processed_record where record_offset < " + belowOffset)) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    private static Admin admin() {
        return Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()));
    }

    private static Connection postgres() throws SQLException {
        return DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }
}

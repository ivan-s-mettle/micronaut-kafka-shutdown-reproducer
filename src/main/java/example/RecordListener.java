package example;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Default settings: OffsetStrategy.AUTO and the default error strategy.
 */
@KafkaListener(groupId = RecordListener.GROUP_ID, offsetReset = OffsetReset.EARLIEST)
public class RecordListener {

    public static final String GROUP_ID = "shutdown-reproducer";
    public static final String TOPIC = "records";

    private final ProcessedRecordRepository repository;

    public RecordListener(ProcessedRecordRepository repository) {
        this.repository = repository;
    }

    @Topic(TOPIC)
    public void receive(ConsumerRecord<String, String> record) throws InterruptedException {
        // Simulate some work, so a batch is still being processed when the context stops
        Thread.sleep(5);
        repository.save(record.offset());
    }
}

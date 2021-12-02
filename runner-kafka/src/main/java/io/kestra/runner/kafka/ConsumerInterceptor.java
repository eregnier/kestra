package io.kestra.runner.kafka;

import io.kestra.runner.kafka.configs.LoggerConfig;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

@NoArgsConstructor
public class ConsumerInterceptor<K,V> extends AbstractInterceptor implements org.apache.kafka.clients.consumer.ConsumerInterceptor<K, V> {
    @Override
    public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
        records.forEach(record -> this.log(
            LoggerConfig.Type.CONSUMER,
            record.topic(),
            record.partition(),
            record.offset(),
            record.timestamp(),
            record.key(),
            record.value()
        ));

        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {

    }
}

package io.kestra.runner.kafka;

import io.kestra.runner.kafka.configs.LoggerConfig;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

@NoArgsConstructor
public class ProducerInterceptor<K, V> extends AbstractInterceptor implements org.apache.kafka.clients.producer.ProducerInterceptor<K, V> {
    @Override
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        this.log(
            LoggerConfig.Type.PRODUCER,
            record.topic(),
            record.partition(),
            null,
            record.timestamp(),
            record.key(),
            record.value()
        );

        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

}

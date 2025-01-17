package io.kestra.indexer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.opensearch.client.RestHighLevelClient;
import org.junit.jupiter.api.Test;
import io.kestra.core.metrics.MetricRegistry;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.ExecutorsUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.repository.elasticsearch.ElasticSearchIndicesService;
import io.kestra.repository.elasticsearch.configs.IndicesConfig;
import io.kestra.runner.kafka.configs.TopicsConfig;
import io.kestra.runner.kafka.services.KafkaConsumerService;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import javax.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


@MicronautTest
class KafkaElasticIndexerTest {
    @Inject
    MetricRegistry metricRegistry;

    @Inject
    RestHighLevelClient elasticClient;

    @Inject
    IndexerConfig indexerConfig;

    @Inject
    List<TopicsConfig> topicsConfig;

    @Inject
    List<IndicesConfig> indicesConfigs;

    @Inject
    ElasticSearchIndicesService elasticSearchIndicesService;

    @Inject
    KafkaConsumerService kafkaConsumerService;

    @Inject
    ExecutorsUtils executorsUtils;

    @Test
    void run() throws IOException, InterruptedException {
        String topic = this.topicsConfig
            .stream()
            .filter(indicesConfig -> indicesConfig.getCls() == Execution.class)
            .findFirst()
            .orElseThrow()
            .getName();

        CountDownLatch countDownLatch = new CountDownLatch(1);

        RestHighLevelClient elasticClientSpy = spy(elasticClient);
        doAnswer(invocation -> {
            countDownLatch.countDown();
            return invocation.callRealMethod();
        }).when(elasticClientSpy).bulk(any(), any());

        KafkaConsumerService kafkaConsumerServiceSpy = mock(KafkaConsumerService.class);
        MockConsumer<String, String> mockConsumer = mockConsumer(topic);
        doReturn(mockConsumer).when(kafkaConsumerServiceSpy).of(any(), any(), any());

        ConsumerRecord<String, String> first = buildExecutionRecord(topic, 0);

        mockConsumer.addRecord(first);
        mockConsumer.addRecord(buildExecutionRecord(topic, 1));
        mockConsumer.addRecord(buildExecutionRecord(topic, 2));
        mockConsumer.addRecord(buildExecutionRecord(topic, 3));
        mockConsumer.addRecord(buildExecutionRecord(topic, 4));
        mockConsumer.addRecord(buildRecord(topic, first.key(), null, 5));

        KafkaElasticIndexer indexer = new KafkaElasticIndexer(
            metricRegistry,
            elasticClientSpy,
            indexerConfig,
            topicsConfig,
            indicesConfigs,
            elasticSearchIndicesService,
            kafkaConsumerServiceSpy,
            executorsUtils
        );

        Thread thread = new Thread(indexer);
        thread.start();

        countDownLatch.await();
        assertThat(countDownLatch.getCount(), is(0L));
    }

    private ConsumerRecord<String, String> buildExecutionRecord(String topic, int offset) throws JsonProcessingException {
        Flow flow = TestsUtils.mockFlow();
        Execution execution = TestsUtils.mockExecution(flow, ImmutableMap.of());

        return buildRecord(
            topic,
            execution.getId(),
            JacksonMapper.ofJson().writeValueAsString(execution),
            offset
        );
    }

    private ConsumerRecord<String, String> buildRecord(String topic, String key, String value, int offset) {
        return new ConsumerRecord<>(
            topic,
            0,
            offset,
            key,
            value
        );
    }

    private MockConsumer<String, String> mockConsumer(String index) {
        MockConsumer<String, String> consumer = spy(new MockConsumer<>(OffsetResetStrategy.EARLIEST));
        doNothing().when(consumer).subscribe(Collections.singleton(any()));

        consumer.assign(Collections.singletonList(new TopicPartition(index, 0)));
        HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(new TopicPartition(index, 0), 0L);
        consumer.updateBeginningOffsets(beginningOffsets);

        return consumer;
    }
}
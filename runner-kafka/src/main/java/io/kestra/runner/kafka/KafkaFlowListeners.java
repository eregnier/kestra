package io.kestra.runner.kafka;

import com.google.common.collect.Streams;
import io.kestra.runner.kafka.services.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.services.FlowListenersInterface;
import io.kestra.core.services.FlowService;
import io.kestra.runner.kafka.serializers.JsonSerde;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
@Slf4j
@KafkaQueueEnabled
public class KafkaFlowListeners implements FlowListenersInterface {
    private final KafkaAdminService kafkaAdminService;
    private final FlowService flowService;

    private SafeKeyValueStore<String, Flow> store;
    private final List<Consumer<List<Flow>>> consumers = new ArrayList<>();
    private final KafkaStreamService.Stream stream;

    @Inject
    public KafkaFlowListeners(
        KafkaAdminService kafkaAdminService,
        KafkaStreamService kafkaStreamService,
        FlowService flowService
    ) {
        this.kafkaAdminService = kafkaAdminService;
        this.flowService = flowService;

        kafkaAdminService.createIfNotExist(Flow.class);
        kafkaAdminService.createIfNotExist(KafkaStreamSourceService.TOPIC_FLOWLAST);

        KafkaStreamService.Stream buillLastVersion = kafkaStreamService.of(FlowListenerBuild.class, FlowListenerBuild.class, new FlowListenerBuild().topology());
        buillLastVersion.start();

        stream = kafkaStreamService.of(FlowListener.class, FlowListener.class, new FlowListener().topology());
        stream.start((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING) {
                try {
                    ReadOnlyKeyValueStore<String, Flow> store = stream.store(StoreQueryParameters.fromNameAndType(
                        "flow",
                        QueryableStoreTypes.keyValueStore()
                    ));

                    this.store = new SafeKeyValueStore<>(store, "flow");
                    this.send(this.flows());
                } catch (InvalidStateStoreException e) {
                    this.store = null;
                    log.warn(e.getMessage(), e);
                }
            } else {
                this.send(new ArrayList<>());
            }
        });
    }

    public class FlowListenerBuild {
        public Topology topology() {
            StreamsBuilder builder = new KafkaStreamsBuilder();

            KStream<String, Flow> stream = builder
                .stream(
                    kafkaAdminService.getTopicName(Flow.class),
                    Consumed.with(Serdes.String(), JsonSerde.of(Flow.class, false))
                );

            KStream<String, Flow> result = KafkaStreamSourceService.logIfEnabled(
                log,
                stream,
                (key, value) -> log.trace(
                    "Flow in '{}.{}' with revision {}",
                    value.getNamespace(),
                    value.getId(),
                    value.getRevision()
                ),
                "flow-in"
            )
                .filter((key, value) -> value != null, Named.as("notNull"))
                .selectKey((key, value) -> value.uidWithoutRevision(), Named.as("rekey"))
                .groupBy(
                    (String key, Flow value) -> value.uidWithoutRevision(),
                    Grouped.<String, Flow>as("grouped")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(JsonSerde.of(Flow.class, false))
                )
                .aggregate(
                    AllFlowRevision::new,
                    (key, value, aggregate) -> {
                        aggregate.revisions.add(value);

                        return aggregate;
                    },
                    Materialized.<String, AllFlowRevision, KeyValueStore<Bytes, byte[]>>as("list")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(JsonSerde.of(AllFlowRevision.class, false))
                )
                .mapValues(
                    (readOnlyKey, value) -> {
                        List<Flow> flows = new ArrayList<>(flowService
                            .keepLastVersion(value.revisions));

                        if (flows.size() > 1) {
                            throw new IllegalArgumentException("Too many flows (" + flows.size() + ")");
                        }

                        return flows.size() == 0 ? null : flows.get(0);
                    },
                    Named.as("last")
                )
                .toStream();

            KafkaStreamSourceService.logIfEnabled(
                log,
                result,
                (key, value) -> log.trace(
                    "Flow out '{}.{}' with revision {}",
                    value.getNamespace(),
                    value.getId(),
                    value.getRevision()
                ),
                "Flow-out"
            )
                .to(
                    kafkaAdminService.getTopicName(KafkaStreamSourceService.TOPIC_FLOWLAST),
                    Produced.with(Serdes.String(), JsonSerde.of(Flow.class))
                );

            Topology topology = builder.build();

            if (log.isTraceEnabled()) {
                log.trace(topology.describe().toString());
            }

            return topology;
        }

    }

    @NoArgsConstructor
    @Getter
    public static class AllFlowRevision {
        private final List<Flow> revisions = new ArrayList<>();
    }

    public class FlowListener {
        public Topology topology() {
            StreamsBuilder builder = new KafkaStreamsBuilder();

            builder
                .table(
                    kafkaAdminService.getTopicName(KafkaStreamSourceService.TOPIC_FLOWLAST),
                    Consumed.with(Serdes.String(), JsonSerde.of(Flow.class, false)),
                    Materialized.<String, Flow, KeyValueStore<Bytes, byte[]>>as("flow")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(JsonSerde.of(Flow.class, false))
                )
                .filter((key, value) -> value != null)
                .toStream()
                .peek((key, value) -> {
                    send(flows());
                });

            Topology topology = builder.build();

            if (log.isTraceEnabled()) {
                log.trace(topology.describe().toString());
            }

            return topology;
        }
    }

    @Override
    public List<Flow> flows() {
        if (this.store == null || stream.state() != KafkaStreams.State.RUNNING) {
            return Collections.emptyList();
        }

        return this.store
            .toStream()
            .filter(flow -> flow != null && !flow.isDeleted())
            .collect(Collectors.toList());
    }

    private void send(List<Flow> flows) {
        this.consumers
            .forEach(consumer -> consumer.accept(flows));
    }

    @Override
    public void listen(Consumer<List<Flow>> consumer) {
        consumers.add(consumer);
        consumer.accept(this.flows());
    }
}

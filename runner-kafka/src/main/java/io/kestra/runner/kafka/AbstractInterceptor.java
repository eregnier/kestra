package io.kestra.runner.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.ListUtils;
import io.kestra.runner.kafka.configs.ClientConfig;
import io.kestra.runner.kafka.configs.LoggerConfig;
import io.kestra.runner.kafka.services.KafkaStreamService;
import io.micronaut.context.ApplicationContext;
import io.micronaut.core.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.event.Level;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

@Slf4j
public abstract class AbstractInterceptor {
    private final static StringDeserializer STRING_DESERIALIZER = new StringDeserializer();

    ClientConfig clientConfig;

    protected <K, V> void log(
        LoggerConfig.Type type,
        String topic,
        Integer partition,
        @Nullable Long offset,
        Long timestamp,
        K key,
        V value
    ) {
        Level level = isMatch(type, topic, key, value);

        if (level == null) {
            return;
        }

        String format = "[{} > {}{}{}{}] {} = {}";
        Object[] args = {
            type,
            topic,
            partition != null ? "[" + partition + "]" : "",
            offset != null ? "@" + offset : "",
            timestamp != null ? " " + Instant.ofEpochMilli(timestamp) : "",
            deserialize(key),
            deserialize(value)
        };

        if (level == Level.TRACE) {
            log.trace(format, args);
        } else if (level == Level.DEBUG) {
            log.debug(format, args);
        } else if (level == Level.INFO) {
            log.info(format, args);
        } else if (level == Level.WARN) {
            log.warn(format, args);
        } else if (level == Level.ERROR) {
            log.error(format, args);
        }
    }

    public <K, V> Level isMatch(LoggerConfig.Type type, String topic, K key, V value) {
        return ListUtils.emptyOnNull(this.clientConfig.getLoggers())
            .stream()
            .map(loggerConfig -> {
                if (logEnabled(loggerConfig.getLevel()) &&
                    (
                        loggerConfig.getType() == null ||
                            loggerConfig.getType() == type
                    ) && (
                        loggerConfig.getTopicRegexp() == null ||
                            topic.matches(loggerConfig.getTopicRegexp())
                    ) &&
                    (
                        loggerConfig.getKeyRegexp() == null ||
                            deserialize(key).matches(loggerConfig.getKeyRegexp())
                    ) &&
                    (
                        loggerConfig.getValueRegexp() == null ||
                            deserialize(value).matches(loggerConfig.getValueRegexp())
                    )
                ) {
                    return loggerConfig.getLevel();
                }

                return null;
            })
            .filter(Objects::nonNull)
            .findFirst()
            .orElse(null);
    }

    private String deserialize(Object value) {
        if (value instanceof byte[]) {
            return STRING_DESERIALIZER.deserialize("", (byte[])value);
        } else if (value instanceof String) {
            return (String) value;
        } else {
            try {
                return JacksonMapper.ofJson(false).writeValueAsString(value);
            } catch (JsonProcessingException e) {
                return "";
            }
        }
    }

    public boolean logEnabled(Level level) {
        return (level == Level.TRACE && log.isTraceEnabled()) ||
            (level == Level.DEBUG && log.isDebugEnabled()) ||
            (level == Level.INFO && log.isInfoEnabled()) ||
            (level == Level.WARN && log.isWarnEnabled()) ||
            (level == Level.ERROR && log.isErrorEnabled());
    }

    public void configure(Map<String, ?> configs) {
        ApplicationContext applicationContext = (ApplicationContext) configs.get(KafkaStreamService.APPLICATION_CONTEXT_CONFIG);
        clientConfig = applicationContext.getBean(ClientConfig.class);
    }

    public void close() {

    }

}

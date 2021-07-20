package com.markcox.kafka.adminclient;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class KafkaConsumerMonitor{

    public static void main(String[] args) {
        KafkaConsumerMonitor monitor = new KafkaConsumerMonitor();

        Map<TopicPartition, PartitionOffsets> consumerLag = monitor.getConsumerGroupOffsets(
                "pkc-xxxxx.us-west-1.aws.confluent.cloud:9092",
                "<Topic Name>",
                "<Consumer Group Name>");

        SortedMap<TopicPartition, PartitionOffsets> sortedConsumerLag =
                new TreeMap<>(Comparator.comparing(TopicPartition::toString));
        sortedConsumerLag.putAll(consumerLag);

        sortedConsumerLag.forEach((key, value) -> System.out.println("Topic=" + value.topic + " ::" +
                " Partition=" + value.partition +
                " End Offset=" + value.endOffset +
                " Current Offset=" + value.currentOffset +
                " Lag=" + (value.endOffset - value.currentOffset)));
    }

    public static class PartitionOffsets {
        private final long endOffset;
        private final long currentOffset;
        private final int partition;
        private final String topic;

        public PartitionOffsets(long endOffset, long currentOffset, int partition, String topic) {
            this.endOffset = endOffset;
            this.currentOffset = currentOffset;
            this.partition = partition;
            this.topic = topic;
        }
    }

    private final String monitoringConsumerGroupID = "monitoring_consumer_" + UUID.randomUUID();

    public Map<TopicPartition, PartitionOffsets> getConsumerGroupOffsets(String host, String topic, String groupId) {
        Map<TopicPartition, Long> logEndOffset = getLogEndOffset(topic, host);

        KafkaConsumer<?, ?> consumer = createNewConsumer(groupId, host);

        Set<TopicPartition> targetSet = new HashSet<>(logEndOffset.keySet());
        Map<TopicPartition, OffsetAndMetadata> targetCommitted = consumer.committed(targetSet);
        targetCommitted = targetCommitted.entrySet().stream().peek(entry -> {
            if (entry.getValue() == null)
            entry.setValue(new OffsetAndMetadata(0));
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        final Map<TopicPartition, OffsetAndMetadata> finalTargetCommitted = targetCommitted;
        return logEndOffset.entrySet()
                        .stream()
                        .collect(Collectors.toMap(
                           Map.Entry::getKey,
                           entry -> new PartitionOffsets(entry.getValue(),
                                   finalTargetCommitted.get(entry.getKey()).offset(),
                                   entry.getKey().partition(), topic)
                        ));
    }

    public Map<TopicPartition, Long> getLogEndOffset(String topic, String host) {
        Map<TopicPartition, Long> endOffsets = new ConcurrentHashMap<>();

        KafkaConsumer<?, ?> consumer = createNewConsumer(monitoringConsumerGroupID, host);
        List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topic);
        List<TopicPartition> topicPartitions = partitionInfoList.stream().map(pi -> new TopicPartition(topic, pi.partition())).collect(Collectors.toList());
        consumer.assign(topicPartitions);
        consumer.seekToEnd(topicPartitions);
        topicPartitions.forEach(topicPartition -> endOffsets.put(topicPartition, consumer.position(topicPartition)));
        consumer.close();

        return endOffsets;
    }

    private static KafkaConsumer<String, String> createNewConsumer(String groupId, String host) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, host);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        properties.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"<API-Key>\" password=\"<API-Secret>\";");

        return new KafkaConsumer<>(properties);
    }
}

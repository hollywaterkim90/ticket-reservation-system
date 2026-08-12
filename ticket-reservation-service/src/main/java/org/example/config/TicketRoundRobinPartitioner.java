package org.example.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class TicketRoundRobinPartitioner implements Partitioner {

    // 멀티스레드 환경에서 안전하게 숫자를 올리기 위한 원자적 정수 객체
    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        if (key instanceof String && ((String) key).startsWith("ticket:stock:")) {
            // 호출될 때마다 0, 1, 2, 3... 순차적으로 증가시키고 파티션 개수로 나머지 연산
            int index = counter.getAndIncrement();
            return Math.abs(index) % numPartitions;
        }

        if (key == null) {
            return 0;
        }
        return Math.abs(key.hashCode()) % numPartitions;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
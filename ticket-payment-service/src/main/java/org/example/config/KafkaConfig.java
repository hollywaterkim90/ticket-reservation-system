package org.example.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

@Configuration
@EnableKafka
public class KafkaConfig {

    private final KafkaProperties kafkaProperties;

    public KafkaConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    public ConsumerFactory<Object, Object> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(kafkaProperties.buildConsumerProperties(null));
    }

    @Bean(name = "manualAckKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<Object, Object> manualAckKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());
        factory.setBatchListener(true); // 배치 리스너
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL); // 수동 Ack

        String concurrencyStr = kafkaProperties.getConsumer().getProperties().get("custom.concurrency");
        int concurrencyCount = (concurrencyStr != null) ? Integer.parseInt(concurrencyStr) : 3;
        factory.setConcurrency(concurrencyCount);

        return factory;
    }
}
package org.example.config;

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

    // 💡 Spring Boot가 YML을 읽어서 만들어주는 KafkaProperties를 주입받습니다.
    private final KafkaProperties kafkaProperties;

    public KafkaConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    public ConsumerFactory<Object, Object> consumerFactory() {
        // 💡 YML 설정값(auto-offset-reset: earliest 포함)을 통째로 맵으로 가져옵니다.
        return new DefaultKafkaConsumerFactory<>(kafkaProperties.buildConsumerProperties(null));
    }

    @Bean(name = "manualAckKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<Object, Object> manualAckKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());
        factory.setBatchListener(true); // 배치 리스너
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL); // 수동 Ack

        return factory;
    }
}
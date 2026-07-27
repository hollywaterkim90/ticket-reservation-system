package org.example.config;

import org.example.dto.TicketReservationDto;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;

@EnableKafka
@Configuration
public class KafkaConfig {

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, TicketReservationDto> kafkaListenerContainerFactory(
            ConsumerFactory<String, TicketReservationDto> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, TicketReservationDto> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);

        // ⚡ 2. 배치 리스너(Batch Listener) 강제 활성화!
        // 이 옵션이 켜져야 List<TicketReservationDto> 수신 시 max.poll.records 만큼 묶어서 들고옵니다.
        factory.setBatchListener(true);

        return factory;
    }
}
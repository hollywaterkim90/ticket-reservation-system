package org.example.listener;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.dto.TicketReservationDto;
import org.example.elasticsearch.document.TicketReservationDocument;
import org.example.elasticsearch.repository.TicketReservationElasticRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class TicketConsumerListener {

    private final KafkaTemplate<String, TicketReservationDto> kafkaTemplate;
    private final TicketReservationElasticRepository repository;

    @KafkaListener(topics = "ticket-reservations", groupId = "ticket-group-payment-worker")
    public void consumeReservation(List<ConsumerRecord<String, TicketReservationDto>> records) {
        for (ConsumerRecord<String, TicketReservationDto> record : records) {
            try {
                TicketReservationDto event = record.value();

                // 💳 외부 결제 API 연동이 발생하는 무거운 비즈니스 로직 구간
                log.info("💳 [결제] PG사 연동 처리 중... 유저: {}", event.getUserId());
                Thread.sleep(5000);

                // 임의의 order id
                event.setOrderId(String.format("%s:%s", event.getTicketId(), event.getUserId()));
                // 결제 성공 시 2차 최종 확정 토픽으로 발행 (acks=all 작동)
                kafkaTemplate.send("ticket-payments", event);
            } catch (Exception e) {
                log.error("❌ 데이터 처리 중 에러 발생: {}", e.getMessage(), e);
            }
        }
    }

    @KafkaListener(topics = "ticket-payments", groupId = "ticket-group-es-indexer")
    public void consumeBatch(List<ConsumerRecord<String, TicketReservationDto>> records) {
        log.info("📦 consumeBatch [Kafka Batch Received] 수신된 메시지 수: {}건", records.size());

        List<TicketReservationDocument> documents = records.stream()
                .map(record -> {
                    TicketReservationDto event = record.value();
                    Instant messageTimestamp = Instant.ofEpochMilli(record.timestamp());

                    return TicketReservationDocument.builder()
                            .orderId(event.getOrderId())
                            .status(event.getStatus())
                            .timestamp(messageTimestamp) // Kafka 타임스탬프 (@timestamp)
                            .build();
                })
                .collect(Collectors.toList());

        // 2. Elasticsearch Bulk 저장 (Document ID = orderId로 Upsert 멱등성 보장)
        repository.saveAll(documents);

        log.info("✅ [Kafka Batch Processed] {}건 ES 색인 완료", documents.size());
    }
}
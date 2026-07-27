package org.example.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.dto.TicketReservationDto;
import org.example.elasticsearch.document.TicketReservationDocument;
import org.example.elasticsearch.repository.TicketReservationElasticRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class TicketConsumerListener {

    private final KafkaTemplate<String, TicketReservationDto> kafkaTemplate;
    private final TicketReservationElasticRepository repository;

    @KafkaListener(topics = "ticket-reservations", groupId = "ticket-group-payment-worker")
    public void consumeReservation(List<TicketReservationDto> records) throws InterruptedException {
        // 📊 배치 크기(max.poll.records 수신 결과) 확인용
        log.info("📦 [consumeReservation Batch Received] 수신된 메시지 수: {}건", records.size());

        for (int i = 0; i < records.size(); i++) {
            TicketReservationDto event = records.get(i);

            log.info("▶️ [{}/{}] 결제 처리 시작 - 유저: {}", i + 1, records.size(), event.getUserId());

//             💳 5초 대기 (max.poll.interval.ms: 15000 초과 여부 확인용)
            Thread.sleep(5000);
            log.info("💳 [결제 완료] 유저: {}", event.getUserId());

            // 임의의 order id
            event.setOrderId(String.format("%s:%s", event.getTicketId(), event.getUserId()));
            // 결제 성공 시 2차 최종 확정 토픽으로 발행 (acks=all 작동)
            kafkaTemplate.send("ticket-payments", event);
        }
    }

    @KafkaListener(topics = "ticket-payments", groupId = "ticket-group-es-indexer")
    public void consumeBatch(List<TicketReservationDto> records) {
        List<TicketReservationDocument> documents = records.stream()
                .filter(Objects::nonNull)
                .map(event -> TicketReservationDocument.builder()
                        .orderId(event.getOrderId())
                        .status(event.getStatus())
                        .timestamp(Instant.now())
                        .build())
                .collect(Collectors.toList());

        if (!documents.isEmpty()) {
            repository.saveAll(documents);
        }
    }
}
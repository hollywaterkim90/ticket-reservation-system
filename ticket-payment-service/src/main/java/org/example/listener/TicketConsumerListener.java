package org.example.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.dto.PaymentStatus;
import org.example.dto.TicketReservationDto;
import org.example.elasticsearch.document.TicketReservationDocument;
import org.example.elasticsearch.repository.TicketReservationElasticRepository;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class TicketConsumerListener {

    private final StringRedisTemplate redisTemplate;
    private final KafkaTemplate<String, TicketReservationDto> kafkaTemplate;
    private final TicketReservationElasticRepository repository;
    // PG 결제 전용 비동기 스레드 풀 (CPU 코어 수 고려하여 설정)
    private final ExecutorService paymentExecutor = Executors.newFixedThreadPool(20);

    @KafkaListener(topics = "ticket-reservations", groupId = "ticket-group-payment-worker"
            , containerFactory = "manualAckKafkaListenerContainerFactory")
    public void consumeReservation(List<TicketReservationDto> records, Acknowledgment ack) throws InterruptedException {
        // 📊 배치 크기(max.poll.records 수신 결과) 확인용
        log.info("📦 [consumeReservation Batch Received] 수신된 메시지 수: {}건", records.size());

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (TicketReservationDto event : records) {
            // PG 결제 로직을 비동기 스레드로 넘김
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                processPayment(event);
            }, paymentExecutor);

            futures.add(future);
        }

        // 💡 [핵심] 배치 내부의 모든 비동기 결제 작업이 끝날 때까지 대기 후 오프셋 커밋!
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenRun(() -> {
                    ack.acknowledge(); // 안전하게 오프셋 수동 커밋 (데이터 유실 방지)
                    log.info("✅ 배치 {}건의 비동기 처리 및 오프셋 커밋 완료", records.size());
                })
                .exceptionally(ex -> {
                    log.error("❌ 비동기 결제 처리 중 오류 발생", ex);
                    return null;
                });
    }

    private void processPayment(TicketReservationDto event) {
        String statusKey = "order:status:" + event.getOrderId();
        String stockKey = "ticket:stock:" + event.getTicketId();

        try {
            log.info("💳 [결제 시작] 유저: {}, OrderID: {}", event.getUserId(), event.getOrderId());

            // PG사 연동 재현 (5초 대기)
            Thread.sleep(5000);

            // 1. 결제 성공 처리
            event.setStatus(PaymentStatus.SUCCESS.name());
            redisTemplate.opsForValue().set(statusKey, PaymentStatus.SUCCESS.name());

            // 2차 최종 확인 토픽으로 발행
            kafkaTemplate.send("ticket-payments", event.getUserId(), event);
            log.info("🎉 [결제 완료] 유저: {}", event.getUserId());

        } catch (Exception e) {
            // 2. 결제 실패 처리 (보상 트랜잭션)
            log.error("❌ [결제 실패] OrderID: {}", event.getOrderId(), e);

            event.setStatus(PaymentStatus.FAILURE.name());
            redisTemplate.opsForValue().set(statusKey, PaymentStatus.FAILURE.name());
            redisTemplate.opsForValue().increment(stockKey); // 선점했던 좌석/재고 원복

            kafkaTemplate.send("ticket-payments", event.getUserId(), event);
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
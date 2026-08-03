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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class TicketConsumerListener {

    private final StringRedisTemplate redisTemplate;
    private final KafkaTemplate<String, TicketReservationDto> kafkaTemplate;
    private final TicketReservationElasticRepository repository;
    // PG 결제 전용 비동기 스레드 풀 (CPU 코어 수 고려하여 설정)
    private final ExecutorService paymentExecutor = Executors.newFixedThreadPool(50);

    @KafkaListener(topics = "ticket-reservations", groupId = "ticket-group-payment-worker",
            containerFactory = "manualAckKafkaListenerContainerFactory")
    public void consumeReservation(List<TicketReservationDto> records, Acknowledgment ack) {
        log.info("📦 [Batch Received] 비동기 수신 메시지 수: {}건", records.size());

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (TicketReservationDto event : records) {
            // 🚀 1. 비동기 스레드 풀로 결제 작업 이관
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                        processPaymentWithTimeout(event);
                    }, paymentExecutor)
                    // 💡 2. 타임아웃 설정 (예: 3초 넘어가면 TimeoutException 발생시켜 비동기 멈춤)
                    .orTimeout(5, TimeUnit.SECONDS)
                    // 💡 3. 비동기 작업 중 에러/타임아웃 발생 시 DLQ 이관 콜백
                    .exceptionally(ex -> {
                        sendToDlq(event, ex.getMessage());
                        return null;
                    });

            futures.add(future);
        }

        // 🚀 4. 배치 내의 모든 비동기 작업(성공 or DLQ 이관)이 수습되면 수동 Ack!
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenRun(() -> {
                    ack.acknowledge(); // 비동기 작업 완료 후 안전하게 커밋!
                    log.info("✅ 배치 {}건 비동기 처리 & DLQ 이관 수습 후 오프셋 커밋 완료", records.size());
                });
    }

    private void processPaymentWithTimeout(TicketReservationDto event) {
        log.info("💳 [결제 시작] 유저: {}, OrderID: {}", event.getUserId(), event.getOrderId());
        String statusKey = String.format("order:status:%s", event.getOrderId());

        try {
            // 🧪 테스트용: 특정 유저 지연 재현 (3초 + 5초 = 총 8초 대기 -> 5초 타임아웃 걸림)
            Thread.sleep(5000);
            if (Objects.equals(event.getUserId(), "5000") || Objects.equals(event.getUserId(), "4000")) {
                Thread.sleep(5000);
            }

            // 🧪 테스트용: 특정 조건일 때 일반 비즈니스/시스템 예외 발생 가상 재현
            if (Objects.equals(event.getUserId(), "9999")) {
                throw new IllegalStateException("PG사 잔액 부족 또는 카드 정보 오류");
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("결제 처리 스레드 인터럽트 발생", e);
        } catch (Exception e) {
            log.error("💥 [결제 로직 내부 에러 발생] OrderID: {}, 원인: {}", event.getOrderId(), e.getMessage());
            throw new RuntimeException(e.getMessage(), e);
        }

        // ✅ 예외 없이 완벽하게 성공했을 때만 아래 성공 로직 실행
        event.setStatus(PaymentStatus.SUCCESS.name());
        redisTemplate.opsForValue().set(statusKey, PaymentStatus.SUCCESS.name());

        kafkaTemplate.send("ticket-payments", event.getUserId(), event);
        log.info("🎉 [결제 완료] 유저: {}", event.getUserId());
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

    // 🚨 지연/실패 건을 DLQ 토픽으로 이관하고 보상 트랜잭션 및 실패 이벤트를 처리하는 메서드
    private void sendToDlq(TicketReservationDto event, String reason) {
        String stockKey = String.format("ticket:stock:%s", event.getTicketId());
        String statusKey = String.format("order:status:%s", event.getOrderId());

        log.warn("🚨 [DLQ 이관 및 보상 트랜잭션 시작] userId: {}, OrderID: {} (사유: {})",
                event.getUserId(), event.getOrderId(), reason);

        // 1. DTO 상태 변경 (FAILURE)
        event.setStatus(PaymentStatus.FAILURE.name());

        // 2. Redis 상태 변경 & 보상 트랜잭션 (선점했던 좌석/재고 원복)
        redisTemplate.opsForValue().set(statusKey, PaymentStatus.FAILURE.name());
        Long restoredStock = redisTemplate.opsForValue().increment(stockKey);
        log.info("🔄 [보상 완료] Redis 재고 원복 완료 (ticketId: {}, 현재재고: {})", event.getTicketId(), restoredStock);

        // 3. DLQ 전용 토픽으로 발송 (사후 모니터링/분석/Dead Letter 보관용)
        kafkaTemplate.send("ticket-reservations.DLQ", event.getUserId(), event);

        // 4. Elasticsearch 인덱싱 컨슈머 등을 위한 최종 결과 토픽 발행 (실패 이벤트)
        kafkaTemplate.send("ticket-payments", event.getUserId(), event);

        log.info("✅ [DLQ 이관 완료] OrderID: {} 실패 처리 및 이벤트 발행 완료", event.getOrderId());
    }
}
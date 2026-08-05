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
import java.util.UUID;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class TicketConsumerListener {

    private final StringRedisTemplate redisTemplate;
    private final KafkaTemplate<String, TicketReservationDto> kafkaTemplate;
    private final TicketReservationElasticRepository repository;

    // PG 결제 전용 비동기 스레드 풀
    private final ExecutorService paymentExecutor = Executors.newFixedThreadPool(50);

    @KafkaListener(topics = "ticket-reservations", groupId = "${custom.kafka.groups.payment}",
            containerFactory = "manualAckKafkaListenerContainerFactory")
    public void consumeReservation(List<TicketReservationDto> records, Acknowledgment ack) {
        log.info("📦 [consumeReservation] 비동기 수신 메시지 수: {}건", records.size());

        String batchId = UUID.randomUUID().toString().substring(0, 8);
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (TicketReservationDto event : records) {
            // 🚀 1. 비동기 스레드 풀로 결제 작업 이관
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                        processPaymentWithTimeout(event, batchId);
                    }, paymentExecutor)
                    // 💡 2. 타임아웃 설정 (5초 경과 시 TimeoutException)
                    .orTimeout(3, TimeUnit.SECONDS)
                    // 💡 3. 비동기 작업 중 에러/타임아웃 발생 시 콜백
                    .exceptionally(ex -> {
                        Throwable cause = (ex instanceof CompletionException) ? ex.getCause() : ex;

                        log.error("💥 [{}] [결제 실패] OrderID: {}, UserId: {}, Cause: {}",
                                batchId, event.getOrderId(), event.getUserId(), cause.getMessage());

                        // DLQ 이관 주석 해제하여 실제 보상 트랜잭션 진행
                        sendToDlq(event, cause.getMessage());
                        return null;
                    });

            futures.add(future);
        }

        // 🚀 4. 배치 내의 모든 비동기 작업 수습 후 오프셋 커밋
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenRun(() -> {
                    // 🧪 [멱등성 테스트용 가상 에러 삽입]
                    // Redis 상태 변경 및 ticket-payments 이벤트 발행은 완료되었으나 커밋 직전 강제 예외 발생!
//                    boolean testIdempotency = true;
//                    if (testIdempotency) {
//                        log.warn("🔥 [멱등성 테스트] Redis 상태 변경 완료 후, Kafka Ack 커밋 직전 강제 RuntimeException 발생!");
//                        throw new RuntimeException("💥 [의도된 에러] 오프셋 커밋 실패 유도 - 컨슈머 재시도 테스트");
//                    }

                    ack.acknowledge();
                    log.info("✅ [{}] [Batch Finished] 배치 {}건 비동기 처리 -> 오프셋 커밋 완료", batchId, records.size());
                });
    }

    private void processPaymentWithTimeout(TicketReservationDto event, String batchId) {
        String statusKey = String.format("order:status:%s", event.getOrderId());

        try {
            // 🛡️ 멱등성 검증: 이미 결제 성공(SUCCESS) 또는 실패(FAILURE) 처리된 주문인지 Redis에서 확인
//            String existingStatus = redisTemplate.opsForValue().get(statusKey);
//            if (PaymentStatus.SUCCESS.name().equals(existingStatus) || PaymentStatus.FAILURE.name().equals(existingStatus)) {
//                log.warn("🛡️ [멱등성 방어 성공] 이미 처리된 OrderID입니다. (현재 상태: {}) - 결제 로직을 중복 수행하지 않고 스킵합니다. | UserId: {}",
//                        existingStatus, event.getUserId());
//                return; // 중복 처리 방지!
//            }

            // 🧪 테스트용: 기본 3초 지연
//            Thread.sleep(3000);

            // 🧪 테스트용: userId "100", "200"은 추가 7초 대기 (총 8초 -> 5초 타임아웃 걸림)
            if (Objects.equals(event.getUserId(), "user100") || Objects.equals(event.getUserId(), "user200")) {
                log.info("💳 [{}] [Timeout user 결제 시작 - 6초 대기] 유저: {}, OrderID: {}", batchId, event.getUserId(), event.getOrderId());
                Thread.sleep(7000);
            }

            // 🧪 테스트용: userId "300"은 비즈니스 에러 발생
            if (Objects.equals(event.getUserId(), "user300")) {
                log.info("💳 [{}] [잔액 부족 user 결제 시작] 유저: {}, OrderID: {}", batchId, event.getUserId(), event.getOrderId());
                throw new IllegalStateException("PG사 잔액 부족 또는 카드 정보 오류");
            }

        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        // 성공 로직
        event.setStatus(PaymentStatus.SUCCESS.name());
        redisTemplate.opsForValue().set(statusKey, PaymentStatus.SUCCESS.name());

        kafkaTemplate.send("ticket-payments", event.getUserId(), event);
//        log.info("🎉 [결제 완료] 유저: {}, OrderID: {}", event.getUserId(), event.getOrderId());
    }

    @KafkaListener(topics = "ticket-payments", groupId = "${custom.kafka.groups.indexer}")
    public void consumePayment(List<TicketReservationDto> records) {
//        log.info("📦 [consumePayment] 비동기 수신 메시지 수: {}건", records.size());
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

    private void sendToDlq(TicketReservationDto event, String reason) {
        String stockKey = "ticket:stock:god";
        String statusKey = String.format("order:status:%s", event.getOrderId());

        // 1. DTO 상태 변경 (FAILURE)
        event.setStatus(PaymentStatus.FAILURE.name());

        // 2. Redis 상태 변경 & 보상 트랜잭션 (선점했던 재고 원복)
        redisTemplate.opsForValue().set(statusKey, PaymentStatus.FAILURE.name());
        Long restoredStock = redisTemplate.opsForValue().increment(stockKey);

        // 3. DLQ 토픽 및 실패 이벤트 발행
        kafkaTemplate.send("ticket-reservations.DLQ", event.getUserId(), event);
        kafkaTemplate.send("ticket-payments", event.getUserId(), event);

        log.info("🚨 [DLQ 이관 & 보상완료] OrderID: {}, UserId: {}, 원복된 재고: {}",
                event.getOrderId(), event.getUserId(), restoredStock);
    }
}
package org.example.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.dto.PaymentStatus;
import org.example.dto.ProcessResult;
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
    private final ExecutorService paymentExecutor = Executors.newFixedThreadPool(50);

    @KafkaListener(topics = "ticket-reservations", groupId = "${custom.kafka.groups.payment}",
            containerFactory = "manualAckKafkaListenerContainerFactory")
    public void consumeReservation(List<TicketReservationDto> records, Acknowledgment ack) {
        log.info("📦 [consumeReservation] 메시지 수신: {}건", records.size());

        String batchId = UUID.randomUUID().toString().substring(0, 8);
        List<CompletableFuture<ProcessResult>> futures = new ArrayList<>();

        for (TicketReservationDto event : records) {
            int asyncTimeoutSec = 5;
            CompletableFuture<ProcessResult> future = CompletableFuture.supplyAsync(() -> processPayment(event, batchId), paymentExecutor)
                    .orTimeout(asyncTimeoutSec, TimeUnit.SECONDS)
                    .handle((result, ex) -> {
                        String statusKey = String.format("order:status:%s", event.getOrderId());
                        if (ex != null) {
                            Throwable cause = (ex instanceof CompletionException) ? ex.getCause() : ex;
                            String errorMsg = cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName();

                            // 실패 시 최초 1회만 Redis에 FAILURE를 기록하도록 제한 (멱등성 상태 보호)
                            redisTemplate.opsForValue().setIfAbsent(statusKey, PaymentStatus.FAILURE.name());
                            log.error("💥 [{}] [결제 실패 처리] OrderID: {}, Cause: {}", batchId, event.getOrderId(), errorMsg);
                            return ProcessResult.failure(event, errorMsg);
                        }
                        return result;
                    });

            futures.add(future);
        }

        // 모든 비동기 스레드 완료 대기
        List<ProcessResult> results = futures.stream()
                .map(CompletableFuture::join)
                .toList();

        // 🧪 [멱등성 테스트 전용 가상 에러 강제 발생]
        // 최초 실행 시 예외가 터져 Kafka 커밋이 씹히고, Kafka가 이 배치를 통째로 다시 읽어오게 만듭니다.
        boolean testIdempotency = false;
        if (testIdempotency) {
            log.warn("🔥 [멱등성 테스트] Kafka Ack 커밋 직전 강제 RuntimeException 발생! (오프셋 커밋 실패 유도)");
            throw new RuntimeException("💥 [의도된 에러] 오프셋 커밋 실패로 재처리 루프 진입");
        }

        // 🚀 정상 흐름일 때만 커밋 및 후속 이벤트 발행
        ack.acknowledge();
        log.info("✅ [{}] [Batch Finished] 오프셋 커밋 완료", batchId);

        for (ProcessResult res : results) {
            if (res.isSuccess()) {
                kafkaTemplate.send("ticket-payments", res.getEvent().getUserId(), res.getEvent());
            } else {
                sendToDlq(res.getEvent(), res.getErrorMessage());
            }
        }
    }

    private ProcessResult processPayment(TicketReservationDto event, String batchId) {
        String statusKey = String.format("order:status:%s", event.getOrderId());

        try {
            // 🛡️ [핵심 방어선] Redis 멱등성 검증
            // 재처리 루프가 돌아서 동일한 OrderID가 다시 들어오면 결제 로직을 태우지 않고 바로 리턴합니다.
            String existingStatus = redisTemplate.opsForValue().get(statusKey);
            if (PaymentStatus.SUCCESS.name().equals(existingStatus)) {
                log.info("⏭️ [{}] [멱등성 블로킹] 이미 성공 처리된 주문입니다. 결제 요청 무시. OrderID: {}", batchId, event.getOrderId());
                return ProcessResult.success(event);
            }
            if (PaymentStatus.FAILURE.name().equals(existingStatus)) {
                log.info("⏭️ [{}] [멱등성 블로킹] 이미 실패 처리된 주문입니다. 결제 요청 무시. OrderID: {}", batchId, event.getOrderId());
                return ProcessResult.failure(event, "이미 실패한 주문");
            }

            // 🧪 가상 비즈니스 에러 조건 (user300은 무조건 실패)
            if (Objects.equals(event.getUserId(), "user300")) {
                log.info("💳 [{}] [잔액 부족 user 결제 시도] 유저: {}, OrderID: {}", batchId, event.getUserId(), event.getOrderId());
                throw new IllegalStateException("PG사 잔액 부족 또는 카드 정보 오류");
            }

            // 💸 [실제 결제 영역] PG사 연동 sleep 정상 유저 처리
            Thread.sleep(500);
            log.info("💳 [{}] [정상 결제 승인 중] 유저: {}, OrderID: {}", batchId, event.getUserId(), event.getOrderId());

            // 성공 상태 기록
            event.setStatus(PaymentStatus.SUCCESS.name());
            redisTemplate.opsForValue().set(statusKey, PaymentStatus.SUCCESS.name());

        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        return ProcessResult.success(event);
    }

    @KafkaListener(topics = "ticket-payments", groupId = "${custom.kafka.groups.indexer}")
    public void consumePayment(List<TicketReservationDto> records) {
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
        event.setStatus(PaymentStatus.FAILURE.name());
        event.setErrorMessage(reason);
        kafkaTemplate.send("ticket-reservations.DLQ", event.getUserId(), event);
        kafkaTemplate.send("ticket-payments", event.getUserId(), event);
        log.info("🚨 [DLQ 이관] OrderID: {}, UserId: {}", event.getOrderId(), event.getUserId());
    }
}
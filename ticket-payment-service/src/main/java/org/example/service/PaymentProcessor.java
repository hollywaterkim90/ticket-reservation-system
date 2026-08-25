package org.example.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.domain.OutboxEvent;
import org.example.domain.PaymentRecord;
import org.example.domain.PaymentStatus;
import org.example.dto.TicketReservationDto;
import org.example.repository.OutboxEventRepository;
import org.example.repository.PaymentRecordRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.UUID;

/**
 * 결제(돈이 오가는 경로)의 원자성을 지키는 곳. consume-process-produce 를 트랜잭셔널 outbox 로 묶는다.
 * <p>
 * 외부 청구를 사이에 두고 <b>트랜잭션을 둘로 나눈다.</b>
 * <pre>
 * [tx 1]  PENDING 선점 후 커밋      ← 청구보다 먼저 흔적을 남긴다
 *         PG 청구                  ← 트랜잭션 밖. DB 커넥션을 쥐지 않는다
 * [tx 2]  결과 확정 + outbox 적재   ← 이 둘은 원자적이어야 한다
 * </pre>
 * 청구 도중 죽어도 {@code PENDING} 이 남아 "청구가 나갔을 수 있다"를 알 수 있고,
 * {@code orderId} 가 멱등키라 재호출해도 이중 청구가 되지 않는다(미확정 건 확정은 #28 스윕이 담당).
 * <p>
 * tx2 는 결제 결과와 발행할 이벤트를 <b>같은 트랜잭션</b>에 커밋한다.
 * → "결제는 확정됐는데 결과 이벤트는 유실"이 불가능해진다.
 * 실제 발행은 {@code OutboxRelay} 가 담당하고, 재시도로 인한 중복 발행은 소비측 멱등성이 흡수한다.
 * <p>
 * 트랜잭션 경계를 {@code TransactionTemplate} 로 직접 잡는 이유: 한 메서드 안에서 두 번 커밋해야 하는데,
 * {@code @Transactional} 은 자기 자신을 호출하면 프록시를 타지 않아 경계가 생기지 않는다.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class PaymentProcessor {

    private static final String PAYMENTS_TOPIC = "ticket-payments";
    private static final String DLQ_TOPIC = "ticket-reservations.DLQ";

    private final PaymentRecordRepository paymentRepository;
    private final OutboxEventRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final PaymentGateway paymentGateway;
    private final TransactionTemplate transactionTemplate;

    public void processAndStage(TicketReservationDto event) {
        String orderId = event.getOrderId();

        // 1. tx1 — 청구 전에 PENDING 을 남긴다. 이미 확정된 주문이면 여기서 끝낸다.
        if (!Boolean.TRUE.equals(transactionTemplate.execute(tx -> reservePending(event)))) {
            log.info("⏭️ [멱등 스킵] 이미 확정된 주문 orderId:{}", orderId);
            return;
        }

        // 2. 트랜잭션 밖 — 외부 청구. sleep 동안 DB 커넥션을 쥐지 않는다.
        boolean success = true;
        String failureReason = null;
        try {
            paymentGateway.charge(orderId, event.getUserId());
        } catch (Exception e) {
            success = false;
            failureReason = e.getMessage();
        }

        // 3. tx2 — 결과 확정과 발행할 이벤트를 같은 트랜잭션에 커밋 (원자성의 핵심)
        PaymentStatus status = success ? PaymentStatus.SUCCESS : PaymentStatus.FAILURE;
        String errorMessage = failureReason;
        transactionTemplate.executeWithoutResult(tx -> confirmAndStage(event, status, errorMessage));

        log.info("💳 [결제 {}] orderId:{} → outbox 적재", status, orderId);
    }

    /**
     * tx1. 처리를 이어갈지 판단하고, 새 주문이면 {@code PENDING} 으로 선점한다.
     * <p>
     * 이미 확정된(SUCCESS/FAILURE) 주문이면 {@code false} — 오프셋 재처리로 같은 메시지가 다시 와도
     * 중복 결제가 생기지 않는다. {@code PENDING} 이면 청구 결과를 모르는 상태이므로 이어서 진행한다
     * (멱등키가 이중 청구를 막아준다).
     */
    private boolean reservePending(TicketReservationDto event) {
        return paymentRepository.findById(event.getOrderId())
                .map(existing -> existing.getStatus() == PaymentStatus.PENDING)
                .orElseGet(() -> {
                    paymentRepository.save(new PaymentRecord(event.getOrderId(), event.getUserId(),
                            event.getTicketId(), PaymentStatus.PENDING, null, Instant.now()));
                    return true;
                });
    }

    /** tx2. 결제 결과를 확정하고, 발행할 이벤트를 같은 트랜잭션에 적재한다. */
    private void confirmAndStage(TicketReservationDto event, PaymentStatus status, String errorMessage) {
        PaymentRecord record = paymentRepository.findById(event.getOrderId())
                .orElseThrow(() -> new IllegalStateException("PENDING 기록이 사라졌다 orderId:" + event.getOrderId()));
        record.confirm(status, errorMessage);

        event.setStatus(status.name());
        event.setErrorMessage(errorMessage);

        outboxRepository.save(OutboxEvent.builder()
                .id(UUID.randomUUID().toString())
                .topic(status == PaymentStatus.SUCCESS ? PAYMENTS_TOPIC : DLQ_TOPIC)
                .msgKey(event.getUserId())
                .payload(toJson(event))
                .createdAt(Instant.now())
                .build());
    }

    private String toJson(TicketReservationDto dto) {
        try {
            return objectMapper.writeValueAsString(dto);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("outbox payload 직렬화 실패", e);
        }
    }
}

package org.example.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.domain.OutboxEvent;
import org.example.domain.PaymentRecord;
import org.example.dto.PaymentStatus;
import org.example.dto.TicketReservationDto;
import org.example.repository.OutboxEventRepository;
import org.example.repository.PaymentRecordRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * 결제(돈이 오가는 경로)의 원자성을 지키는 곳. consume-process-produce 를 트랜잭셔널 outbox 로 묶는다.
 * <p>
 * 결제 결과(PaymentRecord)와 발행할 이벤트(OutboxEvent)를 <b>같은 DB 트랜잭션</b>에 커밋한다.
 * → "결제는 됐는데 결과 이벤트는 유실"(기존 리스너의 ack-후-발행 at-most-once 구멍)이 불가능해진다.
 * 실제 발행은 {@code OutboxRelay} 가 담당하고, 재시도로 인한 중복 발행은 소비측 멱등성이 흡수한다.
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

    @Transactional
    public void processAndStage(TicketReservationDto event) {
        String orderId = event.getOrderId();

        // 1. 멱등성: 이미 처리된 주문이면 스킵. 오프셋 재처리로 같은 메시지가 다시 와도
        //    중복 결제/중복 이벤트가 발생하지 않는다(PaymentRecord 존재 = 처리 완료).
        if (paymentRepository.existsById(orderId)) {
            log.info("⏭️ [멱등 스킵] 이미 처리된 주문 orderId:{}", orderId);
            return;
        }

        // 2. PG 결제(모의). 외부 부작용이라 원칙적으로 트랜잭션 밖에서 처리해야 한다.
        // ponytail: 데모라 tx 안에서 호출(sleep 동안 DB 커넥션 점유). 정석은 PENDING 선점 → PG 호출 →
        //           결과 확정의 2단계로 분리해 "청구는 됐는데 기록 실패 시 재청구" 위험까지 없애는 것.
        boolean success = true;
        String errorMessage = null;
        try {
            callPg(event);
        } catch (Exception e) {
            success = false;
            errorMessage = e.getMessage();
        }

        // 3. 결제 결과 + 발행 이벤트를 같은 트랜잭션에 커밋 (원자성의 핵심)
        String status = success ? PaymentStatus.SUCCESS.name() : PaymentStatus.FAILURE.name();
        event.setStatus(status);
        event.setErrorMessage(errorMessage);

        paymentRepository.save(new PaymentRecord(
                orderId, event.getUserId(), event.getTicketId(), status, errorMessage, Instant.now()));

        String topic = success ? PAYMENTS_TOPIC : DLQ_TOPIC;
        outboxRepository.save(OutboxEvent.builder()
                .id(UUID.randomUUID().toString())
                .topic(topic)
                .msgKey(event.getUserId())
                .payload(toJson(event))
                .status("NEW")
                .createdAt(Instant.now())
                .build());

        log.info("💳 [결제 {}] orderId:{} → outbox({}) 적재", status, orderId, topic);
    }

    // 모의 PG 연동: user300 은 잔액 부족으로 실패, 그 외는 500ms 후 승인.
    private void callPg(TicketReservationDto event) throws InterruptedException {
        if (Objects.equals(event.getUserId(), "user300")) {
            throw new IllegalStateException("PG사 잔액 부족 또는 카드 정보 오류");
        }
        Thread.sleep(500);
    }

    private String toJson(TicketReservationDto dto) {
        try {
            return objectMapper.writeValueAsString(dto);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("outbox payload 직렬화 실패", e);
        }
    }
}

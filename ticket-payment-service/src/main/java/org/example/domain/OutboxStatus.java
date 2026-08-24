package org.example.domain;

/**
 * Outbox 행의 <b>발행</b> 상태. 결제 성공 여부({@link org.example.dto.PaymentStatus})와는 다른 축이다.
 * <p>
 * {@code status=SUCCESS} 인 결제가 {@code status=NEW} 인 outbox 행을 갖는 것이 정상이며,
 * 그 구간(결제는 확정됐으나 아직 발행 전)을 표현하는 것이 outbox 의 존재 이유다.
 */
public enum OutboxStatus {
    /** 발행 대기. 릴레이가 집어갈 대상. */
    NEW,
    /** 브로커 ack 확인 완료. */
    SENT
}

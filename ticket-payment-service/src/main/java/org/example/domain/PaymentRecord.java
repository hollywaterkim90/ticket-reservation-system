package org.example.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import org.example.domain.PaymentStatus;

import java.time.Instant;

/**
 * 결제 결과(주문) 레코드. Redis order:status 를 대체한다.
 * <p>
 * PG 청구 <b>전에</b> {@code PENDING} 으로 먼저 저장되고, 결과가 나오면 {@code SUCCESS/FAILURE} 로 확정된다.
 * 그래서 멱등 판정 근거는 "행의 존재"가 아니라 <b>상태값</b>이다 — 행이 있어도 {@code PENDING} 이면
 * 청구 결과를 모르는 상태이므로 이어서 처리해야 한다.
 * <p>
 * 확정된 기록은 회계/정산에서 참조한다.
 */
@Entity
@Table(name = "payment_record")
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class PaymentRecord {
    @Id
    @Column(length = 13)                       // TSID 는 13자 고정
    private String orderId;

    @Column(length = 64, nullable = false)
    private String userId;

    @Column(length = 64, nullable = false)
    private String ticketId;

    @Enumerated(EnumType.STRING)
    @Column(length = 16, nullable = false)
    private PaymentStatus status;

    private String errorMessage;               // 실패했을 때만 채워지므로 NULL 허용

    @Column(nullable = false)                  // 스윕 배치의 조회 조건이라 비어 있으면 영영 안 잡힌다
    private Instant createdAt;

    /**
     * PG 결과로 결제를 확정한다. {@code createdAt} 은 청구를 <b>시도한</b> 시각이므로 건드리지 않는다.
     * 트랜잭션 안에서 호출하면 더티 체킹으로 반영된다.
     */
    public void confirm(PaymentStatus status, String errorMessage) {
        this.status = status;
        this.errorMessage = errorMessage;
    }
}

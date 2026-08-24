package org.example.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import org.example.dto.PaymentStatus;

import java.time.Instant;

/**
 * 결제 결과(주문) 레코드. Redis order:status 를 대체한다.
 * 존재 여부 자체가 멱등성 판정 근거(이미 처리된 orderId 면 재처리 스킵)이자,
 * 회계/정산에서 참조할 확정 기록이 된다.
 */
@Entity
@Table(name = "payment_record")
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class PaymentRecord {
    @Id
    private String orderId;
    private String userId;
    private String ticketId;
    @Enumerated(EnumType.STRING)
    private PaymentStatus status;
    private String errorMessage;
    private Instant createdAt;
}

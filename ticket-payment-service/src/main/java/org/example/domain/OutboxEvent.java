package org.example.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
// 행의 존재 자체가 "아직 발행되지 않음"을 뜻한다. 발행에 성공하면 릴레이가 지우므로 상태 컬럼이 없다.
// 인덱스는 Flyway 가 소유한다(V1__init.sql). 두 곳에서 선언하면 어긋난다.
@Table(name = "outbox_event")
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OutboxEvent {
    @Id
    @Column(length = 36)                       // UUID
    private String id;

    @Column(length = 64, nullable = false)
    private String topic;    // ticket-payments(성공) / ticket-reservations.DLQ(실패)

    @Column(length = 64, nullable = false)
    private String msgKey;   // 카프카 파티션 키. 비면 라운드로빈이 되어 순서가 깨진다.

    @Column(columnDefinition = "text", nullable = false)
    private String payload;  // 발행할 DTO 의 JSON

    @Column(nullable = false)
    private Instant createdAt;
}

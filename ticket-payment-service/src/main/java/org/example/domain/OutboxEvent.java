package org.example.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
// 행의 존재 자체가 "아직 발행되지 않음"을 뜻한다. 발행에 성공하면 릴레이가 지우므로 상태 컬럼이 없다.
// 릴레이가 created_at 순으로 집어가므로 인덱스는 그 정렬을 받쳐준다.
@Table(name = "outbox_event", indexes = @Index(name = "idx_outbox_created_at", columnList = "createdAt"))
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OutboxEvent {
    @Id
    private String id;

    private String topic;    // ticket-payments(성공) / ticket-reservations.DLQ(실패)
    private String msgKey;

    @Column(columnDefinition = "text")
    private String payload;  // 발행할 DTO 의 JSON

    private Instant createdAt;
}

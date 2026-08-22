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
import lombok.Setter;

import java.time.Instant;

@Entity
@Table(name = "outbox_event", indexes = @Index(name = "idx_outbox_status", columnList = "status"))
@Getter
@Setter
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

    private String status;   // NEW → SENT

    private Instant createdAt;
}

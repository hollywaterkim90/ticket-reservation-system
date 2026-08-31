package org.example.service;

import org.example.domain.PaymentRecord;
import org.example.domain.PaymentStatus;
import org.example.repository.OutboxEventRepository;
import org.example.repository.PaymentRecordRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link PaymentSweeper} — 청구 결과를 모른 채 남은 PENDING 을 확정하는지 검증.
 * <p>
 * 2단계 분리(#21)가 만들어내는 미아를 치우는 것이 목적이다. 스케줄러에 맡기지 않고
 * 메서드를 직접 호출해 타이밍을 통제한다.
 */
@SpringBootTest(
        classes = PaymentSweeperTest.SweepTestApp.class,
        properties = "payment.sweep.stale-after-ms=30000")
@Testcontainers
class PaymentSweeperTest {

    @SpringBootApplication(scanBasePackages = "org.example.service")
    @EnableJpaRepositories(basePackages = "org.example.repository")
    @EntityScan(basePackages = "org.example.domain")
    static class SweepTestApp {
    }

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired PaymentSweeper paymentSweeper;
    @Autowired PaymentRecordRepository paymentRepository;
    @Autowired OutboxEventRepository outboxRepository;

    @BeforeEach
    void cleanUp() {
        outboxRepository.deleteAll();
        paymentRepository.deleteAll();
    }

    /**
     * 청구 직후 죽어 PENDING 으로 남은 결제를 확정하고, 발행할 이벤트까지 적재한다.
     * <p>
     * 이 한 건이 스위퍼의 존재 이유다. 없으면 그 주문은 영원히 미확정으로 남고 후속 시스템은
     * 결제 사실을 영영 모른다 — Outbox 로 막으려던 유실이 그 앞단에서 그대로 발생한다.
     */
    @Test
    void confirmsStalePendingPayment() {
        // given: 임계 시간을 넘긴 PENDING (프로세스가 청구 직후 죽은 상황)
        savePending("order-stale", "user1", Instant.now().minusSeconds(60));

        // when
        paymentSweeper.confirmStalePayments();

        // then: SUCCESS 로 확정되고
        assertThat(paymentRepository.findById("order-stale").orElseThrow().getStatus())
                .isEqualTo(PaymentStatus.SUCCESS);

        // 발행할 이벤트도 함께 적재된다 (확정과 적재는 같은 트랜잭션)
        assertThat(outboxRepository.findAll()).singleElement()
                .satisfies(e -> assertThat(e.getTopic()).isEqualTo("ticket-payments"));
    }

    /**
     * 아직 임계 시간이 지나지 않은 PENDING 은 건드리지 않는다.
     * <p>
     * 정상 처리 중인 결제를 스위퍼가 가로채면 같은 주문을 두 곳에서 확정하려 든다.
     * 멱등키가 이중 청구는 막아주지만, 애초에 건드리지 않는 것이 맞다.
     */
    @Test
    void ignoresRecentPending() {
        // given: 방금 선점된 PENDING (아직 처리 중일 수 있다)
        savePending("order-fresh", "user1", Instant.now());

        // when
        paymentSweeper.confirmStalePayments();

        // then: 그대로 PENDING
        assertThat(paymentRepository.findById("order-fresh").orElseThrow().getStatus())
                .isEqualTo(PaymentStatus.PENDING);
        assertThat(outboxRepository.findAll()).isEmpty();
    }

    /**
     * 거절된 결제도 확정한다. PG 가 기억한 결과(거절)를 그대로 돌려주므로 FAILURE 로 확정되고
     * DLQ 로 갈 이벤트가 적재된다 — 실패도 유실되지 않는다.
     */
    @Test
    void confirmsStaleRejectedPayment() {
        savePending("order-reject", "user300", Instant.now().minusSeconds(60));

        paymentSweeper.confirmStalePayments();

        assertThat(paymentRepository.findById("order-reject").orElseThrow().getStatus())
                .isEqualTo(PaymentStatus.FAILURE);
        assertThat(outboxRepository.findAll()).singleElement()
                .satisfies(e -> assertThat(e.getTopic()).isEqualTo("ticket-reservations.DLQ"));
    }

    private void savePending(String orderId, String userId, Instant createdAt) {
        paymentRepository.save(new PaymentRecord(
                orderId, userId, "ticket:stock:god", PaymentStatus.PENDING, null, createdAt));
    }
}

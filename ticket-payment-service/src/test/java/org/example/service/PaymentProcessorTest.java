package org.example.service;

import org.example.domain.OutboxEvent;
import org.example.dto.PaymentStatus;
import org.example.dto.TicketReservationDto;
import org.example.repository.OutboxEventRepository;
import org.example.repository.PaymentRecordRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

/**
 * {@link PaymentProcessor} — "결제 결과 + 발행할 이벤트"를 <b>하나의 트랜잭션</b>으로 커밋하는지 검증.
 * <p>
 * {@code @DataJpaTest} 가 아니라 {@code @SpringBootTest} 인 이유: 검증 대상이 트랜잭션 경계 자체다.
 * 프로세서를 {@code new} 로 직접 만들면 AOP 프록시가 없어 {@code @Transactional} 이 동작하지 않고,
 * {@code @DataJpaTest} 는 테스트 전체를 한 트랜잭션으로 감싼 뒤 롤백해버려 커밋 자체가 일어나지 않는다.
 * 실제 빈으로 띄우고 진짜로 커밋시킨다(그래서 테스트마다 직접 비운다).
 * <p>
 * Postgres 컨테이너를 쓰는 이유: 여기서 만든 outbox 행을 릴레이가 {@code FOR UPDATE SKIP LOCKED}(Postgres 전용)로
 * 집어간다. H2 로는 그 경로가 검증되지 않는다.
 */
// ddl-auto 는 운영 설정(update)을 물려받지 않고 테스트에서만 create-drop 으로 덮는다.
// 컨테이너는 매번 빈 DB 로 뜨므로 스키마를 만들어줄 누군가가 필요하고, 끝나면 흔적 없이 지운다.
// (운영은 validate + 마이그레이션이 정석 — 그건 별도 과제)
@SpringBootTest(
        classes = PaymentProcessorTest.PaymentTestApp.class,
        properties = "spring.jpa.hibernate.ddl-auto=create-drop")
@Testcontainers
class PaymentProcessorTest {

    /** 결제 경로에 필요한 빈만 올린다(운영 진입점을 쓰면 리스너·ES 까지 딸려온다). */
    @SpringBootApplication(scanBasePackages = "org.example.service")
    @EnableJpaRepositories(basePackages = "org.example.repository")
    @EntityScan(basePackages = "org.example.domain")
    static class PaymentTestApp {
    }

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired PaymentProcessor paymentProcessor;
    @Autowired PaymentRecordRepository paymentRepository;

    /** 마지막 테스트에서 "outbox 저장만 실패"를 만들기 위해 스파이로 받는다. 그 외에는 실제 저장소로 동작한다. */
    @SpyBean OutboxEventRepository outboxRepository;

    @BeforeEach
    void cleanUp() {
        // 롤백에 기대지 않고 실제로 커밋하므로 이전 테스트가 남긴 데이터를 직접 지운다.
        outboxRepository.deleteAll();
        paymentRepository.deleteAll();
    }

    /**
     * 결제 성공 — 결제기록과 발행할 이벤트가 같은 트랜잭션에 함께 남는다.
     * 발행 자체는 여기서 하지 않는다. outbox 에 '적재'만 하고 실제 전송은 릴레이가 맡는다.
     */
    @Test
    void stagesPaymentAndEventTogetherOnSuccess() {
        // given
        TicketReservationDto reservation = reservation("user1", "order-ok");

        // when
        paymentProcessor.processAndStage(reservation);

        // then: 결제는 SUCCESS 로 확정되고
        assertThat(paymentRepository.findById("order-ok").orElseThrow().getStatus())
                .isEqualTo(PaymentStatus.SUCCESS);

        // 발행 대기 이벤트가 결제 완료 토픽으로 함께 적재된다
        assertThat(stagedEvent().getTopic()).isEqualTo("ticket-payments");
        assertThat(stagedEvent().getMsgKey()).isEqualTo("user1");
    }

    /**
     * 결제 실패 — 예외로 끝나지 않고 FAILURE 로 기록된 뒤 DLQ 이벤트로 적재된다.
     * 예외로 날려버리면 "무슨 일이 있었는지" 자체가 남지 않아 보상 처리도 할 수 없다.
     */
    @Test
    void stagesFailureToDlqInsteadOfThrowing() {
        // given: user300 은 모의 PG 가 잔액 부족으로 거절하는 유저
        TicketReservationDto reservation = reservation("user300", "order-fail");

        // when
        paymentProcessor.processAndStage(reservation);

        // then
        assertThat(paymentRepository.findById("order-fail").orElseThrow().getStatus())
                .isEqualTo(PaymentStatus.FAILURE);
        assertThat(stagedEvent().getTopic()).isEqualTo("ticket-reservations.DLQ");
        assertThat(stagedEvent().getPayload()).contains(PaymentStatus.FAILURE.name());   // 실패 사유가 payload 에 실려 나간다
    }

    /**
     * 재처리 방어 — 오프셋 재처리로 같은 메시지가 다시 와도 아무 일도 일어나지 않는다.
     * <p>
     * 확정 시각(createdAt)이 그대로인지를 본다. PaymentRecord 의 {@code @Id} 는 직접 넣는 orderId 라
     * 두 번 저장해도 INSERT 가 아니라 UPDATE 가 되고, 그래서 "건수가 1이다"만으로는 재처리를 잡아내지 못한다.
     */
    @Test
    void skipsAlreadyProcessedOrder() {
        // given: 한 번 처리해두고 그때의 결제 확정 시각을 기억한다
        TicketReservationDto reservation = reservation("user1", "order-1");
        paymentProcessor.processAndStage(reservation);
        Instant firstCreatedAt = paymentRepository.findById("order-1").orElseThrow().getCreatedAt();

        // when: 똑같은 메시지가 다시 들어온다
        paymentProcessor.processAndStage(reservation);

        // then: 발행할 이벤트가 하나 더 생기지 않았고
        assertThat(outboxRepository.findAll()).hasSize(1);

        // 결제 확정 시각도 그대로다 = 두 번째 호출이 정말 아무것도 하지 않았다는 뜻
        assertThat(paymentRepository.findById("order-1").orElseThrow().getCreatedAt())
                .isEqualTo(firstCreatedAt);
    }

    /**
     * 원자성 — 결제기록은 저장됐는데 이벤트 적재에서 실패하면, 결제기록도 함께 롤백된다.
     * <p>
     * 둘이 따로 커밋된다면 "결제는 남았는데 발행할 이벤트는 없는" 상태가 되고, 이건 릴레이도 복구할 수 없다.
     * Transactional Outbox 를 도입한 이유가 이 테스트다.
     */
    @Test
    void rollsBackPaymentWhenOutboxSaveFails() {
        // given: outbox 저장이 실패하는 상황을 준비한다. 여기서 던져지는 게 아니라,
        //        processAndStage 안에서 outboxRepository.save(...) 를 부르는 순간 던져지도록 등록만 한다.
        //        (DB 장애·제약 위반 등 커밋 직전 실패의 대역. paymentRepository 는 정상 동작한다)
        doThrow(new IllegalStateException("outbox 저장 실패"))
                .when(outboxRepository).save(any(OutboxEvent.class));

        // when: 프로세서는 ①결제기록 저장 → ②이벤트 적재 순서로 진행한다.
        //       ①은 정상 통과하고 ②에서 위 예외가 터진다.
        assertThatThrownBy(() -> paymentProcessor.processAndStage(reservation("user1", "order-atomic")))
                .isInstanceOf(IllegalStateException.class);

        // then: ①에서 저장했던 결제기록이 남아 있으면 안 된다
        assertThat(paymentRepository.findById("order-atomic")).isEmpty();
    }

    private TicketReservationDto reservation(String userId, String orderId) {
        return TicketReservationDto.builder()
                .userId(userId).orderId(orderId).ticketId("ticket:stock:god").build();
    }

    /** 테스트마다 예약을 1건만 처리하므로 발행 대기 이벤트도 정확히 1건이어야 한다. */
    private OutboxEvent stagedEvent() {
        List<OutboxEvent> all = outboxRepository.findAll();
        assertThat(all).hasSize(1);
        return all.get(0);
    }
}

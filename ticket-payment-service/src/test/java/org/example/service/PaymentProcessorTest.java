package org.example.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.domain.OutboxEvent;
import org.example.dto.TicketReservationDto;
import org.example.repository.OutboxEventRepository;
import org.example.repository.PaymentRecordRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE) // H2 대신 진짜 Postgres
@Testcontainers
class PaymentProcessorTest {

    // 진짜 Postgres 를 쓰는 이유: outbox 릴레이 쿼리가 'FOR UPDATE SKIP LOCKED' (Postgres 전용).
    // H2 로는 그 SQL 이 검증 안 됨 → 실 DB 컨테이너 필수. (이거 자체가 인터뷰 답변 소재)
    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired PaymentRecordRepository paymentRepository;
    @Autowired OutboxEventRepository outboxRepository;

    PaymentProcessor sut;

    @BeforeEach
    void setUp() {
        // @DataJpaTest 는 @Service 를 스캔하지 않으므로 직접 조립한다.
        sut = new PaymentProcessor(paymentRepository, outboxRepository, new ObjectMapper());
    }

    @Test
    void 같은_주문을_두_번_처리해도_결제와_이벤트는_한_번만_생긴다() {
        // given: orderId 를 고정한 예약 이벤트 1개 (orderId 가 멱등성 키)
        TicketReservationDto dto = TicketReservationDto.builder()
                .userId("user1")
                .orderId("order-1")
                .ticketId("ticket:stock:god")
                .build();

        // when: 같은 dto 로 processAndStage 를 2번 호출
        sut.processAndStage(dto);
        sut.processAndStage(dto);

        // then: 결제기록 1건, 아웃박스 1건, 상태 SUCCESS
        assertThat(paymentRepository.count()).isEqualTo(1);
        assertThat(outboxRepository.count()).isEqualTo(1);
        assertThat(paymentRepository.findAll().get(0).getStatus()).isEqualTo("SUCCESS");

        // ▶ 왜 1건인가: 2번째 호출은 existsById(orderId) 에서 걸려 PG 재호출 없이 스킵.
        //   = 오프셋 재처리로 같은 메시지가 다시 와도 이중결제/중복이벤트가 안 생긴다는 증명.
    }

    @Test
    void 성공은_ticket_payments_실패는_DLQ_로_스테이징된다() {
        // given: 정상 유저 dto 1개, 실패 유저("user300") dto 1개 (orderId 는 서로 다르게! 같으면 멱등 스킵됨)
        TicketReservationDto ok = TicketReservationDto.builder()
                .userId("user1").orderId("order-ok").ticketId("ticket:stock:god").build();
        TicketReservationDto fail = TicketReservationDto.builder()
                .userId("user300").orderId("order-fail").ticketId("ticket:stock:god").build();

        // when: 각각 processAndStage
        sut.processAndStage(ok);
        sut.processAndStage(fail);

        // then: 결제기록 상태 — 정상=SUCCESS, user300=FAILURE
        assertThat(paymentRepository.findById("order-ok").orElseThrow().getStatus()).isEqualTo("SUCCESS");
        assertThat(paymentRepository.findById("order-fail").orElseThrow().getStatus()).isEqualTo("FAILURE");

        // then: 아웃박스 topic — 성공은 ticket-payments, 실패는 DLQ (msgKey = userId 로 매칭)
        assertThat(outboxRepository.findAll())
                .filteredOn(e -> e.getMsgKey().equals("user1"))
                .singleElement()
                .extracting(OutboxEvent::getTopic)
                .isEqualTo("ticket-payments");
        assertThat(outboxRepository.findAll())
                .filteredOn(e -> e.getMsgKey().equals("user300"))
                .singleElement()
                .extracting(OutboxEvent::getTopic)
                .isEqualTo("ticket-reservations.DLQ");

        // ▶ 핵심: 실패해도 예외로 끝나지 않고 DLQ 로 '스테이징'된다 = 결제 결과와 후속 이벤트가
        //   같은 트랜잭션에 남아 유실되지 않는다는 증명. (성공/실패 경로 모두 outbox 로 흡수)
    }
}

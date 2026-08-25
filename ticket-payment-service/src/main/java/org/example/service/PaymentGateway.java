package org.example.service;

import org.example.dto.TicketReservationDto;

/**
 * 외부 결제사(PG) 연동 경계. 테스트에선 가짜, 운영에선 진짜 구현을 꽂는 '이음새'다.
 */
public interface PaymentGateway {

    /** PG 결제 요청. 승인 실패 시 예외를 던진다. */
    void charge(TicketReservationDto event);
}

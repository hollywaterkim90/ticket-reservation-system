package org.example.service;

import org.example.dto.TicketReservationDto;
import org.springframework.stereotype.Service;

import java.util.Objects;

/**
 * 모의 PG 연동: user300 은 잔액 부족으로 실패, 그 외는 500ms 후 승인.
 * 진짜 PG 를 붙일 땐 이 자리를 HttpPaymentGateway 로 교체한다.
 */
@Service
public class FakePaymentGateway implements PaymentGateway {

    @Override
    public void charge(TicketReservationDto event) {
        if (Objects.equals(event.getUserId(), "user300")) {
            throw new IllegalStateException("PG사 잔액 부족 또는 카드 정보 오류");
        }
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("PG 호출 중 인터럽트", e);
        }
    }
}

package org.example.service;

import org.springframework.stereotype.Service;

import java.util.Objects;

/**
 * 모의 PG 연동: user300 은 잔액 부족으로 실패, 그 외는 500ms 후 승인.
 * 진짜 PG 를 붙일 땐 이 자리를 HttpPaymentGateway 로 교체한다.
 */
@Service
public class FakePaymentGateway implements PaymentGateway {

    // ponytail: 멱등키 계약을 문서로만 두고 실제로 강제하지는 않는다(같은 orderId 를 두 번 부르면 두 번 청구된다).
    //           미확정 건을 재호출로 확정하는 #28 스윕에서 필요해지므로, 그때 처리 결과를 키로 기억하게 만든다.
    @Override
    public void charge(String orderId, String userId) {
        if (Objects.equals(userId, "user300")) {
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

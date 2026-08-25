package org.example.service;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 모의 PG 의 <b>멱등키 계약</b>만 확인한다. 컨테이너가 필요 없는 단위 테스트다.
 * <p>
 * 스위퍼(#28)가 미확정 건을 재호출로 확정할 수 있는 근거가 이 계약이므로, 계약이 깨지면
 * "재시도해도 이중 청구가 없다"는 주장 전체가 무너진다.
 */
class FakePaymentGatewayTest {

    private final FakePaymentGateway gateway = new FakePaymentGateway();

    /**
     * 같은 orderId 의 두 번째 요청은 <b>다시 청구하지 않고 기억해둔 결과</b>를 돌려준다.
     * <p>
     * 두 번째 호출에 정상 유저를 넣는 것이 핵심이다. 새로 판단했다면 승인이 나야 하는데,
     * 기억한 결과를 돌려주므로 첫 요청의 거절이 그대로 반복된다.
     */
    @Test
    void returnsRememberedResultForSameOrderId() {
        assertThatThrownBy(() -> gateway.charge("order-1", "user300"))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> gateway.charge("order-1", "user1"))   // 정상 유저인데도
                .isInstanceOf(IllegalStateException.class)             // 첫 결과(거절)가 그대로
                .hasMessageContaining("잔액 부족");
    }

    /** 승인도 마찬가지로 기억한다. 재호출은 새 청구 없이 그대로 통과한다. */
    @Test
    void doesNotChargeAgainAfterApproval() {
        assertThatCode(() -> gateway.charge("order-2", "user1")).doesNotThrowAnyException();
        assertThatCode(() -> gateway.charge("order-2", "user300")).doesNotThrowAnyException();
    }

    /** 키가 다르면 별개 거래다. */
    @Test
    void treatsDifferentOrderIdAsNewCharge() {
        assertThatThrownBy(() -> gateway.charge("order-3", "user300"))
                .isInstanceOf(IllegalStateException.class);
        assertThatCode(() -> gateway.charge("order-4", "user1")).doesNotThrowAnyException();
    }
}

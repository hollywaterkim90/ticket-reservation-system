package org.example.service;

import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 모의 PG 연동: user300 은 잔액 부족으로 실패, 그 외는 500ms 후 승인.
 * 진짜 PG 를 붙일 땐 이 자리를 HttpPaymentGateway 로 교체한다.
 * <p>
 * {@code orderId} 를 멱등키로 삼아 <b>처리 결과를 기억한다.</b> 같은 키로 다시 요청하면 청구하지 않고
 * 기존 결과(승인 또는 거절)를 그대로 돌려준다. 스위퍼가 미확정 건을 재호출로 확정할 수 있는 근거다.
 */
@Service
public class FakePaymentGateway implements PaymentGateway {

    /** orderId → 처리 결과. 값이 비어 있으면 승인, 들어 있으면 그 사유로 거절. */
    private final Map<String, Optional<String>> results = new ConcurrentHashMap<>();

    // ponytail: 실제 PG 의 멱등키에는 유효기간이 있지만(보통 24시간) 모의 구현이라 무기한 보관한다.
    //           TTL 을 넣는다면 만료된 키는 새 거래로 처리되어야 하고, 그때부터 이중 청구가 가능해진다.
    @Override
    public void charge(String orderId, String userId) {
        Optional<String> remembered = results.get(orderId);
        if (remembered != null) {                       // 이미 처리한 키 — 청구하지 않고 같은 결과를 돌려준다
            remembered.ifPresent(reason -> {
                throw new IllegalStateException(reason);
            });
            return;
        }

        if (Objects.equals(userId, "user300")) {
            String reason = "PG사 잔액 부족 또는 카드 정보 오류";
            results.put(orderId, Optional.of(reason));
            throw new IllegalStateException(reason);
        }

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("PG 호출 중 인터럽트", e);
        }
        results.put(orderId, Optional.empty());
    }
}

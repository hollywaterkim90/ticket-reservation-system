package org.example.service;

/**
 * 외부 결제사(PG) 연동 경계. 테스트에선 가짜, 운영에선 진짜 구현을 꽂는 '이음새'다.
 */
public interface PaymentGateway {

    /**
     * PG 결제 요청. 승인 실패 시 예외를 던진다.
     * <p>
     * {@code orderId} 는 <b>멱등키</b>다. 같은 키로 다시 요청하면 PG 는 새로 청구하지 않고
     * 기존 결과를 그대로 돌려준다는 계약이다. 그래서 조회 API 없이 <b>재시도 호출이 곧 조회</b>가 되고,
     * 청구 직후 프로세스가 죽어도 미확정 건을 안전하게 확정할 수 있다.
     * <p>
     * 실제 PG 의 멱등키에는 유효기간이 있으므로(보통 24시간) <b>확정은 그 안에 끝나야 한다.</b>
     */
    void charge(String orderId, String userId);
}

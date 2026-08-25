package org.example.repository;

import org.example.domain.PaymentRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;

public interface PaymentRecordRepository extends JpaRepository<PaymentRecord, String> {

    // 청구 결과를 모른 채 방치된 결제를 잠그고 가져온다.
    // 처리 중인 건까지 건드리지 않도록 일정 시간이 지난 것만 고른다.
    // SKIP LOCKED 로 스위퍼를 여러 개 띄워도 같은 행을 두 번 집지 않는다(OutboxRelay 와 같은 패턴).
    @Query(value = "SELECT * FROM payment_record WHERE status = 'PENDING' AND created_at < :threshold " +
            "ORDER BY created_at LIMIT 100 FOR UPDATE SKIP LOCKED", nativeQuery = true)
    List<PaymentRecord> lockStalePending(@Param("threshold") Instant threshold);
}

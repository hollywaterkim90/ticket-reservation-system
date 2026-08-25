package org.example.repository;

import org.example.domain.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, String> {

    // 남아 있는 행 = 아직 발행되지 않은 행. 잠그고 가져온다.
    // SKIP LOCKED 로 릴레이를 여러 개 띄워도 같은 행을 두 번 집지 않는다.
    // 반드시 트랜잭션 안에서 호출해야 한다(OutboxRelay 가 @Transactional).
    @Query(value = "SELECT * FROM outbox_event " +
            "ORDER BY created_at LIMIT 100 FOR UPDATE SKIP LOCKED", nativeQuery = true)
    List<OutboxEvent> lockPendingBatch();
}

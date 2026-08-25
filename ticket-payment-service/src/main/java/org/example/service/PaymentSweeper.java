package org.example.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.domain.PaymentRecord;
import org.example.dto.TicketReservationDto;
import org.example.repository.PaymentRecordRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * 청구 결과를 모른 채 남은 {@code PENDING} 결제를 확정한다.
 * <p>
 * {@link PaymentProcessor} 는 PG 청구를 트랜잭션 밖에서 하므로, 청구 직후 프로세스가 죽으면
 * <b>돈은 나갔을 수 있는데 결과를 모르는</b> 행이 남는다. 아무도 치우지 않으면 그 주문은 영원히
 * 미확정이고 후속 이벤트도 발행되지 않는다. 유실을 막으려 Outbox 를 넣고서 그 앞단에 미아를 만드는 셈이다.
 * <p>
 * 확정 로직을 새로 쓰지 않고 {@code processAndStage} 를 <b>다시 호출</b>한다. 그쪽은 이미
 * "{@code PENDING} 이면 이어서 처리"하도록 되어 있고, {@code orderId} 가 멱등키라 PG 가 새로 청구하지 않고
 * 기존 결과를 돌려준다. <b>재시도 호출이 곧 조회</b>가 되므로 조회 API 가 필요 없다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentSweeper {

    private final PaymentRecordRepository paymentRepository;
    private final PaymentProcessor paymentProcessor;
    private final TransactionTemplate transactionTemplate;

    /** 이 시간이 지나도 PENDING 이면 미확정으로 본다. 짧으면 처리 중인 건을 건드리고, 길면 미확정이 오래 간다. */
    @Value("${payment.sweep.stale-after-ms:30000}")
    private long staleAfterMs;

    @Scheduled(fixedDelayString = "${payment.sweep.interval-ms:10000}")
    public void confirmStalePayments() {
        List<PaymentRecord> stale = fetchStale();
        if (stale.isEmpty()) return;

        log.warn("🧹 [스윕] 미확정 결제 {}건 발견 — 재확인 시작", stale.size());
        for (PaymentRecord record : stale) {
            try {
                paymentProcessor.processAndStage(toEvent(record));
            } catch (Exception e) {
                // 상태를 바꾸지 않는다. PENDING 으로 남아 다음 주기에 다시 집힌다(릴레이와 같은 방식).
                log.error("❌ [스윕] 확정 실패 orderId:{} → 다음 주기 재시도. cause:{}", record.getOrderId(), e.getMessage());
            }
        }
    }

    /**
     * 대상을 고르는 <b>짧은</b> 트랜잭션. 확정은 각 {@code processAndStage} 가 자기 트랜잭션에서 한다.
     * <p>
     * 배치 전체를 한 트랜잭션으로 감싸지 않는 이유가 두 가지다.
     * <ul>
     *   <li>PG 청구(건당 500ms)를 하는 동안 커넥션과 행 잠금을 쥐게 된다 — #21 에서 없앤 문제가 되살아난다.</li>
     *   <li>한 건의 실패가 트랜잭션을 rollback-only 로 만들어 나머지 건까지 커밋하지 못하게 된다.</li>
     * </ul>
     * 잠금을 여기서 놓으므로 {@code SKIP LOCKED} 는 동시에 조회하는 스위퍼끼리만 겹침을 막는다.
     * 그 뒤에 두 스위퍼가 같은 행을 집더라도 <b>멱등키 덕분에 이중 청구가 되지 않는다.</b>
     */
    private List<PaymentRecord> fetchStale() {
        Instant threshold = Instant.now().minus(Duration.ofMillis(staleAfterMs));
        return transactionTemplate.execute(tx -> paymentRepository.lockStalePending(threshold));
    }

    /** PaymentRecord 에 남은 정보만으로 원래 이벤트를 되살린다(확정에 필요한 것은 이 셋뿐이다). */
    private TicketReservationDto toEvent(PaymentRecord record) {
        return TicketReservationDto.builder()
                .orderId(record.getOrderId())
                .userId(record.getUserId())
                .ticketId(record.getTicketId())
                .build();
    }
}

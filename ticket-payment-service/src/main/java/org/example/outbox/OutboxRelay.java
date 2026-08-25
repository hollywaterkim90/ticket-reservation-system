package org.example.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.domain.OutboxEvent;
import org.example.dto.TicketReservationDto;
import org.example.repository.OutboxEventRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * outbox 에 남아 있는 행을 주기적으로 읽어 각 행의 topic(성공→ticket-payments / 실패→DLQ)으로 발행하고 삭제한다.
 * <ul>
 *   <li>{@code FOR UPDATE SKIP LOCKED} 로 릴레이 다중화 시에도 같은 행을 두 번 집지 않는다.</li>
 *   <li>{@code send().get()} 으로 브로커 ack 확인. 실패 행은 <b>지우지 않아</b> 다음 주기에 재시도(at-least-once).</li>
 *   <li>발행 이력은 남기지 않는다. 발행된 메시지는 Kafka 가 보관하므로 outbox 에 사본을 둘 이유가 없고,
 *       그래서 상태 컬럼도 정리 배치도 필요 없다. <b>행의 존재 자체가 "미발행"이다.</b></li>
 * </ul>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxRelay {

    private final OutboxEventRepository outboxRepository;
    private final KafkaTemplate<String, TicketReservationDto> kafkaTemplate;
    private final ObjectMapper objectMapper;

    /** 브로커 ack 대기 시간. 짧으면 정상 발행도 실패로 보고 재시도해 중복이 늘고, 길면 배치가 한 건에 묶여 밀린다. */
    @Value("${outbox.relay.send-timeout-ms:5000}")
    private long sendTimeoutMs;

    @Scheduled(fixedDelayString = "${outbox.relay.interval-ms:1000}")
    @Transactional
    public void publishPending() {
        List<OutboxEvent> batch = outboxRepository.lockPendingBatch();
        if (batch.isEmpty()) return;

        int sent = 0;
        for (OutboxEvent e : batch) {
            try {
                TicketReservationDto dto = objectMapper.readValue(e.getPayload(), TicketReservationDto.class);
                kafkaTemplate.send(e.getTopic(), e.getMsgKey(), dto).get(sendTimeoutMs, TimeUnit.MILLISECONDS);
                outboxRepository.delete(e);
                sent++;
            } catch (Exception ex) {
                log.error("❌ outbox 발행 실패 id:{} → 행을 남겨 다음 주기 재시도. cause:{}", e.getId(), ex.getMessage());
            }
        }
        log.info("📤 [OutboxRelay] {}/{}건 발행 완료", sent, batch.size());
    }
}

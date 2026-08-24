package org.example.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.domain.OutboxEvent;
import org.example.domain.OutboxStatus;
import org.example.dto.TicketReservationDto;
import org.example.repository.OutboxEventRepository;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * outbox 의 NEW 행을 주기적으로 읽어 각 행의 topic(성공→ticket-payments / 실패→DLQ)으로 발행하고 SENT 로 마킹.
 * <ul>
 *   <li>{@code FOR UPDATE SKIP LOCKED} 로 릴레이 다중화 시에도 같은 행을 두 번 집지 않는다.</li>
 *   <li>{@code send().get()} 으로 브로커 ack 확인. 실패 행은 SENT 로 넘기지 않아 다음 주기에 재시도(at-least-once).</li>
 * </ul>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxRelay {

    private final OutboxEventRepository outboxRepository;
    private final KafkaTemplate<String, TicketReservationDto> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Scheduled(fixedDelayString = "${outbox.relay.interval-ms:1000}")
    @Transactional
    public void publishPending() {
        List<OutboxEvent> batch = outboxRepository.lockNewBatch();
        if (batch.isEmpty()) return;

        int sent = 0;
        for (OutboxEvent e : batch) {
            try {
                TicketReservationDto dto = objectMapper.readValue(e.getPayload(), TicketReservationDto.class);
                kafkaTemplate.send(e.getTopic(), e.getMsgKey(), dto).get(5, TimeUnit.SECONDS);
                e.setStatus(OutboxStatus.SENT);
                sent++;
            } catch (Exception ex) {
                log.error("❌ outbox 발행 실패 id:{} → 다음 주기 재시도. cause:{}", e.getId(), ex.getMessage());
            }
        }
        log.info("📤 [OutboxRelay] {}/{}건 발행 완료", sent, batch.size());
    }
}

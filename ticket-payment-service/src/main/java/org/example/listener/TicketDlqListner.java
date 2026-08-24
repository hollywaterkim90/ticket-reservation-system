package org.example.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.dto.TicketReservationDto;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;


@Component
@RequiredArgsConstructor
@Slf4j
public class TicketDlqListner {

    private final StringRedisTemplate redisTemplate;

    /**
     * 결제 실패 이벤트를 소비해 보상 트랜잭션을 수행한다.
     * 재고는 예매 서비스가 Redis 로 소유하므로 동일 키(ticketId)로 +1 원복한다.
     * (결제 결과 FAILURE 확정은 이미 PaymentProcessor 가 PaymentRecord 에 기록했다.)
     */
    @KafkaListener(
            topics = "ticket-reservations.DLQ",
            groupId = "${custom.kafka.groups.dlq}",
            containerFactory = "manualAckKafkaListenerContainerFactory"
    )
    public void consumeDlq(TicketReservationDto event, Acknowledgment ack,
                           @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key) {
        log.error("🚨 [DLQ 포착] 격리 - key:{}, errorMsg:{}", key, event.getErrorMessage());

        try {
            Long restoredStock = redisTemplate.opsForValue().increment(event.getTicketId());
            log.info("🔄 [보상 트랜잭션] 재고 원복 완료 (ticket:{}, 현재재고:{})", event.getTicketId(), restoredStock);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("💥 [CRITICAL] DLQ 보상 트랜잭션 처리 중 에러 발생!", e);
        }
    }
}

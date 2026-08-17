package org.example.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.dto.PaymentStatus;
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

    @KafkaListener(
            topics = "ticket-reservations.DLQ",
            groupId = "${custom.kafka.groups.dlq}",
            containerFactory = "manualAckKafkaListenerContainerFactory"
    )
    public void consumeDlq(TicketReservationDto event, Acknowledgment ack, @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key) {
        log.error("🚨 [DLQ 포착] 메시지 격리 - key: {}, errorMsg:{}", key, event.getErrorMessage());

        String orderStatusKey = "order:status:" + event.getOrderId();
        try {
            // 🔄 보상 트랜잭션 1: Redis 선점 재고 +1 원복 (재고 누수 방지)
            //    예매 서비스가 ticketId 를 재고 키로 사용하므로 동일한 키로 원복한다.
            Long restoredStock = redisTemplate.opsForValue().increment(event.getTicketId());

            // 🔄 보상 트랜잭션 2: 주문 상태를 FAILURE 로 확정
            redisTemplate.opsForValue().set(orderStatusKey, PaymentStatus.FAILURE.name());

            log.info("🔄 [보상 트랜잭션 완수] Redis 재고 원복 완료 (현재재고: {}) | 주문상태: {}",
                    restoredStock, PaymentStatus.FAILURE.name());

            ack.acknowledge();
        } catch (Exception e) {
            log.error("💥 [CRITICAL] DLQ 보상 트랜잭션 처리 중 에러 발생!", e);
        }
    }
}

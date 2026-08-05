package org.example.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.dto.TicketReservationDto;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;


@Component
@RequiredArgsConstructor
@Slf4j
public class TicketDlqListner {

    private final StringRedisTemplate redisTemplate;

    @KafkaListener(topics = "ticket-reservations.DLQ", groupId = "${custom.kafka.groups.dlq}")
    public void consumeDlq(TicketReservationDto event, Acknowledgment ack) {
        log.error("🚨 [DLQ 포착] 3회 재시도 실패! Poison Pill 메시지 격리 - OrderID: {}", event.getOrderId());

        String stockKey = "ticket:stock:" + event.getTicketId();
        String orderStatusKey = "order:status:" + event.getOrderId();

        try {
            // 🔄 보상 트랜잭션 1: Redis 선점 재고 +1 원복 (재고 누수 방지)
            Long restoredStock = redisTemplate.opsForValue().increment(stockKey);

            // 🔄 보상 트랜잭션 2: 주문 상태 FAILED 변경
            redisTemplate.opsForValue().set(orderStatusKey, "FAILED");

            log.info("🔄 [보상 트랜잭션 완수] Redis 재고 원복 완료 (현재재고: {}) | 주문상태: FAILED", restoredStock);

            // DLQ 메시지 처리 완료 후 커밋
            ack.acknowledge();

        } catch (Exception e) {
            log.error("💥 [CRITICAL] DLQ 보상 트랜잭션 처리 중 에러 발생!", e);
        }
    }
}

package org.example.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
@Slf4j
@RequiredArgsConstructor
public class TicketSerivce {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final StringRedisTemplate redisTemplate;

    public ResponseEntity<String> sendToReservationTopic(String userId, String ticketId) {
        // 💡 [자동화] Redis에 재고 키가 없으면 초기값(예: 10개)으로 세팅
        // TODO: 어드민 페이지에서 재고 추가하도록.
        String stockKey = String.format("ticket:%s:stock", ticketId);
        redisTemplate.opsForValue().setIfAbsent(stockKey, "100");

        // 1. 1인 1매 중복 예약 방지 (Redis SetIfAbsent)
        String redisKey = String.format("ticket:%s:user:%s", ticketId, userId);
        Boolean isFirstRequest = redisTemplate.opsForValue().setIfAbsent(redisKey, "reserved", Duration.ofHours(1));

        if (Boolean.FALSE.equals(isFirstRequest)) {
            log.warn("[중복 예약 거부] 이미 신청한 유저입니다. 유저: {}, 티켓: {}", userId, ticketId);
            // 409 Conflict 또는 400 Bad Request가 적절합니다.
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body("이미 예약을 신청하셨습니다. (1인 1매만 가능)");
        }

        // 2. 선착순 재고 차감 (Redis DECR)
        Long remainStock = redisTemplate.opsForValue().decrement(stockKey);

        if (remainStock == null || remainStock < 0) {
            log.warn("[선착순 마감 실패] 재고가 모두 소진되었습니다. 유저: {}, 티켓: {}", userId, ticketId);
            // 400 Bad Request 또는 422 Unprocessable Entity가 적절합니다.
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body("티켓 재고가 모두 소진되어 예약이 마감되었습니다.");
        }

        // 3. 앞서 작성한 카프카 전송 로직 (동기 방식 예시)
        try {
            String message = String.format("{\"ticketId\":\"%s\",\"userId\":\"%s\"}", ticketId, userId);
            kafkaTemplate.send("ticket-reservations", message).get();

            return ResponseEntity.ok("티켓 예약 요청이 성공적으로 접수되었습니다.");
        } catch (Exception e) {
            log.error("❌ 카프카 전송 실패: {}", e.getMessage());
            return ResponseEntity.internalServerError().body("시스템 오류로 예약 요청에 실패했습니다.");
        }
    }
}

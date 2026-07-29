package org.example.service;

import io.hypersistence.tsid.TSID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.dto.PaymentStatus;
import org.example.dto.TicketReservationDto;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
@Slf4j
@RequiredArgsConstructor
public class TicketService {

    private final KafkaTemplate<String, TicketReservationDto> kafkaTemplate;
    private final StringRedisTemplate redisTemplate;
    final String stockKey = "ticket:stock:god";

    public ResponseEntity<String> sendToReservationTopic(TicketReservationDto requestDto) {
        String userKey = String.format("user:%s", requestDto.getUserId());

        // 1. 중복 예약 체크
        checkDuplicate(userKey);

        // 2. Redis에 임시 상태 저장. (유저가 현재 결제 진행 상태 조회용, TTL 10분)
        redisTemplate.opsForValue().set(userKey, PaymentStatus.PENDING.name(), Duration.ofMinutes(10));

        // 3. 선착순 재고 차감
        checkFirstComeFirstServed();

        // 4. Kafka 메시지 생성 (status: PENDING)
        requestDto.setOrderId(TSID.Factory.getTsid().toString());
        requestDto.setStatus(PaymentStatus.PENDING.name());
        try {
            log.info("send message: user: {}, orderId: {}", requestDto.getUserId(), requestDto.getOrderId());
            // 통과 시 1차 토픽으로 고속 발행 (acks=1 설정 활성화)
            kafkaTemplate.send("ticket-reservations", requestDto);

            return ResponseEntity.ok("선착순 통과! 결제 대기열에 진입했습니다.");
        } catch (Exception e) {
            log.error("❌ 카프카 전송 실패: {}", e.getMessage());
            return ResponseEntity.internalServerError().body("시스템 오류로 예약 요청에 실패했습니다.");
        }
    }

    private void checkDuplicate(String userKey) {
        // 1. 1인 1매 중복 예약 방지 (Redis SetIfAbsent)
        Boolean isFirstRequest = redisTemplate.opsForValue().setIfAbsent(userKey, PaymentStatus.PENDING.name(), Duration.ofMinutes(10));

        if (Boolean.FALSE.equals(isFirstRequest)) {
            log.warn("[중복 예약 거부] 이미 신청한 유저입니다. 유저: {}, 티켓: {}", dto.getUserId(), dto.getTicketId());
            // 409 Conflict 또는 400 Bad Request가 적절합니다.
            ResponseEntity.status(HttpStatus.CONFLICT)
                    .body("이미 예약을 신청하셨습니다. (1인 1매만 가능)");
        }
    }

    private void checkFirstComeFirstServed() {
        Long remainStock = redisTemplate.opsForValue().decrement(stockKey);

        if (remainStock == null || remainStock < 0) {
            log.warn("[선착순 마감] 재고가 모두 소진되었습니다. 100개 추가.");
            ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body("티켓 재고가 모두 소진되어 예약이 마감되었습니다.");
        }
    }
}

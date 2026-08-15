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
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;

@Service
@Slf4j
@RequiredArgsConstructor
public class TicketService {

    private final KafkaTemplate<String, TicketReservationDto> kafkaTemplate;
    private final StringRedisTemplate redisTemplate;

    public ResponseEntity<String> sendToReservationTopic(TicketReservationDto requestDto) {
        String userId = String.format("user:%s", requestDto.getUserId());
        // 재고 카운터의 Redis 키 = 요청의 ticketId. 티켓 종류별로 독립적인 재고를 갖는다.
        String stockKey = requestDto.getTicketId();

        // 1. 중복 예약 체크
        checkDuplicate(userId);

        // 2. 선착순 재고 차감
        checkFirstComeFirstServed(stockKey);

        // 3. Kafka 메시지 생성 (status: PENDING)
        requestDto.setOrderId(TSID.Factory.getTsid().toString());
        requestDto.setStatus(PaymentStatus.RESERVED.name());

        try {
            log.info("send message: user: {}, ticket: {}, remainStock: {}, orderId: {}",
                    requestDto.getUserId(),
                    stockKey,
                    redisTemplate.opsForValue().get(stockKey),
                    requestDto.getOrderId());

            // 통과 시 1차 토픽으로 고속 발행
            kafkaTemplate.send("ticket-reservations", requestDto);

            return ResponseEntity.ok("선착순 통과! 결제 대기열에 진입했습니다.");
        } catch (Exception e) {
            log.error("❌ 카프카 전송 실패: {}", e.getMessage());

            // 🔄 보상 트랜잭션: 카프카 전송 실패 시 Redis 재고 및 유저 상태 원복
            redisTemplate.opsForValue().increment(stockKey);
            redisTemplate.delete(userId);

            return ResponseEntity.internalServerError().body("시스템 오류로 예약 요청에 실패했습니다.");
        }
    }

    private void checkDuplicate(String userId) {
        // 1인 1매 중복 예약 방지, userId를 redis key로 사용
        Boolean isFirstRequest = redisTemplate.opsForValue().setIfAbsent(userId, PaymentStatus.PENDING.name(), Duration.ofMinutes(10));

        if (Boolean.FALSE.equals(isFirstRequest)) {
            log.warn("[중복 예약 거부] 이미 신청한 유저입니다. 유저: {}", userId);
            // ❌ return 대신 예외를 던져서 실행 흐름을 즉시 중단시킵니다.
            throw new ResponseStatusException(HttpStatus.CONFLICT, "이미 예약을 신청하셨습니다. (1인 1매만 가능)");
        }
    }

    private void checkFirstComeFirstServed(String stockKey) {
        // 1) 등록된 티켓인지 먼저 확인
        if (Boolean.FALSE.equals(redisTemplate.hasKey(stockKey))) {
            log.warn("[미등록 티켓] 등록되지 않은 티켓입니다. ticket: {}", stockKey);
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "존재하지 않는 티켓입니다.");
        }

        // 2) 선착순 재고 차감
        Long remainStock = redisTemplate.opsForValue().decrement(stockKey);

        if (remainStock == null || remainStock < 0) {
            // 차감했다가 거절하는 경우 반드시 되돌린다.
            // 되돌리지 않으면 마감 이후 요청마다 카운터가 계속 내려가고,
            // 이후 DLQ 보상 원복(+1)이 실제 재고와 어긋나게 된다.
            redisTemplate.opsForValue().increment(stockKey);

            log.warn("[선착순 마감] 재고가 모두 소진되었습니다. ticket: {}", stockKey);
            // ❌ return 대신 예외를 던져서 카프카 전송을 막습니다.
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "티켓 재고가 모두 소진되어 예약이 마감되었습니다.");
        }
    }
}

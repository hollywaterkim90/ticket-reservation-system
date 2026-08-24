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

/**
 * 예매(입장) 경로는 처리량이 생명이다. Redis 원자적 DECR 로 초과 판매만 막고 고속으로 발행한다.
 * 원자성 비용(트랜잭셔널 outbox)은 돈이 오가는 결제 서비스에만 쓴다.
 * <p>
 * 트레이드오프: 발행이 비동기라 200 응답 후 전송이 실패하면 사후 보상 콜백으로 재고/유저를 원복한다.
 * 그 짧은 창에서 크래시가 겹치면 "재고는 깎였는데 주문 없음"인 유령 재고가 드물게 남을 수 있다(감수 or 정산 sweep).
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class TicketService {

    private final KafkaTemplate<String, TicketReservationDto> kafkaTemplate;
    private final StringRedisTemplate redisTemplate;

    public ResponseEntity<String> sendToReservationTopic(TicketReservationDto requestDto) {
        String userId = String.format("user:%s", requestDto.getUserId());
        String stockKey = requestDto.getTicketId();   // 재고 카운터 키 = ticketId

        // 1. 1인 1매 중복 예약 방지 (선점)
        checkDuplicate(userId);

        // 2. 선착순 재고 차감. 실패(미등록/마감) 시 방금 선점한 중복키를 풀어 재시도 가능하게 한다.
        try {
            checkFirstComeFirstServed(stockKey);
        } catch (RuntimeException e) {
            redisTemplate.delete(userId);
            throw e;
        }

        requestDto.setOrderId(TSID.Factory.getTsid().toString());
        requestDto.setStatus(PaymentStatus.RESERVED.name());

        // 3. 고속 발행(논블로킹). send() 는 CompletableFuture 를 반환하므로 비동기 전송 실패는
        //    동기 try/catch 가 아니라 콜백에서만 잡힌다. 실패 시에만 재고/유저를 원복(보상)한다.
        kafkaTemplate.send("ticket-reservations", requestDto)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("❌ 카프카 전송 실패 → 보상 원복. orderId:{}, cause:{}",
                                requestDto.getOrderId(), ex.getMessage());
                        redisTemplate.opsForValue().increment(stockKey);
                        redisTemplate.delete(userId);
                    }
                });

        return ResponseEntity.ok("선착순 통과! 결제 대기열에 진입했습니다.");
    }

    private void checkDuplicate(String userId) {
        Boolean isFirstRequest = redisTemplate.opsForValue()
                .setIfAbsent(userId, PaymentStatus.PENDING.name(), Duration.ofMinutes(10));
        if (Boolean.FALSE.equals(isFirstRequest)) {
            log.warn("[중복 예약 거부] user: {}", userId);
            throw new ResponseStatusException(HttpStatus.CONFLICT, "이미 예약을 신청하셨습니다. (1인 1매만 가능)");
        }
    }

    private void checkFirstComeFirstServed(String stockKey) {
        if (Boolean.FALSE.equals(redisTemplate.hasKey(stockKey))) {
            log.warn("[미등록 티켓] ticket: {}", stockKey);
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "존재하지 않는 티켓입니다.");
        }

        Long remainStock = redisTemplate.opsForValue().decrement(stockKey);
        if (remainStock == null || remainStock < 0) {
            // 차감했다가 거절하면 반드시 되돌린다(마감 후 카운터가 계속 내려가 DLQ 보상 +1 과 어긋나는 것 방지).
            redisTemplate.opsForValue().increment(stockKey);
            log.warn("[선착순 마감] ticket: {}", stockKey);
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "티켓 재고가 모두 소진되어 예약이 마감되었습니다.");
        }
    }
}

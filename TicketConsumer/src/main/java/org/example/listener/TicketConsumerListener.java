package org.example.listener;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.protocol.types.Field;
import org.example.elasticsearch.document.TicketReservationDocument;
import org.example.elasticsearch.repository.TicketReservationElasticRepository;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;

@Slf4j
@Component
@RequiredArgsConstructor
public class TicketConsumerListener {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final TicketReservationElasticRepository repository;

    @KafkaListener(topics = "ticket-reservations", groupId = "ticket-group-es")
    public void consume(String message) {
        try {
            JsonNode jsonNode = objectMapper.readTree(message);
            String userId = jsonNode.get("userId").asText();
            String ticketId = jsonNode.get("ticketId").asText();

            // 1. 1인 1매 중복 예약 방지 (Redis SetIfAbsent)
            String redisKey = String.format("ticket:%s:user:%s", ticketId, userId);
            Boolean isFirstRequest = redisTemplate.opsForValue().setIfAbsent(redisKey, "reserved", Duration.ofHours(1));
            if (Boolean.FALSE.equals(isFirstRequest)) {
                // 이미 레디스에 키가 존재한다면? 중복 유저이므로 무시하고 리턴 (고속 필터링)
                log.warn("[중복 예약 거부] 이미 신청한 유저입니다. 유저: {}, 티켓: {}", userId, ticketId);
                return;
            }

            // 2. 선착순 재고 차감 (Redis DECR)
            String stockKey = String.format("ticket:%s:stock", ticketId);
            // DECR 연산은 원자적으로 실행되며, 줄어든 후의 결과 값을 반환합니다.
            Long remainStock = redisTemplate.opsForValue().decrement(stockKey);
            if (remainStock == null || remainStock < 0) {
                // 재고가 없는데 마이너스가 되었다면 (또는 재고 설정이 안 되어 있다면)
                log.warn("[선착순 마감 실패] 재고가 모두 소진되었습니다. 유저: {}, 티켓: {}", userId, ticketId);
                return;
            }

            log.info("[선착순 통과] 🎉 축하합니다! 결제 단계로 진입합니다. 남은 선착순 재고: {} | 유저: {}", remainStock, userId);

            // TODO: 프로듀서 구조 수정
            // 3. 💡 [구조 변경] 멱등성이 보장되는 결제 전용 토픽(ticket-payments)으로 메시지 전환 전송!
            // 이 method가 실행될 때 application.yml에 설정한 acks=all과 enable.idempotence가 작동합니다.
            kafkaTemplate.send("ticket-payments", message);

        } catch (Exception e) {
            log.error("❌ 데이터 처리 중 에러 발생: {}", e.getMessage(), e);
        }
    }


    // [2단계 파이프라인] 멱등성이 보장된 고신뢰성 결제 처리 리스너
    @KafkaListener(topics = "ticket-payments", groupId = "ticket-group-payment")
    public void consumePayment(String message) {
        try {
            JsonNode jsonNode = objectMapper.readTree(message);
            String userId = jsonNode.get("userId").asText();
            String ticketId = jsonNode.get("ticketId").asText();

            log.info("[결제 파이프라인 수신] 중복/유실 없는 안전 구간 진입 완료. 유저: {}", userId);

            // 💳 외부 결제 API 연동 혹은 실제 DB 트랜잭션이 발생하는 무거운 비즈니스 로직 구간
            // (여기서는 로그로 대체하고 바로 ES에 적재합니다.)
            log.info("💳 [결제 승인 중...] 외부PG사 연동 처리 중... 유저: {}", userId);

            // 최종 성공 데이터만 무거운 저장소인 Elasticsearch에 안전하게 안착
            TicketReservationDocument document = new TicketReservationDocument(userId, ticketId);
            TicketReservationDocument saved = repository.save(document);

            log.info("🏁 [최종 예매 확정 완료] ES 적재 완료! 문서 ID: {}, 유저: {}", saved.getId(), saved.getUserId());
        } catch (Exception e) {
            log.error("결제 확정 처리 중 에러 발생: ", e);
        }
    }
}
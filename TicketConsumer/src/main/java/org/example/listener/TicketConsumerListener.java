package org.example.listener;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.protocol.types.Field;
import org.example.elasticsearch.document.TicketReservationDocument;
import org.example.elasticsearch.repository.TicketReservationElasticRepository;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;

@Slf4j
@Component
@RequiredArgsConstructor
public class TicketConsumerListener {

    private final TicketReservationElasticRepository repository;
    private final StringRedisTemplate redisTemplate; // Redis 제어용 템플릿 추가
    private final ObjectMapper objectMapper = new ObjectMapper();

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

            // 처음 통과한 유저만 엘라스틱서치(ES)에 적재
            TicketReservationDocument document = new TicketReservationDocument(userId, ticketId);
            TicketReservationDocument saved = repository.save(document);

            log.info("🎉 [ES 적재 완료] 문서 ID: {} | 유저: {} | 티켓: {}",
                    saved.getId(),
                    saved.getUserId(),
                    saved.getTicketId()
            );
        } catch (Exception e) {
            log.error("❌ 데이터 처리 중 에러 발생: {}", e.getMessage(), e);
        }
    }
}
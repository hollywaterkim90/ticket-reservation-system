package org.example.listener;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.elasticsearch.document.TicketReservationDocument;
import org.example.elasticsearch.repository.TicketReservationElasticRepository;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

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

            // 💡 Redis Key 설계: "ticket:예약ID:user:유저ID" 형식으로 중복 방지 키 생성
            String redisKey = String.format("ticket:%s:user:%s", ticketId, userId);

            // [핵심] Redis의 setIfAbsent(NX 옵션)를 사용해 원자적(Atomic)으로 중복 체크
            // 값이 없으면 true를 반환하고 값을 저장, 있으면 false를 반환 (1인 1매 보장)
            Boolean isFirstRequest = redisTemplate.opsForValue().setIfAbsent(redisKey, "reserved", Duration.ofHours(1));

            if (Boolean.FALSE.equals(isFirstRequest)) {
                // 이미 레디스에 키가 존재한다면? 중복 유저이므로 무시하고 리턴 (고속 필터링)
                log.warn("[중복 예약 거부] 이미 신청한 유저입니다. 유저: {}, 티켓: {}", userId, ticketId);
                return;
            }

            // 처음 통과한 유저만 엘라스틱서치(ES)에 적재
            TicketReservationDocument document = new TicketReservationDocument(userId, ticketId);
            TicketReservationDocument saved = repository.save(document);

            System.out.println("========================================");
            System.out.println("[ES 적재 완료] 문서 ID: " + saved.getId() + " | 유저: " + saved.getUserId() + " | 티켓: " + saved.getTicketId());
            System.out.println("========================================");

        } catch (Exception e) {
            log.error("❌ 데이터 처리 중 에러 발생: {}", e.getMessage(), e);
        }
    }
}
package org.example.listener;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.example.elasticsearch.document.TicketReservationDocument;
import org.example.elasticsearch.repository.TicketReservationElasticRepository;
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
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final TicketReservationElasticRepository repository;

    @KafkaListener(topics = "ticket-reservations", groupId = "ticket-group-es")
    public void consume(String message) {
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
            log.error("❌ 데이터 처리 중 에러 발생: {}", e.getMessage(), e);
        }
    }
}
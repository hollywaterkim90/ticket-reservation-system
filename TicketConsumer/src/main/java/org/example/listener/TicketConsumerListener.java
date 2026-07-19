package org.example.listener;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.elasticsearch.document.TicketReservationDocument;
import org.example.elasticsearch.repository.TicketReservationElasticRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TicketConsumerListener {

    private final TicketReservationElasticRepository repository;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public TicketConsumerListener(TicketReservationElasticRepository repository) {
        this.repository = repository;
    }

    @KafkaListener(topics = "ticket-reservations", groupId = "ticket-group-es")
    public void consume(String message) {
        try {
            JsonNode jsonNode = objectMapper.readTree(message);
            String userId = jsonNode.get("userId").asText();
            String ticketId = jsonNode.get("ticketId").asText();

            // 엘라스틱서치 Document 생성 및 저장
            TicketReservationDocument document = new TicketReservationDocument(userId, ticketId);
            TicketReservationDocument saved = repository.save(document);

            System.out.println("========================================");
            System.out.println("[ES 적재 완료] 문서 ID: " + saved.getId() + " | 유저: " + saved.getUserId() + " | 티켓: " + saved.getTicketId());
            System.out.println("========================================");

        } catch (Exception e) {
            System.err.println("ES 적재 중 에러 발생: " + e.getMessage());
        }
    }
}
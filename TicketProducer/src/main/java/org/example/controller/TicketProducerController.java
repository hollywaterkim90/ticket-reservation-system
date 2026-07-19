package org.example.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class TicketProducerController {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping("/reserve")
    public String reserveTicket(@RequestParam String userId, @RequestParam String ticketId) {
        String message = String.format("{\"userId\":\"%s\", \"ticketId\":\"%s\"}", userId, ticketId);

        // "ticket-reservations" 토픽으로 메시지 전송
        kafkaTemplate.send("ticket-reservations", message);

        return "예약 요청 완료";
    }
}
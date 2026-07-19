package org.example.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class TicketProducerController {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final StringRedisTemplate redisTemplate;

    @PostMapping("/reserve")
    public String reserveTicket(@RequestParam String userId, @RequestParam String ticketId) {
        // 💡 [자동화] Redis에 재고 키가 없으면 초기값(예: 10개)으로 세팅
        // TODO: 어드민 페이지에서 재고 추가하도록.
        String stockKey = "ticket:" + ticketId + ":stock";
        redisTemplate.opsForValue().setIfAbsent(stockKey, "10");

        String message = String.format("{\"userId\":\"%s\", \"ticketId\":\"%s\"}", userId, ticketId);

        // "ticket-reservations" 토픽으로 메시지 전송
        kafkaTemplate.send("ticket-reservations", message);

        return "예약 요청 완료";
    }
}
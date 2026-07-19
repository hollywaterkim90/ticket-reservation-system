package org.example.controller;

import lombok.RequiredArgsConstructor;
import org.example.Service.TicketSerivce;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class TicketProducerController {

    private final TicketSerivce ticketSerivce;

    @PostMapping("/reserve")
    public ResponseEntity<String> reserveTicket(@RequestParam String userId, @RequestParam String ticketId) {
        return ticketSerivce.sendToReservationTopic(userId, ticketId);
    }
}
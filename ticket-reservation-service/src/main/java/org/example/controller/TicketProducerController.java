package org.example.controller;

import lombok.RequiredArgsConstructor;
import org.example.dto.TicketReservationDto;
import org.example.service.TicketService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class TicketProducerController {

    private final TicketService ticketSerivce;

    @PostMapping("/reserve")
    public ResponseEntity<String> reserveTicket(@RequestBody TicketReservationDto dto) {
        return ticketSerivce.sendToReservationTopic(dto);
    }
}
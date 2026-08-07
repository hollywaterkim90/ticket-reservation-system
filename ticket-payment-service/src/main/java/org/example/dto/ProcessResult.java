package org.example.dto;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

// 결과를 추적하기 위한 가벼운 래퍼 헬퍼 클래스
@Getter
@RequiredArgsConstructor
public class ProcessResult {
    private final TicketReservationDto event;
    private final boolean success;
    private final String errorMessage;

    public static ProcessResult success(TicketReservationDto event) {
        return new ProcessResult(event, true, null);
    }

    public static ProcessResult failure(TicketReservationDto event, String msg) {
        return new ProcessResult(event, false, msg);
    }
}
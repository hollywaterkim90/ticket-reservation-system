package org.example.dto;
import lombok.*;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class TicketReservationDto {

    private String userId;     // 유저 ID
    private String orderId;    // 주문 ID
    private String status;     // 결제 상태 (예: PENDING, PAID, FAILED)
    private String ticketId;   // 티켓 ID
    private String errorMessage;    // 오류 발생시 저장. DLQ 토픽에서 확인.

}
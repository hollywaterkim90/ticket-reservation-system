package org.example.dto;
import lombok.*;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class TicketReservationDto {
    // TODO: common 프로젝트로 이관.

    private String orderId;    // 주문 ID
    private String status;     // 결제 상태 (예: PENDING, PAID, FAILED)
    private String userId;     // 유저 ID
    private String ticketId;   // 티켓 ID

}
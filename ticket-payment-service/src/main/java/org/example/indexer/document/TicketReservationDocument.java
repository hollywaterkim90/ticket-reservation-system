package org.example.indexer.document;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.DateFormat;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.time.Instant;

@Document(indexName = "ticket-reservations") // 엘라스틱서치 인덱스 지정
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TicketReservationDocument {

    @Id
    private String id;

    private String userId;
    private String ticketId;
    private String orderId;
    private String status;

    @Field(type = FieldType.Date, format = DateFormat.date_hour_minute_second)
    private String createdAt;

    @JsonProperty("@timestamp")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "UTC")
    private Instant timestamp;
}
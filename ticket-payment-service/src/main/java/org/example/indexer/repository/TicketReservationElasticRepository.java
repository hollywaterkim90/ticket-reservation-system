package org.example.indexer.repository;

import org.example.indexer.document.TicketReservationDocument;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

public interface TicketReservationElasticRepository extends ElasticsearchRepository<TicketReservationDocument, String> {
}
package org.example.Init;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class RedisDataInitializer {

    private final StringRedisTemplate redisTemplate;

    @EventListener(ApplicationReadyEvent.class)
    public void init() {
        // 1. 현재 Redis DB의 모든 키 삭제
        redisTemplate.execute((RedisCallback<Object>) connection -> {
            connection.serverCommands().flushDb();
            return null;
        });

        // 티켓 종류별 초기 재고. 키는 요청의 ticketId 와 동일하다.
        // 여기에 등록되지 않은 ticketId 로 요청이 들어오면 404(존재하지 않는 티켓)로 거절된다.
        Map<String, String> initialStock = Map.of(
                "ticket:stock:god", "5000",
                "ticket:stock:iu", "1000",
                "ticket:stock:bts", "3000"
        );

        initialStock.forEach((key, stock) -> redisTemplate.opsForValue().set(key, stock));

        log.info("🚀 [ApplicationReadyEvent] Redis 초기 재고 세팅 완료: {}", initialStock);
    }
}

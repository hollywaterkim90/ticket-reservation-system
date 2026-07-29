package org.example.Init;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class RedisDataInitializer {

    private final StringRedisTemplate redisTemplate;

    @EventListener(ApplicationReadyEvent.class)
    public void init() {
        redisTemplate.opsForValue().setIfAbsent("ticket:stock:god", "100");
        log.info("🚀 [ApplicationReadyEvent] 모든 준비 완료 후 Redis 초기 재고 세팅!");
    }
}

package org.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling   // OutboxRelay 의 @Scheduled 폴링 활성화
public class Main {
    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }
}
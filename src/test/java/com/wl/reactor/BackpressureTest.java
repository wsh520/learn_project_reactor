package com.wl.reactor;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;

/**
 * 被压
 */
@SpringBootTest
public class BackpressureTest {

    /**
     * 丢弃策略
     *
     * @throws InterruptedException
     */
    @Test
    void baseBackpressureDropTest() throws InterruptedException {
        Flux.interval(Duration.ofMillis(1))
                .onBackpressureDrop(drop -> System.out.println("drop: " + drop))
                .publishOn(Schedulers.boundedElastic()) // 委托给线程池
                .subscribe( v -> {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    System.out.println(v);
                });

        Thread.sleep(1000);
    }

    /**
     * 缓存策略
     * # BufferOverflowStrategy
     *
     * @throws InterruptedException
     */
    @Test
    void baseBackpressureBufferTest() throws InterruptedException {
        Flux.interval(Duration.ofMillis(30))
                .onBackpressureBuffer(500, BufferOverflowStrategy.DROP_OLDEST) // 生产者缓存
                .publishOn(Schedulers.boundedElastic()) // 委托给线程池，消费者默认缓存256
                .subscribe( v -> {
                    try {
                        Thread.sleep(60);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    System.out.println(v);
                });

        Thread.sleep(10000);
    }

    @Test
    void testBackpressure() {
        Flux<Integer> flux = Flux.range(1, 5);
        StepVerifier.create(flux, 1) // ⭐ 初始只 request 1 条
                .expectNext(1) // 收到 1
                .thenRequest(2) // ⭐ 再要 2 条
                .expectNext(2, 3) // 收到 2、3
                .thenRequest(10) // 再要一大把
                .expectNext(4, 5) // 收到剩下的
                .expectComplete()
                .verify();
    }
}

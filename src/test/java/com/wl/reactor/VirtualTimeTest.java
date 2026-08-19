package com.wl.reactor;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import java.time.Duration;

@SpringBootTest
public class VirtualTimeTest {

    @Test
    void testInterval() {
        // 注意：要用 Supplier 形式（withVirtualTime 内部才能替换调度器）
        StepVerifier.withVirtualTime(() ->
                        Flux.interval(Duration.ofSeconds(1)).take(3))
                .expectSubscription()
                .expectNoEvent(Duration.ofSeconds(1)) // 前 1 秒内没有事件
                .expectNext(0L) // 第 1 秒收到 0
                .thenAwait(Duration.ofSeconds(2)) // ⭐ 快进 2 秒
                .expectNext(1L, 2L) // 收到 1、2
                .expectComplete()
                .verify(); // 整个测试瞬间完成，不真等 3秒
    }

    @Test
    void testTimeout() {
        // 验证 5 秒超时逻辑，但测试瞬间跑完
        StepVerifier.withVirtualTime(() ->
                        Flux.just("x").concatWith(Flux.never())
                                .timeout(Duration.ofSeconds(5)))
                .expectNext("x")
                .thenAwait(Duration.ofSeconds(5)) // 快进 5 秒
                .expectError(java.util.concurrent.TimeoutException.class)
                .verify();
    }
}

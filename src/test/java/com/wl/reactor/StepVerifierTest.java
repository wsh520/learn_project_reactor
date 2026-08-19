package com.wl.reactor;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;

@SpringBootTest
public class StepVerifierTest {

    @Test
    void testBasic() {
        // ⭐ 核心：使用 withVirtualTime 开启虚拟时空！
        // 里面传入你要测试的流的生成逻辑
        StepVerifier.withVirtualTime(() ->
                        Flux.just(1, 2, 3).delayElements(Duration.ofHours(1))
                )
                // 1. 我们断言，如果时间没有流逝，现在什么都不会发生
                .expectSubscription()
                .expectNoEvent(Duration.ofMinutes(59))

                // 2. 魔法：上帝之手拨快时钟！直接让系统时间往后快进 3 个小时
                .thenAwait(Duration.ofHours(3))

                // 3. 验收结果：虽然真实世界才过去几毫秒，但程序以为 3 小时过去了，立刻吐出所有数据
                .expectNext(1, 2, 3)
                .expectComplete()
                .verify(); // 真实耗时：约 50 毫秒！
    }

    @Test
    void testError() {
        Flux<Integer> flux = Flux.just(1, 2, 3, 4)
                .concatWith(Flux.error(new RuntimeException("炸了")))
                .doOnNext(v -> System.out.println("输出的值为：" + v))
                .doOnError(v -> System.out.println("错误信息为： " + v.getMessage()));

        StepVerifier.create(flux)
                .expectNext(1)
                .expectNext(2, 3, 4) // 乖乖把中间的数据消费掉
                .expectErrorMessage("炸了") // 期望以指定错误消息结束
                .verify();
    }

}

package com.wl.reactor;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

@SpringBootTest
public class CancelTest {


    @Test
    void testCancel() {

        // 正确写法：只装配流和探针，绝对不要在这里调用 .subscribe()！
        Flux<Integer> flux = Flux.range(1, 100)
                .doOnNext(v -> System.out.println("👀 [探针] 看到数据流过： " + v)) // 用 doOnNext 替代 subscribe 的打印功能
                .doOnCancel(() -> System.out.println("🛑 [探针] 收到 Cancel 信号！流被强行掐断！"))
                .doFinally(signal -> System.out.println("🏁 [探针] 流生命周期结束，最终死因: " + signal));

        System.out.println("🚀 开始测试...");

        // 让 StepVerifier 成为唯一的订阅者，触发整个流水线！
        StepVerifier.create(flux)
                .expectNext(1)
                .expectNext(2)
                .thenCancel()
                .verify();

        System.out.println("✅ 测试剧本执行完毕！");
    }
}

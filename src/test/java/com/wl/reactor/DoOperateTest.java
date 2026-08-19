package com.wl.reactor;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.time.Duration;

@SpringBootTest
public class DoOperateTest {

    /**
     * 副作用操作符演示
     *
     * @throws InterruptedException
     */
    @Test
    void doOperateTest() throws InterruptedException {
        Disposable disposable = Flux.interval(Duration.ofMillis(100))
                .map(v -> {
                    if (v == 3) throw new RuntimeException("4.5 数据出错了");
                    return v;
                })
                .doOnSubscribe(sub -> System.out.println("1. 开始订阅了。" + sub))
                .doOnNext(v -> System.out.println("2. 即将发出: " + v))
                .doOnCancel(() -> System.out.println("4. 订阅取消了！ "))
                .doOnError(System.err::println)
                .doFinally(signalType -> System.out.println("5. 无论如何都会执行， 信号=" + signalType))
                .subscribe(v -> System.out.println("③ 订阅者收到: " + v));

        Thread.sleep(700); // 收几个后
        disposable.dispose(); // ⭐ 主动取消 → 触发 doOnCancel 和 doFinally
        Thread.sleep(200);
    }
}

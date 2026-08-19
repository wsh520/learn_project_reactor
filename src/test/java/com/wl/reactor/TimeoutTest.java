package com.wl.reactor;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

@SpringBootTest
public class TimeoutTest {

    /**
     * 元素之间加入间隔，限速输出
     *
     */
    @Test
    void delayElementsTest() throws InterruptedException {
        Flux.just("你","好","世","界").delayElements(Duration.ofMillis(100)).subscribe(System.out::println);

        Thread.sleep(2000);
    }

    /**
     * timeout：若两个元素之间超过指定时间没来新数据，就抛超时错误 —— ⭐ 超时停止
     */
    @Test
    void timeoutTest() {
        Flux.just("你","好","世","界").publishOn(Schedulers.boundedElastic()).map(v -> {
            try {
                if (v.equalsIgnoreCase("好")) Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return v;
        }).timeout(Duration.ofMillis(500)).subscribe(
                v -> System.out.println("timeout值: " + v),
                err -> System.out.println("超时触发: " + err.getMessage())
        );
    }

    /**
     * [0, 1, 2] [3, 4, 5] buffer 满了就触发一次
     */
    @Test
    void bufferTest() {
        Flux.interval(Duration.ofMillis(100)).buffer(3).subscribe(System.out::println);

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * 流开始运行...
     * sample: 2
     * sample: 5
     * sample: 8
     * sample: 9
     * 主线程优雅退出！
     * <p>
     * 周期性采样， 只保留每个周期最后一个， 用于降频
     */
    @Test
    void sampleTest() {

        Flux.interval(Duration.ofMillis(100))
                .take(10)                           // 限制只产生 10 个元素（0 到 9，耗时约 1 秒）
                .sample(Duration.ofMillis(300))     // 每 300 毫秒采样一次最新数据
                .doOnNext(v -> System.out.println("sample: " + v)) // 替换 subscribe 的打印逻辑
                .blockLast();                       // 阻塞主线程，直到 take(10) 结束流

    }

}

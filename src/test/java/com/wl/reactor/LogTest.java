package com.wl.reactor;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;

import java.time.Duration;

@SpringBootTest(classes = LogTest.class)
public class LogTest {

    /**
     *
     * 插在链路的任意位置， 它会原样透传数据，
     * 同时把经过它的每一个信号（ onSubscribe 、 request 、 onNext 、 onComplete 、 cancel ） 打印出 来
     *
     */
    @Test
    void testLog() throws InterruptedException {

        Flux.range(1, 10)
                .delayElements(Duration.ofMillis(100))
                .map(x -> "Token_" + x)
                .log() //
                .subscribe(System.out::println);

        Thread.sleep(1000);
    }

    @Test
    void testCheckPoint() {
        Flux.range(1, 5)
                .map(i -> i * 10)
                .checkpoint("乘10之后") // 路标1
                .map(i -> 100 / (i - 30)) // 当 i==30 时除零出错
                .checkpoint("除法这一步") // 路标2：错误会指向这里
                .subscribe(
                        System.out::println,
                        err -> {
                            System.out.println("❌ 发生错误，请看堆栈：");
                            // ⭐ 核心修正：必须打印完整的异常堆栈！
                            err.printStackTrace(System.out);
                        },
                        () -> System.out.println("completed"));
    }

    @Test
    void testHooksDebug() {
        Hooks.onOperatorDebug();
        Flux.range(1, 10)
                .map(i -> i / (i - 2)) // i==2 时除零
                .subscribe(
                        System.out::println,
                        err -> {
                            System.out.println("❌ 带完整装配堆栈的错误:");
                            err.printStackTrace(System.out); // ⭐ 必须打印堆栈！
                        });

    }

}

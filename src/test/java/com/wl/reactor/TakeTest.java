package com.wl.reactor;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Flux;

import java.time.Duration;

@SpringBootTest
public class TakeTest {

    @Test
    void takeTest() throws InterruptedException {

        // 只获取十个 输出0~9
//        Flux.interval(Duration.ofMillis(100)).take(10).subscribe(System.out::println);

        // 取到满足条件 输出 0 1 2 3
//        Flux.interval(Duration.ofMillis(100)).takeUntil(i -> i>=3).subscribe(System.out::println);

        // 只要满足条件就取，一旦不满足立即停止 输出1， 第一个数是奇数
//        Flux.just(1,2,3,4,5,6,7,8,9,10).takeWhile(i -> i %2 > 0).subscribe(System.out::println);

        // 只在指定时间内取，超时自动停止
        Flux.interval(Duration.ofMillis(200)).take(Duration.ofMillis(700))
                .subscribe(v -> System.out.println("takeDuration: " + v)); // 约 0,1,2

        Thread.sleep(2000);

    }
}

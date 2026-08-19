package com.wl.reactor;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import java.time.Duration;

@SpringBootTest
public class SchedulersTest {

    /**
     * 都是运行在 main 线程上
     */
    @Test
    void threadBaseTest() {
        Flux.just(1, 2, 3)
                .map(i -> {
                    System.out.println("map 运行在: " +
                            Thread.currentThread().getName());
                    return i * 10;
                })
                .subscribe(v -> System.out.println("收到 " + v + " 在: " +
                        Thread.currentThread().getName()));
    }

    /**
     *
     * interval 绑定在了 Schedulers.parallel()
     *
     * @throws InterruptedException
     */
    @Test
    void SchedulersTest1() throws InterruptedException {

        Flux.interval(Duration.ofMillis(30), Schedulers.boundedElastic()).take(30)
                .onBackpressureBuffer(50, BufferOverflowStrategy.DROP_OLDEST)
//                .subscribeOn(Schedulers.boundedElastic()) // 在此处制定源头线程池不生效，可以在interval中设定
                .map(i -> {
                            System.out.println("map 运行在: " + Thread.currentThread().getName());
                            return i * 10;
                        }
                )
                .publishOn(Schedulers.boundedElastic())
                .subscribe(v -> System.out.println("收到 " + v + " 在: " + Thread.currentThread().getName()),
                        err -> System.out.println("err 线程为： " + Thread.currentThread().getName()),
                        () -> System.out.println("complete 线程为： " + Thread.currentThread().getName()));

        Thread.sleep(1200);

    }

    /**
     * subscribeOn 可以指定源头使用线程池
     *
     * @throws InterruptedException
     */
    @Test
    void SchedulersTest2() throws InterruptedException {

        Flux.range(1, 30)
                .onBackpressureBuffer(50, BufferOverflowStrategy.DROP_OLDEST)
                .subscribeOn(Schedulers.boundedElastic()) // 影响订阅那一刻起、 整条链的源头
                .map(i -> {
                            System.out.println("map 运行在: " + Thread.currentThread().getName());
                            return i * 10;
                        }
                )
                .publishOn(Schedulers.boundedElastic()) // 从它往下的操作符切换到新线程， 放在哪就从哪切
                .subscribe(v -> System.out.println("收到 " + v + " 在: " + Thread.currentThread().getName()),
                        err -> System.out.println("err 线程为： " + Thread.currentThread().getName()),
                        () -> System.out.println("complete 线程为： " + Thread.currentThread().getName()));

        Thread.sleep(1200);

    }


    @Test
    void SchedulersTest3() throws InterruptedException {
        Flux.just(1, 2).publishOn(Schedulers.boundedElastic())
                .subscribe(v -> System.out.println(" " + v + " @ " +
                        Thread.currentThread().getName()));
        Flux.just(1, 2).publishOn(Schedulers.parallel())
                .subscribe(v -> System.out.println(" " + v + " @ " +
                        Thread.currentThread().getName()));
        Thread.sleep(300);
    }


    @Test
    void SchedulersTest4() throws InterruptedException {
        Flux.just("data")
                .map(s -> log("① 源头附近的 map", s))
                .subscribeOn(Schedulers.boundedElastic()) // 影响源头开始的线程
                .map(s -> log("② publishOn 之前的 map", s))
                .publishOn(Schedulers.parallel()) // 从这往下切到 parallel
                .map(s -> log("③ publishOn 之后的 map", s))
                .subscribe(s -> log("④ subscribe", s));
        Thread.sleep(300);
    }

    String log(String stage, String s) {
        return s;
    }
}

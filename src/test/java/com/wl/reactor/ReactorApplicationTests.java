package com.wl.reactor;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootTest
class ReactorApplicationTests {

    /**
     * 停止流测试
     *
     * @throws InterruptedException
     */
    @Test
    void disposableTest () throws InterruptedException {
        Disposable disposable = Flux.interval(Duration.ofMillis(500)).subscribe(System.out::println);

        Thread.sleep(1600);

        // 停止流
        disposable.dispose();

        System.out.println("Disposable is :" + disposable.isDisposed());
    }

    /**
     * subscribe 的处理方式
     */
    @Test
    void subscribeTest () {
        Flux<Integer> range = Flux.range(1, 5);

        // 方式一，直接订阅 什么都不做,触发流动但不处理数据（一般用于只关心副作用的场景）
        System.out.println("方式一");
        range.subscribe();

        // 方式二，订阅后只关注 next
        System.out.println("方式二");
        range.subscribe( v -> System.out.println(v),err -> System.out.println(err));

        // 方式三 订阅后关注next和err和complete
        System.out.println("方式三");
        range.subscribe( v -> System.out.println(v),err -> System.out.println(err), () -> System.out.println("已完成，方式三"));

    }


    /**
     *
     * 无限流
     *
     * @throws InterruptedException
     */
    @Test
    void fluxInterval() throws InterruptedException {
        Flux<Long> interval = Flux.interval(Duration.ofMillis(500));
        interval.subscribe(System.out::println);

        Thread.sleep(21000);
    }

    /**
     * 延迟执行方式
     */
    @Test
    void fromCallable() {
        Mono<String> stringMono = Mono.fromCallable(() -> {
            return "2 + 3 = " + (2 / 0); // 这里不会立即执行
        });
        stringMono.subscribe(System.out::println); // 订阅的时候才会执行，报错

    }


    /**
     * 立即执行和延迟执行的案例
     *
     * @throws InterruptedException
     */
    @Test
    void fluxDefer() throws InterruptedException {
        Mono<Long> just = Mono.just(System.currentTimeMillis()); // 这里会立即执行，报错的话会直接终止流程
        Mono<Long> defer = Mono.defer(() -> Mono.just(System.currentTimeMillis()));
        just.subscribe(System.out::println);
        defer.subscribe(times -> {
            System.out.println("defer times: " + times);
        });
        TimeUnit.SECONDS.sleep(1);
        just.subscribe(System.out::println);
        defer.subscribe(times -> {
            System.out.println("defer times: " + times);
        });
        TimeUnit.SECONDS.sleep(1);
        just.subscribe(System.out::println);
        defer.subscribe(times -> {
            System.out.println("defer times: " + times);
        });

    }

    /**
     * 直接列出元素
     */
    @Test
    void fluxCreateWay1() {
        Flux<Integer> just = Flux.just(1, 2, 3);
        just.subscribe(System.out::println);
    }

    /**
     * 从集合数据创建
     */
    @Test
    void fluxCreateWay2() {
        List<Integer> list = Arrays.asList(1, 2, 3);

        Flux.fromIterable(list).subscribe(System.out::println);
    }


    /**
     * 从 start 开始的 count 个连续整数
     */
    @Test
    void fluxCreateWay3() {
        Flux.range(0, 10).subscribe(System.out::println);
    }

    /**
     * 单个值
     */
    @Test
    void fluxCreateWay4() {
        Mono.just(1).subscribe(System.out::println);
    }

    /**
     * 空值
     */
    @Test
    void fluxCreateWay5() {
        Mono.empty().subscribe(
                onNext -> System.out.println("不会执行"),
                onError -> {
                },
                () -> System.out.println("empty 完成， 没有数据"));
    }

    /**
     * 同步、 逐个生成， 每次只能发一个元素
     * 两个参数
     * 生成斐波那契数列
     */
    @Test
    void fluxGenerate() {
        Flux.generate(() -> new long[]{0, 1}, (state, sink) -> {

            sink.next(state[0]); // 发射一个元素
            long next = state[0] + state[1];
            state[0] = state[1];
            state[1] = next;
            if (state[0] > 50) sink.complete(); // 满足条件就结束
            return state;
        }).subscribe(System.out::println);
    }

    /**
     * 一个参数
     */
    @Test
    void fluxGenerate2() {
        AtomicInteger count = new AtomicInteger();
        Flux.generate(sink -> {
            if (count.get() >= 10) {
                sink.complete();
            }
            sink.next(count);
            count.getAndIncrement();

        }).subscribe(System.out::println);
    }

    /**
     * 三个参数
     */
    @Test
    void fluxGenerate3() {
        // 模拟一个多行文本内容，实际应用中可能是 new FileReader("xxx.txt")
        String mockFileContent = "第一行\n第二行\n第三行";

        Flux.generate(() -> new BufferedReader(new StringReader(mockFileContent)), (reader, sink) -> {
            try {
                String line = reader.readLine();
                if (line != null && !line.isEmpty()) {
                    sink.next(line);
                } else {
                    sink.complete();
                }
                return reader;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, (reader) -> {
            try {
                if (reader != null) reader.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).subscribe(
                line -> System.out.println("消费: " + line),
                error -> System.err.println("报错: " + error),
                () -> System.out.println("流正常完成")
        );

    }

    /***
     * 可异步、 可批量发射 两个参数
     *
     * OverflowStrategy:
     *  生产的慢，消费的快， prefetch 的容量限制就不会影响流，如果消费的慢，生产的快prefetch则会根据OverflowStrategyc策略来进行处理，
     *  ERROR则是直接报错，
     *  BUFFER则是正常缓存输出，
     *  LATEST 则是输出容量的数量后还会输出最后一个
     *  DROP 则是丢球后面的值
     *
     *
     * @throws InterruptedException
     */
    @Test
    void fluxCreate() throws InterruptedException {
        Flux.create(sink -> {
                    for (int i = 0; i < 100; i++) {
                        sink.next(i);
                    }
                    sink.complete();
                }, FluxSink.OverflowStrategy.IGNORE).publishOn(Schedulers.boundedElastic(), false, 20) // 这里的20相当于容量，在不同OverflowStrategy下含义不一样
                .subscribe(
                        v -> {
                            try {
                                // 【核心修改 3】：让下游变成“慢性子”，每 0.1 秒才能处理一条
                                TimeUnit.MILLISECONDS.sleep(100);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            System.out.println("   📥 下游成功消费: " + v);
                        },
                        err -> System.err.println("发生错误: " + err),
                        () -> System.out.println("已完成")
                );

        // 因为下游现在是后台异步线程，主线程需要等待一会儿，防止程序立刻退出
        TimeUnit.SECONDS.sleep(20);
    }

    /**
     * 可异步、 可批量发射 单个参数
     */
    @Test
    void fluxCreate2() {
        Flux.create(sink -> {
            for (int i = 0; i < 50; i++) {
                sink.next(i);
            }
            sink.complete();
        }).subscribe(System.out::println);
    }

    @Test
    void testHotStream() throws InterruptedException {
        // 1. 创建一个多播 (multicast) 的 Sink，它天然就是一个热流发射器
        // onBackpressureBuffer() 表示如果消费者处理不过来，先放在缓冲区
        Sinks.Many<String> hotSink = Sinks.many().multicast().onBackpressureBuffer();

        // 2. 将 Sink 转换为普通的 Flux 供消费者订阅
        Flux<String> hotFlux = hotSink.asFlux();

        System.out.println("--- 观众A 进入直播间 ---");
        hotFlux.subscribe(msg -> System.out.println("观众A 收到: " + msg));

        // 3. 开始发射数据
        hotSink.tryEmitNext("第1首歌");
        hotSink.tryEmitNext("第2首歌");

        // 模拟时间流逝
        Thread.sleep(1000);

        System.out.println("--- 观众B 姗姗来迟，进入直播间 ---");
        // 4. 观众B 在第2首歌之后才订阅
        hotFlux.subscribe(msg -> System.out.println("观众B 收到: " + msg));

        // 5. 继续发射数据
        hotSink.tryEmitNext("第3首歌");
        hotSink.tryEmitNext("第4首歌");

    }

    @Test
    void testColdStream() {

        Flux<String> cold = Flux.just("1", "2", "3", "4", "5", "6", "7", "8", "9");

        System.out.println("------------第一个订阅者------------");
        cold.subscribe(System.out::println);
        System.out.println();
        System.out.println();
        System.out.println("------------第二个订阅者------------");
        cold.subscribe(System.out::println);

    }
}

package com.wl.reactor;


import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

@SpringBootTest
public class SinksTest {


    /**
     * Sinks.one测试 只发单个元素
     */
    @Test
    void sinksOneTest() {
        Sinks.One<String> one = Sinks.one();
        one.asMono().subscribe(v -> System.out.println("订阅者收到： " + v));

        // 这里可以发送信息到订阅者
        one.tryEmitValue("hello");
        one.tryEmitValue("world"); // Mono是只有一个元素或者没有元素，这里第二个是发不出的
    }

    /**
     *
     * unicast 单播
     *
     * @throws InterruptedException
     */
    @Test
    void sinksManyUnicastTest() throws InterruptedException {

        Sinks.Many<Object> objectMany = Sinks.many().unicast().onBackpressureBuffer();
        objectMany.asFlux().subscribe(
                v -> System.out.println("Sinks.many 收到信息为： " + v),
                err -> System.out.println("错误信息为： " + err),
                () -> System.out.println("completed"));

        String answer = "欢迎来到卢本伟广场，我不信你能十七张牌秒我!  大哥，我错了。[DONE] 我没错我骗你的，略略略";


        Thread producer = new Thread(() -> {
            for (String word : answer.split("")) {
                if (word.equals("[")) {
                    objectMany.tryEmitComplete();
                    break;
                } else {
                    objectMany.tryEmitNext(word);
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        producer.start();

        producer.join(); // 等生成结束

        Thread.sleep(300);

    }


    /**
     * 多播
     */
    @Test
    void sinksMulticastTest() throws InterruptedException {

        Sinks.Many<Object> multicast = Sinks.many().multicast().onBackpressureBuffer();

        Flux<Object> multicastFlux = multicast.asFlux();

        multicastFlux.subscribe(
                v -> System.out.println("订阅者一 收到信息为： " + v),
                err -> System.out.println("订阅者一 收到错误信息为： " + err),
                () -> System.out.println("订阅者一 已完成！" ));

        multicastFlux.subscribe(
                v -> System.out.println("订阅者二 收到信息为： " + v),
                err -> System.out.println("订阅者二 收到错误信息为： " + err),
                () -> System.out.println("订阅者二 已完成！" ));

        String answer = "欢迎来到卢本伟广场，我不信你能十七张牌秒我!  大哥，我错了。[DONE] 我没错我骗你的，略略略";


        Thread producer = new Thread(() -> {
            for (String word : answer.split("")) {
                if (word.equals("[")) {
                    multicast.tryEmitComplete();
                    break;
                } else {
                    multicast.tryEmitNext(word);
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        producer.start();

        producer.join(); // 等生成结束

        Thread.sleep(500);

    }

    /**
     * 回放
     */
    @Test
    void sinksReplayTest() throws InterruptedException {
        // 核心改变：使用 replay().limit(10)
        // 意思是：不管谁什么时候来订阅，立刻把最近缓存的 10 条数据“甩”给他！
        Sinks.Many<String> replaySink = Sinks.many().replay().limit(10);
        Flux<String> replayFlux = replaySink.asFlux();

        // ==========================================
        // 1. 订阅者一（早早就来了，全程都在听）
        // ==========================================
        replayFlux.subscribe(
                v -> System.out.println("🧑 订阅者一(早到) 收到: " + v),
                err -> System.err.println("错误: " + err),
                () -> System.out.println("🧑 订阅者一 已完成！")
        );

        // ==========================================
        // 2. 生产者：开始发射 15 条数据 (模拟聊天室发了 15 条消息)
        // ==========================================
        System.out.println("\n📢 --- 生产者开始发送前 15 条消息 ---");
        for (int i = 1; i <= 15; i++) {
            replaySink.tryEmitNext("消息-" + i);
            Thread.sleep(50); // 稍微停顿一下
        }

        // ==========================================
        // 3. 订阅者二（迟到了，在第 15 条消息发完后才加入）
        // ==========================================
        System.out.println("\n🏃‍♂️ --- 订阅者二 此时姗姗来迟，开始订阅 ---");
        replayFlux.subscribe(
                v -> System.out.println("    😎 订阅者二(迟到) 收到: " + v),
                err -> System.err.println("错误: " + err),
                () -> System.out.println("    😎 订阅者二 已完成！")
        );

        // ==========================================
        // 4. 生产者：继续发射最后 3 条数据
        // ==========================================
        System.out.println("\n📢 --- 生产者继续发送最后 3 条消息 ---");
        for (int i = 16; i <= 18; i++) {
            replaySink.tryEmitNext("消息-" + i);
            Thread.sleep(50);
        }

        // 结束流
        replaySink.tryEmitComplete();
    }

    @Test
    void sinksStopTest() throws InterruptedException {
        Sinks.Many<Object> objectMany = Sinks.many().unicast().onBackpressureBuffer();
        objectMany.asFlux().subscribe(
                v -> System.out.println("Sinks.many 收到信息为： " + v),
                err -> System.out.println("错误信息为： " + err),
                () -> System.out.println("completed"));

        String answer = "欢迎来到卢本伟广场，我不信你能十七张牌秒我!  大哥，我错了。[DONE] 我没错我骗你的，略略略";


        Thread producer = new Thread(() -> {
            for (String word : answer.split("")) {
                if (word.equals("[")) {
                    objectMany.tryEmitComplete();
                    break;
                } else {
                    Sinks.EmitResult emitResult = objectMany.tryEmitNext(word);
                    System.out.println(emitResult);
                    if (emitResult.isFailure()) {
                        break;
                    }
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        producer.start();

        Thread.sleep(500);

//        objectMany.tryEmitComplete(); // 正常停止

        objectMany.tryEmitError(new RuntimeException("数据出错了！"));

        Thread.sleep(5000);

    }
}

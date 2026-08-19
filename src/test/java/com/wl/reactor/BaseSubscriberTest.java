package com.wl.reactor;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.lang.NonNull;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.time.Duration;

/**
 * 自定义Subscriber，手动控制获取和输出
 */
@SpringBootTest
public class BaseSubscriberTest {


    /**
     * 基础自定义订阅
     *
     * @throws InterruptedException
     */
    @Test
    void myBaseSubscriberTest() throws InterruptedException {

        Flux<String> baseFlux = Flux.just("今天", "天气", "不错", "[STOP]", "这句", "不该", "输出");
        baseFlux.subscribe(new BaseSubscriber<String>() {

            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                System.out.println("订阅开始了，订阅信息是： " + subscription);
                // 订阅后，先获取一条
                request(1);
            }

            @Override
            protected void hookOnNext(String item) {

                // 遇到停止标识，则进行取消订阅
                if ("[STOP]".equals(item)) {
                    cancel();
                    return;
                }
                System.out.println("获取的信息为： " + item);
                // 然后拿下一条
                request(1);
            }

            @Override
            protected void hookOnError(Throwable throwable) {
                System.out.println("流信息出错啦，错误为： " + throwable.getMessage());
            }

            @Override
            protected void hookOnComplete() {
                System.out.println("流信息以全部获取完成");
            }

            @Override
            protected void hookOnCancel() {
                System.out.println(">> ⭐ hookOnCancel： 已取消， 可在此清理资源");
            }
        });
    }

    /**
     * 带有资源的订阅
     *
     * @throws InterruptedException
     */
    @Test
    void resourceTest() throws InterruptedException {
        // 模拟一个文件路径（请替换为你真实的文件路径）
        // String filePath = "data.txt";

        // 为了演示，这里用一段带换行的字符串模拟文件内容
        String mockFileContent = "第一行数据\n第二行数据\n[STOP]\n第四行数据（不该输出）";

        // ==========================================
        // 1. 发布者端：使用 Flux.using 负责读取文件和释放资源
        // ==========================================
        Flux<String> fileFlux = Flux.using(
                        // 1. 打开资源 (如 new BufferedReader(new FileReader(filePath)))
                        () -> new BufferedReader(new StringReader(mockFileContent)),

                        // 2. 使用资源生成数据流 (逐行读取)
                        reader -> Flux.<String>generate(sink -> {
                            try {
                                String line = reader.readLine();
                                if (line != null) {
                                    sink.next(line);
                                } else {
                                    sink.complete();
                                }
                            } catch (IOException e) {
                                sink.error(e);
                            }
                        }),

                        // 3. 清理资源 (无论发生什么，这里一定会执行！)
                        reader -> {
                            try {
                                System.out.println(">> 🔒 发布者清理：正在关闭 BufferedReader 资源...");
                                reader.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                )
                // doFinally 写在操作符链条上，而不是 Subscriber 里
                .doFinally(signalType -> System.out.println(">> 🏁 Flux 层面 doFinally：流已彻底终结，原因=" + signalType)
                );


        // ==========================================
        // 2. 消费者端：使用你的 BaseSubscriber 按需拉取
        // ==========================================
        fileFlux.subscribe(new BaseSubscriber<String>() {

            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                System.out.println("1. 订阅开始了");
                request(1); // 请求第一行
            }

            @Override
            protected void hookOnNext(String item) {
                if ("[STOP]".equals(item)) {
                    System.out.println("2. 遇到 [STOP] 标识，准备主动 cancel()...");
                    cancel(); // 触发上游断开连接
                    return;
                }

                System.out.println("   获取的文件内容： " + item);
                request(1); // 继续请求下一行
            }

            @Override
            protected void hookOnError(Throwable throwable) {
                System.out.println("流信息出错啦： " + throwable.getMessage());
            }

            @Override
            protected void hookOnComplete() {
                System.out.println("文件全部读取完成");
            }

            @Override
            protected void hookOnCancel() {
                // 这里只做“消费者”自己的状态清理！不要在这里关文件！
                System.out.println(">> 🛑 消费者清理 (hookOnCancel)：订阅已取消，正在清除客户端本地缓存/状态...");
            }
        });
    }

    /**
     * 模拟消费耗时操作，处理完一条要一条
     *
     * @throws InterruptedException
     */
    @Test
    void requestOneByOneTest() throws InterruptedException {

        Flux.interval(Duration.ofMillis(100))
                .publishOn(Schedulers.boundedElastic(),2)
                .subscribe(new BaseSubscriber<Long>() {

                    @Override
                    protected void hookOnNext(Long value) {
                        System.out.println("处理中: " + value);
                        slowWork(); // 模拟处理这条要花点时间
                        System.out.println(" 处理完 " + value + "， 再请求下一条");
                        request(1);
                    }

                    private void slowWork() {
                        try {
                            Thread.sleep(300);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    protected void hookOnSubscribe(Subscription subscription) {
                        System.out.println("开始订阅了！");
                        // 只取一条用于处理
                        request(1);
                    }

                    @Override
                    protected void hookOnComplete() {

                        System.out.println("hookOnComplete");
                    }

                    @Override
                    protected void hookOnCancel() {

                        System.out.println("cancel");
                    }

                    @Override
                    protected void hookOnError(@NonNull Throwable throwable) {
                        System.err.println("💥 流被炸毁了，原因: " + throwable.getMessage());
                    }
                });
        // 核心修复点：不让主线程跑路
        System.out.println("主线程开始等待...");
        Thread.sleep(5000);
        System.out.println("程序结束！");
    }
}


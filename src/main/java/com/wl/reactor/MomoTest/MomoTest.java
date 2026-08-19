package com.wl.reactor.MomoTest;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class MomoTest {


    public static void main(String[] args) throws InterruptedException {
        // 使用 AtomicReference 保证多线程并发环境下的可见性和安全性
        AtomicReference<Disposable> disposableRef = new AtomicReference<>();

        Thread r1 = new Thread(() -> {
            Flux<Integer> just = Flux.range(0, 2000);

            // 核心修改：使用 subscribeOn 将数据流的处理放到 Reactor 的弹性线程池中
            // 这样 .subscribe() 就会变成异步非阻塞，立刻返回 Disposable 对象
            Disposable d = just
                    .subscribeOn(Schedulers.boundedElastic())
                    .subscribe(x -> {
                        try {
                            TimeUnit.SECONDS.sleep(1);
                        } catch (InterruptedException e) {
                            // 当外部调用 dispose() 时，睡眠中的线程会被中断
                            System.out.println("流被取消，线程被中断...");
                        }
                        System.out.println("接收到数据: " + x);
                    });

            // 此时赋值瞬间完成，不需要等待 2000 秒
            disposableRef.set(d);
        });

        System.out.println("1111111111111111");

        Thread r2 = new Thread(() -> {
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            Disposable d = disposableRef.get();
            if (d != null) {
                System.out.println("5秒时间到，准备取消订阅...");
                d.dispose(); // 执行取消
            }
        });

        r1.start();
        r2.start();

        System.out.println("22222222222222222");

        // 稍微等一下让 r1 完成 subscribe 的非阻塞返回并赋值
        TimeUnit.MILLISECONDS.sleep(200);

        Disposable d = disposableRef.get();
        System.out.println("此时是否已取消: " + (d != null && d.isDisposed()));

        // 防止主线程立刻退出导致后台 Daemon 线程也跟着退出
        r1.join();
        r2.join();
        // 额外睡眠确保能看到最后的中断打印
        TimeUnit.SECONDS.sleep(1);
    }
}


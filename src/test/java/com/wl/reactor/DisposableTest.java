package com.wl.reactor;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;

import java.time.Duration;

@SpringBootTest
public class DisposableTest {

    /**
     * disposable取消流订阅
     *
     * @throws InterruptedException
     */
    @Test
    public void disposableTest() throws InterruptedException {

        Disposable disposable = Flux.interval(Duration.ofMillis(100))
                .map(i -> "数字是： " + i)
                .subscribe(v -> System.out.println(v),
                        onErr -> System.out.println("数据出错率，错误为： " + onErr.getMessage()),
                        () -> System.out.println("已完成所有输出"));

        // 主线程休眠，便于观察输出
        Thread.sleep(2000);

        // 取消订阅
        disposable.dispose();
        System.out.println("已调用 dispose()， isDisposed = " + disposable.isDisposed());

        // 休眠，便于观察后续是否有输出
        Thread.sleep(2000);
        System.out.println("结束。 可以看到 dispose 之后不再有新数据。 ");
    }

    /**
     * 管理多个流，对多个流取消订阅
     * @throws InterruptedException
     */
    @Test
    public void disposableTest2() throws InterruptedException {
        Disposable.Composite group = Disposables.composite();

        // 添加第一个流
        group.add(Flux.interval(Duration.ofMillis(100)).subscribe(v -> System.out.println("第一个流数据： " + v)));


        // 添加第二个流
        group.add(Flux.interval(Duration.ofMillis(100)).subscribe(v -> System.out.println("第二个流数据： " + v)));

        // 主线程休眠，便于观察输出
        Thread.sleep(2000);

        // 取消订阅
        group.dispose();
        System.out.println("已调用 dispose()， isDisposed = " + group.isDisposed());

        // 休眠，便于观察后续是否有输出
        Thread.sleep(2000);
        System.out.println("结束。 可以看到 dispose 之后不再有新数据。 ");
    }

}

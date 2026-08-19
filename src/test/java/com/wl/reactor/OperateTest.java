package com.wl.reactor;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

@SpringBootTest
public class OperateTest {

    /**
     * 按照顺序进行操作 同步的一对一变换
     */
    @Test
    void mapTest() {
        Flux.range(1, 10).map(v -> v * v).subscribe(System.out::println);
    }


    /**
     * 异步的一对多展开
     *
     * @throws InterruptedException
     */
    @Test
    void flatMapTest() throws InterruptedException {
        long start = System.currentTimeMillis();
        Flux.just("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")
                .flatMap(letter ->
                        Mono.just(letter.toUpperCase()).delayElement(Duration.ofMillis((long) (Math.random() * 100))))// 这里表名不是按顺序的
                .subscribe(System.out::println);
        System.out.println("运行时间为： " +( System.currentTimeMillis() - start )); //这里可以表名 Flux异步执行
        Thread.sleep(5000);
    }

    /**
     * 保证顺序的flatmap
     *
     * @throws InterruptedException
     */
    @Test
    void concatMapTest() throws InterruptedException {
        long start = System.currentTimeMillis();
        Flux.just("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")
                .concatMap(letter ->
                        Mono.just(letter.toUpperCase()).delayElement(Duration.ofMillis((long) (Math.random() * 100))))// 这里时间是随机的，但是输出结果是按顺序的
                .subscribe(System.out::println);
        System.out.println("运行时间为： " +( System.currentTimeMillis() - start )); //这里可以表名 Flux异步执行
        Thread.sleep(5000);
    }

    @Test
    void operateTest() {
        Flux<Integer> just = Flux.just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        just.filter(v -> v >= 6).subscribe(System.out::println); // 6,7,8,9,10

        just.take(3).subscribe(System.out::println); // 1,2,3

        just.takeLast(2).subscribe(System.out::println); // 9,10

        just.skip(8).subscribe(System.out::println); // 9,10


        just.elementAt(1).subscribe(System.out::println); // 2

        System.out.println();
        System.out.println();
        System.out.println();

        Flux<Integer> just1 = Flux.just(1, 2, 2, 3, 3, 5, 6, 7, 1, 3, 4, 5, 6, 7, 8, 9, 10);
        just1.distinct().subscribe(System.out::println);
    }

}

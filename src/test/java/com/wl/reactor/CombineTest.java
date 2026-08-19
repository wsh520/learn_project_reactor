package com.wl.reactor;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Flux;

import java.time.Duration;


@SpringBootTest
public class CombineTest {

    /**
     * 顺序 流合并
     */
    @Test
    void concatTest() {
        Flux<String> just1 = Flux.just("a", "b", "c");
        Flux<String> just2 = Flux.just("d", "e", "f");

        Flux.concat(just1, just2).map(String::toUpperCase).subscribe(System.out::println);
    }

    /**
     * 异步 谁先来先处理谁 无序
     *
     * @throws InterruptedException
     */
    @Test
    void mergeTest() throws InterruptedException {
        Flux<String> takeA = Flux.interval(Duration.ofMillis(100)).take(10).map(v -> "takeA" + v);
        Flux<String> takeB = Flux.interval(Duration.ofMillis(120)).take(10).map(v -> "takeB" + v);
        Flux.merge(takeA, takeB).subscribe(System.out::println);
        Thread.sleep(5000);
    }

    @Test
    void zipTest() {
        Flux<String> names = Flux.just("张三", "李四");
        Flux<Integer> ages = Flux.just(18, 28);
//        Flux.zip(names, ages).subscribe(System.out::println); // [张三,18] [李四,28]
//        Flux.zip(names, ages,(name,age) ->  name + ":" + age).subscribe(System.out::println); // 张三:18 李四:28
        Flux.zip(names, ages,(name,age) ->  {
            // 1. 这里可以写任意多行的复杂逻辑
            String status;
            if (age < 18) {
                status = "未成年";
            } else if (age < 60) {
                status = "成年打工人";
            } else {
                status = "退休大爷";
            }

            // 2. 也可以做数据清洗，比如给名字加前缀
            String cleanName = "VIP-" + name;

            // 3. 最后返回一个复杂的实体对象
            return status + cleanName;
        }).subscribe(System.out::println);
    }
}

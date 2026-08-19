package com.wl.reactor;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SpringBootTest
public class EmptyTest {

    /**
     * 空流给默认值
     */
    @Test
    public void defaultTest() {
        Mono.empty().defaultIfEmpty("这里是空的").subscribe(System.out::println);    // 这里是空的
    }

    /**
     * 空流切换到指定流
     */
    @Test
    void emptySwitchTest() {
        Mono.empty().switchIfEmpty(Mono.just("空流切换到这里了")).subscribe(System.out::println);
    }

    /**
     * 判断是不是空流 和是否包含某个元素
     */
    @Test
    void hasElementTest() {
        Mono.empty().hasElement().subscribe(System.out::println);

        Flux.just("a", "b", "c").hasElement("a").subscribe(System.out::println);
    }
}

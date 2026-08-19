package com.wl.reactor;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SpringBootTest
public class ErrorOperateTest {

    @Test
    void errorOperateTest() {
        // 制造一个中途出错的流：1, 2, 然后抛异常
        Flux<Integer> failing = Flux.just(1, 2)
                .concatWith(Flux.error(new RuntimeException("炸了")));
        // onErrorReturn：出错时返回一个兜底默认值，然后正常完成
        failing.onErrorReturn(-1)
                .subscribe(v -> System.out.println("return: " + v)); // 1,2,-1

        // onErrorResume：出错时切换到"另一个备用流"
        failing.onErrorResume(err -> Flux.just(100, 200))
                .subscribe(v -> System.out.println("resume: " + v)); // 1,2,100,200

        // onErrorMap：把异常转换成另一种异常（不吞掉，只是换类型）
        failing.onErrorMap(err -> new IllegalStateException("包装后: " +
                        err.getMessage()))
                .subscribe(
                        v -> System.out.println("map: " + v),
                        e -> System.out.println("map 捕获: " + e.getMessage()));

        // retry：出错时"重新订阅"整个流，最多重试 N 次
        int[] attempt = {0};
        Mono.fromCallable(() -> {
                    attempt[0]++;
                    if (attempt[0] < 3) throw new RuntimeException("第" + attempt[0] +
                            "次失败");
                    return "第" + attempt[0] + "次成功";
                })
                .retry(3)
                .subscribe(v -> System.out.println("retry: " + v)); // 第3次成功
    }
}

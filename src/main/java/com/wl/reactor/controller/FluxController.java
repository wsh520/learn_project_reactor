package com.wl.reactor.controller;

import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

@RestController
@RequestMapping("/flux")
public class FluxController {


    /**
     * 单个值测试
     *
     * @return 单值 直接是值
     */
    @GetMapping("/mono")
    public Mono<String> mono() {

        return Mono.just("this is test mono");
    }

    /**
     * 多值测试
     *
     * @return 多值 直接是值
     */
    @GetMapping("/range")
    public Flux<String> range() {

        return Flux.range(1, 20).map(String::valueOf).delayElements(Duration.ofSeconds(1));
    }

    /**
     * SSE
     *
     * @return data: value
     */
    @GetMapping(value = "/sse", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> sse() {


        return Flux.range(1, 20).map(String::valueOf).delayElements(Duration.ofSeconds(1));
    }

    /**
     * id:3
     * event:message
     * data:第 3 条
     * <p>
     * 可以多字段返回
     *
     * @return
     */
    @GetMapping("/sse/events")
    public Flux<ServerSentEvent<String>> events() {
        return Flux.interval(Duration.ofSeconds(1))
                .map(seq -> ServerSentEvent.<String>builder()
                        .id(String.valueOf(seq)) // 事件 id，前端可用于断线续传
                        .event("message") // 事件类型
                        .data("第 " + seq + " 条") // 数据体
                        .build());
    }

    @GetMapping(value = "/agent/chat", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> chat(@RequestParam String q) {
        // 模拟大模型：把回答逐字流式输出（真实场景这里是模型 SDK 的流式结果）
        String answer = "你问的是【 " + q + "】 。 这是我的流式回答， 逐字输出中……";
        return Flux.fromArray(answer.split("")) // 拆成一个个字
                .delayElements(Duration.ofMillis(150)) // ⭐ 打字机限速
                .take(Duration.ofSeconds(30)) // ⭐ 最多输出 30 秒，超时优 雅结束
                .doOnCancel(() -> System.out.println("⭐ 客户端断开/主动停止， 取消生 成"))
                .doFinally(sig -> System.out.println("⭐ 流终结(" + sig + ")， 释放资源"));
    }
}

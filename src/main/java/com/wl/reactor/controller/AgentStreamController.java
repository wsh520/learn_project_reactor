package com.wl.reactor.controller;

import com.wl.reactor.AgentSession;
import com.wl.reactor.AgentSessionManager;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/agent")
public class AgentStreamController {
    private final AgentSessionManager manager = new AgentSessionManager();

    /**
     * 建立 SSE 流并开始生成。前端：new EventSource('/agent/stream?userId=u1&q=你好')
     */
    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> stream(@RequestParam String userId, @RequestParam String q) {
        String answer = "针对【 " + q + "】 的流式回答： 这是逐字输出的内容， 可随时暂停或停止。 ";
        AgentSession session = manager.createSession(userId, answer);
        Flux<String> flux = session.getFlux(); // 已内置限速/超时/清理
        session.start(); // 启动后台生成
        return flux; // 客户端断开时框架自动 cancel → 触发 doFinally 清理
    }

    @PostMapping("/pause")
    public void pause(@RequestParam String userId) {
        manager.pause(userId);
    }

    @PostMapping("/resume")
    public void resume(@RequestParam String userId) {
        manager.resume(userId);
    }

    @PostMapping("/stop")
    public void stop(@RequestParam String userId) {
        manager.stop(userId);
    }
}

package com.wl.reactor;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 单个用户的 Agent 输出会话：封装一个可控的 token 流。
 */
public class AgentSession {
    enum State {IDLE, RUNNING, PAUSED, STOPPED}

    private final String userId;
    private final String fullAnswer; // 模拟模型将要输出的完整回答
    private final Sinks.Many<String> sink; // “话筒”：热流数据源
    private final AtomicReference<State> state = new AtomicReference<>(State.IDLE);
    private volatile Thread producer; // 后台生成线程
    // 可配置项
    private final Duration ratePerToken; // 每个 token 的最小输出间隔（限速）
    private final Duration totalTimeout; // 总时长上限
    private final Duration idleTimeout; // 单 token 空闲超时

    public AgentSession(String userId,
                        String fullAnswer,
                        Duration ratePerToken,
                        Duration totalTimeout,
                        Duration idleTimeout) {
        this.userId = userId;
        this.fullAnswer = fullAnswer;
        this.ratePerToken = ratePerToken;
        this.totalTimeout = totalTimeout;
        this.idleTimeout = idleTimeout;
        // 单播 + 缓冲：一对一给这个用户，订阅前的数据先缓存不丢
        this.sink = Sinks.many().unicast().onBackpressureBuffer();
    }

    /**
     * 提供给前端订阅的流：叠加限速、总超时、空闲超时、清理
     */
    public Flux<String> getFlux() {
        return sink.asFlux()
                .delayElements(ratePerToken) // 第五章：限速输出
                .timeout(idleTimeout) // 第四章：单 token空闲超时
                .take(totalTimeout) // 第四章：总时长上限
                .doOnCancel(() -> log("流被取消（ 用户断开/主动停止） "))
                .doFinally(sig -> { // 第八章：可靠清理
                    state.set(State.STOPPED);
                    stopProducer();
                    log("流终结(" + sig + ")， 已清理资源");
                });
    }

    /**
     * 开始生成：启动后台线程逐 token emit
     */
    public synchronized void start() {
        if (!state.compareAndSet(State.IDLE, State.RUNNING)) {
            log("已经开始过， 忽略 start");
            return;
        }
        producer = new Thread(() -> {
            for (int i = 0; i < fullAnswer.length(); i++) {
                // 暂停：自旋等待恢复（真实项目可用 wait/notify 或信号量，此处简化）
                while (state.get() == State.PAUSED) {
                    sleep(20);
                }
                if (state.get() == State.STOPPED) {
                    log("检测到停止， 生产线程退出");
                    return;
                }
                String token = String.valueOf(fullAnswer.charAt(i));
                Sinks.EmitResult r = sink.tryEmitNext(token); // 第七章：推一个token
                if (r.isFailure()) { // 下游已取消等
                    log("emit 失败(" + r + ")， 生产线程退出");
                    return;
                }
                sleep(30); // 模拟模型生成耗时（真实间隔，另有 delayElements 做展示限速）
            }
            sink.tryEmitComplete(); // 第七章：正常生成完毕
            log("生成完成");
        }, "agent-producer-" + userId);
        producer.setDaemon(true);
        producer.start();
        log("开始生成");
    }

    /**
     * 暂停：生产线程停止 emit，流挂起
     */
    public void pause() {
        if (state.compareAndSet(State.RUNNING, State.PAUSED)) log("已暂停");
    }

    /**
     * 继续：恢复 emit
     */
    public void resume() {
        if (state.compareAndSet(State.PAUSED, State.RUNNING)) log("已继续");
    }

    /**
     * 停止：优雅结束流并终止生产线程
     */
    public void stop() {
        state.set(State.STOPPED);
        sink.tryEmitComplete(); // 优雅结束（下游收到 onComplete）
        stopProducer();
        log("已停止");
    }

    private void stopProducer() {
        Thread p = producer;
        if (p != null) p.interrupt();
    }

    private void log(String msg) {
        System.out.println("[" + userId + "] " + msg);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ============ 演示 main ============
    public static void main(String[] args) throws InterruptedException {
        AgentSession session = new AgentSession(
                "user-1",
                "你好， 我是AI助手， 这是一段用于演示开始暂停继续停止的流式回答内容。 ",
                Duration.ofMillis(60), // 展示限速：每 60ms 一个字
                Duration.ofSeconds(30), // 总时长上限 30 秒
                Duration.ofSeconds(5)); // 单 token 空闲 5 秒算超时

        // 前端订阅（放到弹性线程，避免阻塞 main）
        session.getFlux()
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                        token -> System.out.print(token),
                        err -> System.out.println("\n[错误] " + err),
                        () -> System.out.println("\n[前端: 输出结束]"));
        session.start();
        Thread.sleep(800);
        System.out.println("\n>>> 主动暂停");
        session.pause();
        Thread.sleep(1500); // 这 1.5 秒内没有新字输出
        System.out.println("\n>>> 主动继续");
        session.resume();
        Thread.sleep(800);
        System.out.println("\n>>> 主动停止");
        session.stop(); // 用户点“停止生成”
        Thread.sleep(500);
    }
}
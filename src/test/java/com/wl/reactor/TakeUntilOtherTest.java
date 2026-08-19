package com.wl.reactor;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

public class TakeUntilOtherTest {
    public static void main(String[] args) throws InterruptedException {

        // 1. 模拟外部数据库的全局状态（初始为 RUNNING）
        AtomicReference<String> dbStatus = new AtomicReference<>("RUNNING");

        // 2. 模拟外部干预：用户在 2 秒后点击了网页上的【暂停】按钮
        new Thread(() -> {
            try {
                Thread.sleep(2000); // 运行两秒后
                System.out.println("\n🛑 [外部干预] 用户点击【暂停】，将数据库状态修改为 SUSPENDED！\n");
                dbStatus.set("SUSPENDED");
            } catch (InterruptedException e) {}
        }).start();

        // ====================================================================

        // 3. 【主管道】：模拟 LangChain4j 的底层大模型流 (每 300ms 吐一个字)
        Flux<String> mockLlmStream = Flux.interval(Duration.ofMillis(300))
                .map(i -> "Token_" + i)
                // ⭐ 这里是核心：这就相当于 LangChain4j 底层注册的清理动作！
                // 当它收到 takeUntilOther 传来的 cancel 信号时，就会执行这里的代码
                .doOnCancel(() -> System.out.println("🔪 [大模型底层] 收到上层的 Cancel 信号！正在暴力掐断 TCP 连接，停止计费！"))
                .doFinally(signal -> System.out.println("📦 [大模型底层] 流生命周期结束，最终信号为: " + signal));

        // 4. 【副管道 / 起爆器】：模拟状态轮询流 (每 500ms 查一次数据库)
        Mono<String> pollingSignal = Flux.interval(Duration.ofMillis(500))
                .map(i -> dbStatus.get())
                .filter(status -> "SUSPENDED".equals(status)) // 狙击镜：只抓 SUSPENDED
                .next() // 扳机：抓到第一个就发射，并掐断自己的轮询
                .doOnNext(s -> System.out.println("💥 [状态轮询流] 发现 SUSPENDED！起爆器触发，向 takeUntilOther 输送截断子弹！"));

        // ====================================================================

        System.out.println("🚀 引擎启动：开始执行大模型工作流...");

        // 5. 【枢纽】：用 takeUntilOther 将主副管道物理绑定！
        mockLlmStream
                .takeUntilOther(pollingSignal)
                // 模拟我们之前的聚合操作
                .subscribe(
                        token -> System.out.println("    接收到文字: " + token),
                        err -> System.err.println("发生错误: " + err),
                        () -> System.out.println("✅ [业务应用层] takeUntilOther 强行结算！收到 onComplete，保存已生成的半截文本，退出隔离节点。")
                );

        // 主线程睡眠 4 秒，防止程序过早退出，以便观察完整的异步日志
        Thread.sleep(4000);
        System.out.println("🏁 测试案例执行结束。");
    }
}
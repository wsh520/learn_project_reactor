package com.wl.reactor;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AgentSessionManager {
    private final Map<String, AgentSession> sessions = new ConcurrentHashMap<>();

    /**
     * 为某用户创建并启动一个会话；若已存在旧会话，先停掉
     */
    public AgentSession createSession(String userId, String answer) {

        // 幂等：同一用户重复开启，先清理旧的
        AgentSession old = sessions.remove(userId);
        if (old != null) old.stop();
        AgentSession session = new AgentSession(
                userId, answer,
                Duration.ofMillis(60),
                Duration.ofSeconds(30),
                Duration.ofSeconds(5));
        sessions.put(userId, session);
        return session;
    }

    public void pause(String userId) {
        withSession(userId, AgentSession::pause);
    }

    public void resume(String userId) {
        withSession(userId, AgentSession::resume);
    }

    public void stop(String userId) {
        AgentSession s = sessions.remove(userId); // 停止即移除，释放引用
        if (s != null) s.stop();
    }

    private void withSession(String userId, java.util.function.Consumer<AgentSession> action) {
        AgentSession s = sessions.get(userId);
        if (s != null) action.accept(s);
    }
}

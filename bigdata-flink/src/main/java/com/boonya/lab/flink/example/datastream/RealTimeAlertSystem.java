package com.boonya.lab.flink.example.datastream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 实时登录失败告警 —— 同一用户 5 分钟内登录失败超过 3 次即触发告警。
 * 核心：KeyedProcessFunction + ValueState + Event-Time 定时器。
 */
public class RealTimeAlertSystem {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 模拟登录事件流
        DataStream<LoginEvent> events = env.socketTextStream("localhost", 9999)
                .map(line -> {
                    String[] f = line.split(",");
                    return new LoginEvent(f[0], Boolean.parseBoolean(f[1]), f[2], Long.parseLong(f[3]));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((event, ts) -> event.timestamp));

        events.keyBy(e -> e.userId)
                .process(new LoginFailDetector(3, 300_000L))
                .print();

        env.execute("Real-time Login Alert Job");
    }

    // --- Model ---

    public static class LoginEvent {
        public String userId;
        public boolean success;
        public String ip;
        public long timestamp;

        public LoginEvent() {}

        public LoginEvent(String userId, boolean success, String ip, long timestamp) {
            this.userId = userId;
            this.success = success;
            this.ip = ip;
            this.timestamp = timestamp;
        }
    }

    public static class Alert {
        public String userId;
        public String alertType;
        public String message;
        public long timestamp;

        public Alert() {}

        public Alert(String userId, String alertType, String message, long timestamp) {
            this.userId = userId;
            this.alertType = alertType;
            this.message = message;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return String.format("ALERT[%s] userId=%s, %s", alertType, userId, message);
        }
    }

    // --- KeyedProcessFunction ---

    /** 统计登录失败次数并在窗口内超过阈值时告警。 */
    public static class LoginFailDetector extends KeyedProcessFunction<String, LoginEvent, Alert> {
        private final int maxFailCount;
        private final long timeWindow;

        private transient ValueState<Integer> failCountState;
        private transient ValueState<Long> firstFailTimeState;

        public LoginFailDetector(int maxFailCount, long timeWindow) {
            this.maxFailCount = maxFailCount;
            this.timeWindow = timeWindow;
        }

        @Override
        public void open(Configuration parameters) {
            failCountState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("failCount", Integer.class));
            firstFailTimeState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("firstFailTime", Long.class));
        }

        @Override
        public void processElement(LoginEvent event, Context ctx, Collector<Alert> out) throws Exception {
            if (event.success) {
                failCountState.clear();
                firstFailTimeState.clear();
                return;
            }

            Integer count = failCountState.value();
            Long firstTime = firstFailTimeState.value();

            if (count == null) {
                count = 0;
                firstTime = event.timestamp;
                firstFailTimeState.update(firstTime);
                ctx.timerService().registerEventTimeTimer(firstTime + timeWindow);
            }

            count++;
            failCountState.update(count);

            if (count >= maxFailCount) {
                out.collect(new Alert(event.userId, "LOGIN_FAIL",
                        "User " + event.userId + " failed login " + count + " times within " + timeWindow / 1000 + "s",
                        event.timestamp));
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) {
            failCountState.clear();
            firstFailTimeState.clear();
        }
    }
}

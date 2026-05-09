package com.boonya.lab.flink.example.state;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 状态管理实战 —— 演示 ValueState、ListState、MapState 三种 Keyed State。
 * 实时构建用户画像：累计消费金额、最近 N 条操作、品类偏好。
 */
public class StateManagementExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<UserAction> actions = env.socketTextStream("localhost", 9999)
                .map(line -> {
                    String[] f = line.split(",");
                    return new UserAction(f[0], f[1], f[2], Double.parseDouble(f[3]), Long.parseLong(f[4]));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserAction>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((a, ts) -> a.timestamp));

        actions.keyBy(a -> a.userId)
                .process(new UserProfileFunction())
                .print();

        env.execute("State Management Example Job");
    }

    // --- Model ---

    public static class UserAction {
        public String userId;
        public String action;
        public String category;
        public double amount;
        public long timestamp;

        public UserAction() {}

        public UserAction(String userId, String action, String category, double amount, long timestamp) {
            this.userId = userId;
            this.action = action;
            this.category = category;
            this.amount = amount;
            this.timestamp = timestamp;
        }
    }

    public static class UserProfile {
        public String userId;
        public double totalAmount;
        public int recentActionCount;
        public String favoriteCategory;
        public long updateTime;

        public UserProfile() {}

        public UserProfile(String userId, double totalAmount, int recentActionCount,
                           String favoriteCategory, long updateTime) {
            this.userId = userId;
            this.totalAmount = totalAmount;
            this.recentActionCount = recentActionCount;
            this.favoriteCategory = favoriteCategory;
            this.updateTime = updateTime;
        }

        @Override
        public String toString() {
            return String.format("UserProfile{userId='%s', total=%.2f, recent=%d, favorite='%s'}",
                    userId, totalAmount, recentActionCount, favoriteCategory);
        }
    }

    // --- KeyedProcessFunction ---

    /** 维护用户画像的三种状态，每次用户行为事件触发画像更新。 */
    public static class UserProfileFunction extends KeyedProcessFunction<String, UserAction, UserProfile> {

        private ValueState<Double> totalAmountState;
        private ListState<UserAction> recentActionsState;
        private MapState<String, Integer> categoryCountState;

        @Override
        public void open(Configuration parameters) {
            totalAmountState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("totalAmount", Double.class));
            recentActionsState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("recentActions", UserAction.class));
            categoryCountState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("categoryCount", String.class, Integer.class));
        }

        @Override
        public void processElement(UserAction action, Context ctx, Collector<UserProfile> out) throws Exception {
            Double total = totalAmountState.value();
            if (total == null) total = 0.0;
            total += action.amount;
            totalAmountState.update(total);

            // 维护最近 10 条操作
            List<UserAction> recent = new ArrayList<>();
            for (UserAction a : recentActionsState.get()) recent.add(a);
            recent.add(action);
            while (recent.size() > 10) recent.remove(0);
            recentActionsState.update(recent);

            // 统计品类偏好
            Integer cnt = categoryCountState.get(action.category);
            categoryCountState.put(action.category, (cnt == null ? 0 : cnt) + 1);

            // 计算最喜爱品类
            String favorite = "unknown";
            int max = 0;
            for (Map.Entry<String, Integer> e : categoryCountState.entries()) {
                if (e.getValue() > max) {
                    max = e.getValue();
                    favorite = e.getKey();
                }
            }

            out.collect(new UserProfile(action.userId, total, recent.size(), favorite, action.timestamp));
        }
    }
}

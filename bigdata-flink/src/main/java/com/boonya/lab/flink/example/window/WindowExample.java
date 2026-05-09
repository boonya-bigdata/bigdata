package com.boonya.lab.flink.example.window;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 窗口机制详解 —— 演示 Tumbling、Sliding、Session 三种窗口的类型和用法。
 */
public class WindowExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<Transaction> txStream = env.socketTextStream("localhost", 9999)
                .map(line -> {
                    String[] f = line.split(",");
                    return new Transaction(f[0], f[1], f[2], Double.parseDouble(f[3]), f[4], Long.parseLong(f[5]));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((tx, ts) -> tx.timestamp));

        // 1. 滚动窗口（1 分钟）—— 每商户的交易统计
        txStream.keyBy(tx -> tx.merchantId)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new TransactionAggregator())
                .print("Tumbling-1min");

        // 2. 滑动窗口（5 分钟窗口，1 分钟滑动）—— 每商户
        txStream.keyBy(tx -> tx.merchantId)
                .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
                .aggregate(new TransactionAggregator())
                .print("Sliding-5min-1min");

        // 3. 会话窗口（30 分钟 gap）—— 按用户会话
        txStream.keyBy(tx -> tx.userId)
                .window(EventTimeSessionWindows.withGap(Time.minutes(30)))
                .process(new SessionProcessor())
                .print("Session-30min");

        env.execute("Window Example Job");
    }

    // --- Model ---

    public static class Transaction {
        public String transactionId;
        public String userId;
        public String merchantId;
        public double amount;
        public String status;
        public long timestamp;

        public Transaction() {}

        public Transaction(String transactionId, String userId, String merchantId,
                           double amount, String status, long timestamp) {
            this.transactionId = transactionId;
            this.userId = userId;
            this.merchantId = merchantId;
            this.amount = amount;
            this.status = status;
            this.timestamp = timestamp;
        }
    }

    public static class TransactionStatistics {
        public String merchantId;
        public long windowStart;
        public long windowEnd;
        public long totalCount;
        public long successCount;
        public long failedCount;
        public double totalAmount;
        public double avgAmount;
        public double maxAmount;
        public double minAmount;

        public TransactionStatistics() {}

        @Override
        public String toString() {
            return String.format("TxStats[merchant=%s, count=%d, success=%d, failed=%d, "
                            + "total=%.2f, avg=%.2f, max=%.2f, min=%.2f]",
                    merchantId, totalCount, successCount, failedCount,
                    totalAmount, avgAmount, maxAmount, minAmount);
        }
    }

    public static class TransactionAccumulator {
        public String merchantId;
        public long count;
        public long successCount;
        public long failedCount;
        public double totalAmount;
        public Double maxAmount;
        public Double minAmount;
    }

    // --- AggregateFunction ---

    public static class TransactionAggregator
            implements AggregateFunction<Transaction, TransactionAccumulator, TransactionStatistics> {

        @Override
        public TransactionAccumulator createAccumulator() {
            return new TransactionAccumulator();
        }

        @Override
        public TransactionAccumulator add(Transaction tx, TransactionAccumulator acc) {
            acc.merchantId = tx.merchantId;
            acc.count++;
            if ("SUCCESS".equals(tx.status)) acc.successCount++;
            else acc.failedCount++;
            acc.totalAmount += tx.amount;
            acc.maxAmount = acc.maxAmount == null ? tx.amount : Math.max(acc.maxAmount, tx.amount);
            acc.minAmount = acc.minAmount == null ? tx.amount : Math.min(acc.minAmount, tx.amount);
            return acc;
        }

        @Override
        public TransactionStatistics getResult(TransactionAccumulator acc) {
            TransactionStatistics s = new TransactionStatistics();
            s.merchantId = acc.merchantId;
            s.totalCount = acc.count;
            s.successCount = acc.successCount;
            s.failedCount = acc.failedCount;
            s.totalAmount = acc.totalAmount;
            s.avgAmount = acc.count > 0 ? acc.totalAmount / acc.count : 0;
            s.maxAmount = acc.maxAmount != null ? acc.maxAmount : 0;
            s.minAmount = acc.minAmount != null ? acc.minAmount : 0;
            return s;
        }

        @Override
        public TransactionAccumulator merge(TransactionAccumulator a, TransactionAccumulator b) {
            TransactionAccumulator merged = new TransactionAccumulator();
            merged.merchantId = a.merchantId;
            merged.count = a.count + b.count;
            merged.successCount = a.successCount + b.successCount;
            merged.failedCount = a.failedCount + b.failedCount;
            merged.totalAmount = a.totalAmount + b.totalAmount;
            merged.maxAmount = null;
            if (a.maxAmount != null) merged.maxAmount = a.maxAmount;
            if (b.maxAmount != null) {
                merged.maxAmount = merged.maxAmount == null ? b.maxAmount
                        : Math.max(merged.maxAmount, b.maxAmount);
            }
            merged.minAmount = null;
            if (a.minAmount != null) merged.minAmount = a.minAmount;
            if (b.minAmount != null) {
                merged.minAmount = merged.minAmount == null ? b.minAmount
                        : Math.min(merged.minAmount, b.minAmount);
            }
            return merged;
        }
    }

    // --- ProcessWindowFunction ---

    public static class SessionProcessor
            extends ProcessWindowFunction<Transaction, UserSession, String, TimeWindow> {

        @Override
        public void process(String userId, Context context, Iterable<Transaction> transactions,
                            Collector<UserSession> out) {
            UserSession session = new UserSession();
            session.userId = userId;
            session.windowStart = context.window().getStart();
            session.windowEnd = context.window().getEnd();
            for (Transaction tx : transactions) {
                session.txCount++;
                session.totalAmount += tx.amount;
            }
            out.collect(session);
        }
    }

    public static class UserSession {
        public String userId;
        public long windowStart;
        public long windowEnd;
        public long txCount;
        public double totalAmount;

        @Override
        public String toString() {
            return String.format("UserSession[userId=%s, txs=%d, total=%.2f, start=%d, end=%d]",
                    userId, txCount, totalAmount, windowStart, windowEnd);
        }
    }
}

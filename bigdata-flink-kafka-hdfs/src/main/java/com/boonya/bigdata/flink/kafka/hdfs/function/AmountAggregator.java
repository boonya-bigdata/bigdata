package com.boonya.bigdata.flink.kafka.hdfs.function;

import com.boonya.bigdata.flink.kafka.hdfs.model.UserEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 基础金额聚合器（无倾斜处理）
 * 按userId聚合，累计金额
 */
public class AmountAggregator extends KeyedProcessFunction<Integer, UserEvent, String> {

    private transient ValueState<Double> totalAmountState;

   // @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>("totalAmount", Double.class);
        totalAmountState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(UserEvent event, Context ctx, Collector<String> out) throws Exception {
        Double currentTotal = totalAmountState.value();
        if (currentTotal == null) {
            currentTotal = 0.0;
        }
        Double newTotal = currentTotal + event.amount();
        totalAmountState.update(newTotal);

        out.collect(String.format("用户[%d]累计金额: %.2f", event.userId(), newTotal));
    }
}
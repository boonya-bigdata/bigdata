package com.boonya.bigdata.flink.kafka.hdfs.function;

import com.boonya.bigdata.flink.kafka.hdfs.model.UserEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JSON解析器（处理异常数据）
 * 输入: JSON字符串
 * 输出: UserEvent Record
 */
public class JsonParserFunction extends ProcessFunction<String, UserEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(JsonParserFunction.class);

    // 侧输出流：异常数据
    public static final OutputTag<String> ERROR_TAG = new OutputTag<>("errors", TypeInformation.of(String.class));

    @Override
    public void processElement(String json, ProcessFunction<String, UserEvent>.Context ctx, Collector<UserEvent> out) {
        try {
            UserEvent event = UserEvent.fromJson(json);
            if (event.userId() != null && event.userId() > 0) {
                out.collect(event);
            } else {
                ctx.output(ERROR_TAG, "Invalid user_id: " + json);
                LOG.warn("无效的userId: {}", json);
            }
        } catch (Exception e) {
            ctx.output(ERROR_TAG, "Parse error: " + json + ", " + e.getMessage());
            LOG.error("JSON解析失败: {}", json, e);
        }
    }
}
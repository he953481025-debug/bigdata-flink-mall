package org.example;

import com.alibaba.fastjson.JSON;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.example.domain.OrderEvent;

import java.util.List;
import java.util.Map;

/**
 * @author hejincai
 * @since 2023/7/2 17:12
 **/
public class OrderPayMonitor {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        // 这里只读取本地文件测试
        DataStreamSource<String> dataStreamSource = executionEnvironment
                .readTextFile("file:///D:\\afanti\\bigdata-flink-mall\\common-base\\src\\main\\resources\\order-event.json");
        KeyedStream<OrderEvent, String> orderEventStringKeyedStream = dataStreamSource.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String s) throws Exception {
                return JSON.parseObject(s, OrderEvent.class);
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
            @Override
            public long extractTimestamp(OrderEvent orderEvent, long l) {
                return orderEvent.getEventTime();
            }
        })).keyBy(orderEvent -> orderEvent.getOrderId());
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("first").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent orderEvent) throws Exception {
                return "1".equals(orderEvent.getOrderStatus());
            }
        })
                .followedByAny("second").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent orderEvent) throws Exception {
                return "2".equals(orderEvent.getOrderStatus());
            }
        })
                .within(Time.minutes(15));
        orderEventStringKeyedStream.print();
        PatternStream<OrderEvent> patternedStream = CEP.pattern(orderEventStringKeyedStream, pattern);
        patternedStream.process(new PatternProcessFunction<OrderEvent, String>() {
            @Override
            public void processMatch(Map<String, List<OrderEvent>> match, Context ctx, Collector<String> out) throws Exception {
                List<OrderEvent> first = match.get("first");
                List<OrderEvent> second = match.get("second");
                if (CollectionUtils.isEmpty(first) || CollectionUtils.isEmpty(second)) {
                    return;
                }
                OrderEvent firstOrderEvent = first.get(0);
                OrderEvent secondOrderEvent = second.get(0);
                out.collect("用户id" + firstOrderEvent.getUserId() + "订单id" + firstOrderEvent.getOrderId() + "触发警报");
            }
        }).print();
        executionEnvironment.execute();

    }
}

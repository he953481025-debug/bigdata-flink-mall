package org.example;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.domain.ProductCount;
import org.example.domain.UserBehavior;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author hejincai
 * @since 2023/7/2 16:52
 */
public class PopularProductStatistics {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        // 这里只读取本地文件测试
        DataStreamSource<String> dataStreamSource = executionEnvironment
                .readTextFile("file:///D:\\afanti\\bigdata-flink-mall\\common-base\\src\\main\\resources\\userbehavior.json");
        SingleOutputStreamOperator<UserBehavior> userBehaviorStream = dataStreamSource.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String s) throws Exception {
                return JSON.parseObject(s, UserBehavior.class);
            }
        }).filter(userBehavior -> "pv".equals(userBehavior.getBehavior()))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
            @Override
            public long extractTimestamp(UserBehavior userBehavior, long l) {
                return userBehavior.getTimestamp();
            }
        }));
        userBehaviorStream
                .map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(UserBehavior userBehavior) throws Exception {
                        return new Tuple2<>(userBehavior.getItemId(), 1);
                    }
                })
                .windowAll(SlidingEventTimeWindows.of(Time.of(1, TimeUnit.HOURS), Time.of(5, TimeUnit.MINUTES)))
                .process(new ProcessAllWindowFunction<Tuple2<String, Integer>, List<ProductCount>, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<Tuple2<String, Integer>, List<ProductCount>, TimeWindow>.Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<List<ProductCount>> collector) throws Exception {
                        List<Tuple2<String, Integer>> list = new ArrayList<>();
                        iterable.forEach(list::add);
                        Map<String, IntSummaryStatistics> statisticsMap = list.stream().collect(Collectors.groupingBy(tuple -> tuple.f0, Collectors.summarizingInt(t -> t.f1)));
                        Set<Map.Entry<String, IntSummaryStatistics>> entrySet = statisticsMap.entrySet();
                        List<Tuple2<String, Integer>> list2 = new ArrayList<>(entrySet.size());
                        for (Map.Entry<String, IntSummaryStatistics> entry : entrySet) {
                            Tuple2<String, Integer> init = new Tuple2<>();
                            init.f0 = entry.getKey();
                            init.f1 = (int) entry.getValue().getCount();
                            list2.add(init);
                        }
                        List<ProductCount> result = new ArrayList<>(list2.size());
                        // 排序输出前N个商品
                        for (Tuple2<String, Integer> tuple2 : list2) {
                            ProductCount productCount = new ProductCount();
                            productCount.setItemId(tuple2.f0);
                            productCount.setCount(tuple2.f1);
                            result.add(productCount);
                        }
                        result.sort(Comparator.comparing(ProductCount::getCount).reversed());
                        collector.collect(result);
                    }
                }).print();
        executionEnvironment.execute();

    }
}

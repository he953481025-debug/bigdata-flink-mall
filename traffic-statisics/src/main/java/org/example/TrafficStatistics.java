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
import org.example.domain.UserBuriedLog;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**  
 * @author hejincai
 * @since 2023/7/2 16:52
 */
public class TrafficStatistics {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        // 这里只读取本地文件测试
        DataStreamSource<String> dataStreamSource = executionEnvironment
                .readTextFile("file:///D:\\afanti\\bigdata-flink-mall\\common-base\\src\\main\\resources\\user-buried-log.json");
        SingleOutputStreamOperator<UserBuriedLog> userBuriedLogStream = dataStreamSource.map(new MapFunction<String, UserBuriedLog>() {
            @Override
            public UserBuriedLog map(String s) throws Exception {
                return JSON.parseObject(s, UserBuriedLog.class);
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<UserBuriedLog>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<UserBuriedLog>() {
            @Override
            public long extractTimestamp(UserBuriedLog userBuriedLog, long l) {
                return userBuriedLog.getEventTime();
            }
        }));

        userBuriedLogStream
                .map(new MapFunction<UserBuriedLog, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(UserBuriedLog userBuriedLog) throws Exception {
                        return new Tuple2<>(userBuriedLog.getUrl(), 1);
                    }
                })
                .windowAll(SlidingEventTimeWindows.of(Time.of(10, TimeUnit.MINUTES), Time.of(5, TimeUnit.SECONDS)))
                .process(new ProcessAllWindowFunction<Tuple2<String, Integer>, List<Tuple2<String, Integer>>, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<Tuple2<String, Integer>, List<Tuple2<String, Integer>>, TimeWindow>.Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<List<Tuple2<String, Integer>>> collector) throws Exception {
                        List<Tuple2<String, Integer>> list = new ArrayList<>();
                        iterable.forEach(list::add);
                        Map<String, IntSummaryStatistics> statisticsMap = list.stream().collect(Collectors.groupingBy(tuple -> tuple.f0, Collectors.summarizingInt(t -> t.f1)));
                        Set<Map.Entry<String, IntSummaryStatistics>> entrySet = statisticsMap.entrySet();
                        List<Tuple2<String, Integer>> result = new ArrayList<>(entrySet.size());
                        for (Map.Entry<String, IntSummaryStatistics> entry : entrySet) {
                            Tuple2<String, Integer> init = new Tuple2<>();
                            init.f0 = entry.getKey();
                            init.f1 = (int) entry.getValue().getCount();
                            result.add(init);
                        }
                        result.sort((o1, o2) -> {
                            if (o2.f1 < o1.f1) {
                                return -1;
                            } else if (o2.f1.equals(o1.f1)) {
                                return 0;
                            }
                            return 1;
                        });
                        collector.collect(result);
                    }
                }).print();
        executionEnvironment.execute();

    }
}

package org.example;

import com.alibaba.fastjson.JSON;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.example.domain.UserBuriedLog;

import java.util.List;
import java.util.Map;

/**
 * @author hejincai
 * @since 2023/7/2 17:12
 **/
public class LoginMonitor {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        // 这里只读取本地文件测试
        DataStreamSource<String> dataStreamSource = executionEnvironment
                .readTextFile("file:///D:\\afanti\\bigdata-flink-mall\\common-base\\src\\main\\resources\\user-buried-log.json");
        KeyedStream<UserBuriedLog, String> userBuriedLogKeyedStream = dataStreamSource.map(new MapFunction<String, UserBuriedLog>() {
            @Override
            public UserBuriedLog map(String s) throws Exception {
                return JSON.parseObject(s, UserBuriedLog.class);
            }
        }).filter(userBuriedLog -> userBuriedLog.getUrl().equals("/login")).assignTimestampsAndWatermarks(WatermarkStrategy.<UserBuriedLog>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<UserBuriedLog>() {
            @Override
            public long extractTimestamp(UserBuriedLog userBuriedLog, long l) {
                return userBuriedLog.getEventTime();
            }
        })).keyBy(UserBuriedLog::getUserId);
        Pattern<UserBuriedLog, UserBuriedLog> pattern = Pattern.<UserBuriedLog>begin("first").where(new SimpleCondition<UserBuriedLog>() {
            @Override
            public boolean filter(UserBuriedLog userBuriedLog) throws Exception {
                return "/login".equals(userBuriedLog.getUrl());
            }
        }).followedByAny("second").where(new SimpleCondition<UserBuriedLog>() {
            @Override
            public boolean filter(UserBuriedLog userBuriedLog) throws Exception {
                return "/login".equals(userBuriedLog.getUrl());
            }
        }).within(Time.seconds(2));
        userBuriedLogKeyedStream.print();
        PatternStream<UserBuriedLog> patternedStream  = CEP.pattern(userBuriedLogKeyedStream, pattern);
        patternedStream.process(new PatternProcessFunction<UserBuriedLog, String>() {
            @Override
            public void processMatch(Map<String, List<UserBuriedLog>> match, Context ctx, Collector<String> out) throws Exception {
                List<UserBuriedLog> first = match.get("first");
                List<UserBuriedLog> second = match.get("second");
                if (CollectionUtils.isEmpty(first) || CollectionUtils.isEmpty(second)) {
                    return;
                }
                UserBuriedLog firstLog = first.get(0);
                UserBuriedLog secondLog = second.get(0);
                if (!firstLog.getIp().equals(secondLog.getIp())) {
                    out.collect("用户id" + firstLog.getUserId() + "触发警报");
                }
            }
        }).print();
        executionEnvironment.execute();
        
    }
}

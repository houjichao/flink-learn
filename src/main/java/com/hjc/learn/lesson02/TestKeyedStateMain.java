package com.hjc.learn.lesson02;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 需求：当接收到的相同 key 的元素个数等于 3个，就计算这些元素的 value 的平均值。
 * 计算keyed stream中每3个元素的 value 的平均值
 * @author houjichao
 */
public class TestKeyedStateMain {

    public static void main(String[] args) throws Exception{
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(12);
        //获取数据源
        DataStreamSource<Tuple2<Long, Long>> dataStreamSource =
                env.fromElements(
                        Tuple2.of(1L, 3L),
                        Tuple2.of(1L, 7L),
                        Tuple2.of(2L, 4L),
                        Tuple2.of(1L, 5L),
                        Tuple2.of(2L, 2L),
                        Tuple2.of(2L, 6L));
        /**
         * 1L, 3L
         * 1L, 7L
         * 1L, 5L
         *
         * 1L,5.0 double
         *
         *
         *
         * 2L, 4L
         * 2L, 2L
         * 2L, 6L
         *
         * 2L,4.0 double
         *
         *
         *
         */


        // 输出：
        //(1,5.0)
        //(2,4.0)
        dataStreamSource
                //分组
                .keyBy(tuple -> tuple.f0)
                .flatMap(new CountAverageWithValueState())
                .print();


        env.execute("TestStatefulApi");
    }

}

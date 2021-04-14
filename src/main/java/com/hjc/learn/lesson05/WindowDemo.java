package com.hjc.learn.lesson05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink中的窗口：
 * 窗口是将无限流切割为有限流的一种方式，他会将流数据分发到有限大小的桶（bucket)中进行分析，[)区间
 * window类型：
 * 1. 时间窗口
 *    滚动时间窗口(Tumbling Windows)
 *      将数据依据固定的窗口长度对数据进行切分
 *      时间对齐，窗口长度固定，没有重叠
 *    滑动时间窗口(Sliding Windows)
 *      滑动窗口由固定的时间长度和滑动间隔组成
 *      窗口长度固定，可以有重叠
 *    回话窗口(Session Windows)
 *      指定时间长度timeout的间隙，一段时间没有接收到新数据就生成新的窗口
 * 2. 计数窗口
 *    滚动计数窗口
 *    滑动计数窗口
 * @author houjichao
 */
public class WindowDemo {

    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //步骤二：数据的输入
        //socket的并行度只能是1
        //nc -lk
        DataStreamSource<String> dataStream = env.socketTextStream("127.0.0.1", 9999);
        //步骤三：数据的处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line,
                    Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] fields = line.split(",");
                for (String word : fields) {
                    //key,value
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }).keyBy(tuple -> tuple.f0) //按单词进行分组
                .sum(1);

        //步骤四：数据的输出
        result.print().setParallelism(1);

        //步骤五：启动程序
        env.execute("window-demo");
    }

}

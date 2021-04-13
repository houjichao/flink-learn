package com.hjc.learn.lesson01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *  单词计数
 *  
 *  hello,1
 *  hello,1
 * @author houjichao
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        //步骤一：初始化程序入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
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
                .sum(1); //求和  state
        //步骤四：数据的输出
        result.print().setParallelism(1);
        //步骤五：启动程序
        env.execute("WordCount");

    }
}

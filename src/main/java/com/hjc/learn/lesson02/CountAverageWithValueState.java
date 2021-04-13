package com.hjc.learn.lesson02;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * ValueState：每一个key保存一个值 value() 获取状态值 update() 更新状态值 clear() 清除状态
 *
 * @author houjichao
 */
public class CountAverageWithValueState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

    /**
     * 1，5 1，7 1，2 Tuple2<Long, Long> key , value key:long  count 记录，当前的key出现了多少次 3 value:long  sum 记录，当前key对应value的总的值
     * 14
     *
     * value/key=avg
     *
     * 注册state（初始化state）
     */
    private ValueState<Tuple2<Long, Long>> countAndSum;

    @Override
    public void open(Configuration parameters) throws Exception {
        //注册状态
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<>(
                        // 状态的名字
                        "average",
                        // 状态存储的数据类型
                        Types.TUPLE(Types.LONG, Types.LONG));
        countAndSum = getRuntimeContext().getState(descriptor);

    }

    /**
     * 每来一条数据，都会调用这个方法 key相同
     */
    @Override
    public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Double>> out) throws Exception {
        // 拿到当前key的状态值
        Tuple2<Long, Long> currentState = countAndSum.value();
        // 如果状态值还没有初始化，则初始化
        if (currentState == null) {
            currentState = Tuple2.of(0L, 0L);
        }
        // 更新状态中元素的个数： count
        currentState.f0 += 1;
        // 更新￿状态中的宗旨
        currentState.f1 += element.f1;
        // 更新状态
        countAndSum.update(currentState);

        // 判断，如果当前的 key 出现了 3 次，则需要计算平均值，并且输出
        if (currentState.f0 == 3) {
            double avg = (double) currentState.f1 / currentState.f0;
            //输出key和平均值
            out.collect(Tuple2.of(element.f0, avg));
            //清空状态值
            countAndSum.clear();
        }

    }
}

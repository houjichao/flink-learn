package com.hjc.learn.dau;


import com.hjc.learn.lesson05.DataPath;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * redis-server.exe redis.windows.conf
 *
 * redis-cli.exe -h 127.0.0.1 -p 6379
 *
 * keys *
 *
 * HGETALL count
 *
 * flushall
 *
 * 去重类型
 *
 * 统计UV
 *
 * @author houjichao
 */
public class UVByBloom {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // env.socketTextStream("192.168.123.102",9999)
        env.readTextFile(DataPath.USER_BEHAVIOR_PATH) //读取数据
                .map(new UVCount.ParseUserLog()) //解析日志
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .forGenerator((ctx) -> new PeriodicWatermarkGenerator())
                                .withTimestampAssigner((ctx) -> new TimeStampExtractor())) //指定watermark
                .filter(behavior -> behavior.getBehavior().equalsIgnoreCase("P")) //过滤数据
                .map(new getKeyMapFunction()) //数据转换
                .keyBy(tuple2 -> tuple2.f0) //按照key进行分组
                .window(TumblingEventTimeWindows.of(Time.hours(1))) //滚动的窗口是1小时（默认1小时运行一次）
                .trigger(new UVTrigger()) //每一条数据来了更一下结果
                .process(new UvCountWithBloom()) //进行UV的统计，借助布隆过滤器
                .print();

        env.execute("UVByBloom");
    }


    /**
     * 布隆过滤器
     */

    public static class Bloom implements Serializable {

        private long cap;

        //布隆过滤器的默认大小是32M
        //32 * 1024 * 1024 * 8
        //2^5  2^10   2^10 * 2^3
        public long getCap() {
            if (cap > 0) {
                return cap;
            } else {
                //1后面28个0
                return 1 << 28;
            }
        }

        public void setCap(long cap) {
            this.cap = cap;
        }

        public Bloom(long cap) {
            this.cap = cap;
        }


        /**
         * 自己的实现的 大家也可以自己实现，思路大家自己来就可以。 主要的目的就是，让不同的userID过来以后计算出来 的hash不一样就行。
         */

        public long hash(String value, int seed) {
            long result = 0L;
            for (int i = 0; i < value.length(); i++) {
                result += result * seed + value.charAt(i);
            }
            //他们之间进行&运算结果一定在位图之间
            return result & (cap - 1);//0后面28个1
        }
    }


    public static class UvCountWithBloom
            extends ProcessWindowFunction<Tuple2<String, Long>,
            UVCount.UvInfo, String, TimeWindow> {


        //初始化自己的布隆过滤器（32M）
        public Bloom bloom = new Bloom(1 << 28);
        //初始化了一个redis的工具类。
        RedisUtils redisUtils = new RedisUtils();

        @Override
        public void process(String s, Context context,
                Iterable<Tuple2<String, Long>> elements,
                Collector<UVCount.UvInfo> out) throws Exception {
            //自定义了一个key
            String storeKey = new Timestamp(context.window().getEnd()).toString();

            long count = 0L;

            //从redis里面获取数据
            if (RedisUtils.jedis.hget("count", storeKey) != null) {
                count = Long.parseLong(RedisUtils.jedis.hget("count", storeKey));
            }
            //获取当前的用户ID
            String userId = elements.iterator().next().f1.toString();

            //通过布隆狗率其计算 offset
            long offset = bloom.hash(userId, 77);
            //计算这个用户之前是否出现过
            Boolean isExist = RedisUtils.jedis.getbit(storeKey, offset);
            //如果没有出现
            if (!isExist) {
                //把状态改为true（1）
                RedisUtils.jedis.setbit(storeKey, offset, true);
                //让UV累加1
                RedisUtils.jedis.hset("count", storeKey, (count + 1) + "");
                //输出结果
                out.collect(new UVCount.UvInfo(new Timestamp(context.window().getEnd()).toString(), count + 1));

            } else {
                //如果出现过，UV不进行累加
                out.collect(new UVCount.UvInfo(new Timestamp(context.window().getEnd()).toString(), count));

            }

        }
    }

    public static class UVTrigger extends Trigger<Tuple2<String, Long>, TimeWindow> {

        @Override
        public TriggerResult onElement(Tuple2<String, Long> stringLongTuple2, long l, TimeWindow timeWindow,
                TriggerContext triggerContext) throws Exception {
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext)
                throws Exception {
            return null;
        }

        @Override
        public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext)
                throws Exception {
            return null;
        }

        @Override
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

        }
    }


    /**
     * 指定数据类型
     */

    public static class getKeyMapFunction
            implements MapFunction<UVCount.UserBehavior, Tuple2<String, Long>> {

        @Override
        public Tuple2<String, Long> map(UVCount.UserBehavior userBehavior) throws Exception {
            return Tuple2.of("key", userBehavior.getUserId());
        }
    }

    private static class PeriodicWatermarkGenerator implements WatermarkGenerator<UVCount.UserBehavior>, Serializable {

        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

        private long currentMaxEventTime = 0L;
        private long maxOutOfOrderness = 10L; // 最大允许的乱序时间 10 秒

        @Override
        public void onEvent(
                UVCount.UserBehavior event, long eventTimestamp, WatermarkOutput output) {
            long currentElementEventTime = event.getTimeStamp();
            currentMaxEventTime = Math.max(currentMaxEventTime, currentElementEventTime);

        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {

            output.emitWatermark(new Watermark((currentMaxEventTime - maxOutOfOrderness) * 1000));
        }
    }

    private static class TimeStampExtractor implements TimestampAssigner<UVCount.UserBehavior> {

        @Override
        public long extractTimestamp(UVCount.UserBehavior element, long recordTimestamp) {
            return element.getTimeStamp() * 1000;
        }
    }
}


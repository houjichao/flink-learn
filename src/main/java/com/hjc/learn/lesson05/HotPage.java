package com.hjc.learn.lesson05;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 每5S统计近10分钟的热门页面，求TOP N
 *
 * @author houjichao
 */
public class HotPage {

    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取数据
        env.readTextFile(DataPath.APACHE_LOG_PAH) //读取数据
                .map(new ParseLog()) //解析日志数据
                //添加水位，有些数据可能会迟到10s
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .forGenerator((ctx) -> new PeriodicWatermarkGenerator())
                                .withTimestampAssigner((ctx) -> new TimeStampExtractor())) //添加水位

                //按照URL分组
                .keyBy(ApacheLogEvent::getUrl)
                //实现滑动窗口
                // TumblingEventTimeWindows  滚动时间窗口
                // SlidingEventTimeWindows   滑动时间窗口

                /**
                 * EventTime 数据本身携带的时间，默认的时间属性；
                 * ProcessingTime 处理时间；
                 * IngestionTime 数据进入flink程序的时间；
                 *
                 * Tumbling windows（滚动窗口）
                 * 滚动窗口下窗口之间不重叠，且窗口长度是固定的。我们可以用
                 * TumblingEventTimeWindows和TumblingProcessingTimeWindow
                 * s创建一个基于Event Time或Processing Time的滚动时间窗口。
                 */

                /**
                 * Sliding windows（滑动窗口）
                 *
                 * 滑动窗口以一个步长（Slide）不断向前滑动，窗口的长度固定。使用时，我们要设置Slide和Size。
                 * Slide的大小决定了Flink以多大的频率来创建新的窗口，Slide较小，窗口的个数会很多。
                 * Slide小于窗口的Size时，相邻窗口会重叠，一个事件会被分配到多个窗口；Slide大于Size，有些事件可能被丢掉。
                 *
                 * SlidingEventTimeWindows of(Time size, Time slide)
                 */
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(5)))
                //sum  //自定义窗口输出的结果
                //每个URL在window里面出现的次数就计算出来
                //计算每个URL出现的次数
                .aggregate(new PageCountAgg(),new PageWindowResult())
                //按照窗口进行分组
                .keyBy(UrlView::getWindowEnd)
                //求TopN
                .process(new TopNHotPage(3))
                .print();

        env.execute("HotPage");
    }

    /**
     * 计算热门热面
     * K, I, O
     */
    public static class TopNHotPage extends KeyedProcessFunction<Long, UrlView,String> {

        private int topN = 0;

        public TopNHotPage(int topN) {
            this.topN = topN;
        }

        public int getTopN() {
            return topN;
        }

        public void setTopN(Integer topN) {
            this.topN = topN;
        }

        //key:url
        //value:count 出现的次数
        public MapState<String,Long> urlState;
        @Override
        public void open(Configuration parameters) throws Exception {
            // 注册状态
            MapStateDescriptor<String,Long> descriptor =
                    new MapStateDescriptor<String, Long>(
                            "average",  // 状态的名字
                            String.class, Long.class); // 状态存储的数据类型
            urlState = getRuntimeContext().getMapState(descriptor);

        }

        @Override
        public void processElement(UrlView urlView,
                Context context,
                Collector<String> out) throws Exception {

            urlState.put(urlView.getUrl(),urlView.getCount());

            context.timerService().registerEventTimeTimer(urlView.getWindowEnd() + 1);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            List<UrlView> urlViewArrayList = new ArrayList<UrlView>();

            List<String> allElementKey = Lists.newArrayList(urlState.keys());
            for(String url:allElementKey){
                urlViewArrayList.add(new UrlView(url, new Timestamp(timestamp - 1).getTime(),urlState.get(url).longValue()));
            }
            Collections.sort(urlViewArrayList);

            List<UrlView> topN = urlViewArrayList.subList(0,this.topN);

            for (UrlView urlView:topN){
                System.out.println(urlView);
            }
            System.out.println("======================");


        }
    }

    /**
     * IN, 输入的数据类型
     * OUT, 输出的数据类型
     * KEY, key
     * W <: Window window的类型
     *
     */
    public static class PageWindowResult
            implements WindowFunction<Long,UrlView, String, TimeWindow> {

        @Override
        public void apply(String key, TimeWindow timeWindow, Iterable<Long> iterable,
                Collector<UrlView> collector) throws Exception {
            collector.collect(
                    new UrlView(
                            key, //URL
                            timeWindow.getEnd(), //窗口的标识
                            iterable.iterator().next())); //次数
        }
    }


    /**
     * ApacheLogEvent, 输入
     * Long, 辅助变量，累加变量
     * Long 输出：URL出现的次数
     * 实现了一个sum的效果
     */
    public static class PageCountAgg implements AggregateFunction<ApacheLogEvent,Long,Long> {

        @Override
        public Long createAccumulator() { //初始化辅助变量
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent apacheLogEvent, Long acc) {
            return acc + 1L;
        }

        @Override
        public Long merge(Long acc1, Long acc2) {
            return acc1 + acc2;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }
    }

    private static class PeriodicWatermarkGenerator implements WatermarkGenerator<ApacheLogEvent>, Serializable {

        private long currentMaxEventTime = 0L;
        private long maxOutOfOrderness = 10L; // 最大允许的乱序时间 10 秒

        @Override
        public void onEvent(
                ApacheLogEvent event, long eventTimestamp, WatermarkOutput output) {
            long currentElementEventTime = event.getEventTime();
            currentMaxEventTime = Math.max(currentMaxEventTime, currentElementEventTime);

        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {

            output.emitWatermark(new Watermark((currentMaxEventTime - maxOutOfOrderness) * 1000));
        }
    }

    private static class TimeStampExtractor implements TimestampAssigner<ApacheLogEvent> {
        @Override
        public long extractTimestamp(ApacheLogEvent element, long recordTimestamp) {
            return element.getEventTime();
        }
    }
}

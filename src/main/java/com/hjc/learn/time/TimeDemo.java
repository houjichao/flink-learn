package com.hjc.learn.time;

/**
 * Flink中的时间语义（Time)
 * 1. Event Time: 事件创建时间
 * 2. Ingestion Time: 数据进入Flink的时间
 * 3. Processing Time: 执行操作算子的本地系统时间，与机器相关
 * 当Flink以Event Time模式处理数据时，它会根据数据里的时间戳来处理基于时间的算子，但是往往由于网络传输等原因，会导致乱序
 * 数据产生，比如先产生的数据，比后产生的数据先到达，会导致窗口计算不准确，那么怎么样来避免乱序数据带来的计算不准确？
 * Flink中引入了Watermark水位线，遇到一个时间戳达到了窗口关闭时间，不应该立刻触发窗口计算，而应该等待一段时间，等迟到的数据
 * 来了再关闭窗口。
 * Watermark是一种衡量Event Time进展的机制，可以设定延迟触发，因此Watermark是用来处理乱序事件的，而想要正确的处理乱序
 * 事件，通常用Watermark机制结合window来实现。
 * 数据流中的Watermark用于表示timestamp小于Watermark的数据都已经到达了，因此window的执行也是由Watermark来触发的
 * watermark用来让程序自己平衡延迟和结果正确性两者之间的权重
 * Watermark的特点：
 * 1. watermark是一条特殊的记录
 * 2. watermark必须单调递增，以确保任务的事件时间时钟在向前推进，而不是在后退
 * 3. watermark与数据的时间戳相关
 * 4. watermark传递时，上游向下游传递，使用广播的方式传递，上游有多个任务时，watermark向下游传递到分区watermark，下游任务
 * 的时间按照多个上游时间中最小的为准，比如上游3，4，5数据广播到下游任务那么下游任务的时间为3，表示3s之前的数据都到了
 *
 * 窗口起始点的确定：
 * 整天整小时的，按照8-9， 9-10确定窗口大小
 * 源码计算start的方法：timestamp - (timestamp - offset + windowSize) % windowSize;
 * @author houjichao
 */
public class TimeDemo {

}

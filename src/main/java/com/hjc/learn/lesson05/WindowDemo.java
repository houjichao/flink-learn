package com.hjc.learn.lesson05;

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

}

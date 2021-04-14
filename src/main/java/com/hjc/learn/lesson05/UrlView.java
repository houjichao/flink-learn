package com.hjc.learn.lesson05;

/**
 * @author houjichao
 */
public class UrlView implements Comparable<UrlView> {

    /**
     * 页面
     */
    private String url;

    /**
     * 窗口结束时间
     */
    private long windowEnd;

    /**
     * 次数
     */
    private long count;

    public UrlView() {

    }

    public UrlView(String url, long windowEnd, long count) {
        this.url = url;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "UrlView{" +
                "url='" + url + '\'' +
                ", windowEnd=" + windowEnd +
                ", count=" + count +
                '}';
    }


    /**
     * 降序排序
     * @param urlView
     * @return
     */
    @Override
    public int compareTo(UrlView urlView) {
        return Long.compare(urlView.count, this.count);
    }
}

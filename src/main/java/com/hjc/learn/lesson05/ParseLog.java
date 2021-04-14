package com.hjc.learn.lesson05;

import java.text.SimpleDateFormat;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author houjichao
 */
public class ParseLog implements MapFunction<String,ApacheLogEvent> {

    @Override
    public ApacheLogEvent map(String line) throws Exception {
        String[] fields = line.split(" ");
        SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
        long timeStamp = dateFormat.parse(fields[3].trim()).getTime();
        return new ApacheLogEvent(fields[0].trim(),fields[1].trim(),timeStamp,
                fields[5].trim(),fields[6].trim());
    }
}

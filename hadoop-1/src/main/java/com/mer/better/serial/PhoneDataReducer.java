package com.mer.better.serial;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author MaiShuRen
 * @site https://www.maishuren.top
 * @since 2021-12-12 16:47
 **/
public class PhoneDataReducer extends Reducer<Text, PhoneFlow, Text, PhoneFlow> {

    private final Text outKey = new Text();
    private final PhoneFlow outValue = new PhoneFlow();

    @Override
    protected void reduce(Text key, Iterable<PhoneFlow> values, Context context) throws IOException, InterruptedException {
        AtomicInteger totalUpFlow = new AtomicInteger(0);
        AtomicInteger totalDownFlow = new AtomicInteger(0);
        values.forEach(e -> {
            totalUpFlow.addAndGet(e.getUpFlow());
            totalDownFlow.addAndGet(e.getDownFlow());
        });

        outKey.set(key);
        outValue.setUpFlow(totalUpFlow.get());
        outValue.setDownFlow(totalDownFlow.get());
        outValue.setSumFlow();

        context.write(outKey, outValue);
    }
}

package com.mer.better.mr1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author MaiShuRen
 * @site https://www.maishuren.top
 * @since 2021-12-11 21:50
 **/
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final Text outKey = new Text();
    private final IntWritable outValue = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalCount = 0;
        for (IntWritable value : values) {
            totalCount += value.get();
        }
        outKey.set(key);
        outValue.set(totalCount);
        context.write(outKey, outValue);
    }
}

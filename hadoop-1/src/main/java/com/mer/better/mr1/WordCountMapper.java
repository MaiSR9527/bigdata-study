package com.mer.better.mr1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author MaiShuRen
 * @site https://www.maishuren.top
 * @since 2021-12-11 21:50
 **/
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final Text outKey = new Text();
    private final IntWritable outValue = new IntWritable(1);

    /**
     * 每读取一次数据，执行一次map操作
     *
     * @param key     输入数据的key
     * @param value   输入数据的value
     * @param context 上下文对象
     * @throws IOException          io异常
     * @throws InterruptedException 中断异常
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word : words) {
            outKey.set(word);
            context.write(outKey, outValue);
        }
    }
}

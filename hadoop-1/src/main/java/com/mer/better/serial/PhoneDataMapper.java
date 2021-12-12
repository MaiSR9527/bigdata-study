package com.mer.better.serial;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author MaiShuRen
 * @site https://www.maishuren.top
 * @since 2021-12-12 16:37
 **/
public class PhoneDataMapper extends Mapper<LongWritable, Text, Text, PhoneFlow> {

    private final Text outKey = new Text();
    private final PhoneFlow outValue = new PhoneFlow();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        String phoneNum = fields[1];
        outKey.set(phoneNum);
        outValue.setUpFlow(Integer.parseInt(fields[fields.length - 3]));
        outValue.setDownFlow(Integer.parseInt(fields[fields.length - 2]));
        outValue.setSumFlow();

        context.write(outKey, outValue);
    }
}

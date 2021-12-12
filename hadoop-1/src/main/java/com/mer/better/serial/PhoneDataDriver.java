package com.mer.better.serial;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author MaiShuRen
 * @site https://www.maishuren.top
 * @since 2021-12-12 16:47
 **/
public class PhoneDataDriver {

    public static void main(String[] args) {
        Configuration configuration = new Configuration();

        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(PhoneDataDriver.class);

            job.setMapperClass(PhoneDataMapper.class);
            job.setReducerClass(PhoneDataReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(PhoneFlow.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(PhoneFlow.class);

            FileInputFormat.setInputPaths(job,new Path("D:\\BaiduNetdiskDownload\\data\\phone_data"));
            FileOutputFormat.setOutputPath(job,new Path("D:\\BaiduNetdiskDownload\\data\\output2"));

            job.waitForCompletion(true);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}

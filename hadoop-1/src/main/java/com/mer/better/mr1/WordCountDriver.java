package com.mer.better.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author MaiShuRen
 * @site https://www.maishuren.top
 * @since 2021-12-11 21:50
 **/
public class WordCountDriver {

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(WordCountDriver.class);
            job.setMapperClass(WordCountMapper.class);
            job.setReducerClass(WordCountReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

//            FileInputFormat.setInputPaths(job, new Path("D:\\BaiduNetdiskDownload\\data\\input"));
//            FileOutputFormat.setOutputPath(job,new Path("D:\\BaiduNetdiskDownload\\data\\output"));
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.waitForCompletion(true);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}

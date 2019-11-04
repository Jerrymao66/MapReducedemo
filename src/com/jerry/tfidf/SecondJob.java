package com.jerry.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondJob {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.set("mapreduce.app-submission.coress-paltform", "true");
		conf.set("mapreduce.framework.name", "local");
		try {
			Job job = Job.getInstance(conf);
			job.setJobName("weibo2");

			FileInputFormat.addInputPath(job, new Path("/data/tfidf/output/weibo1"));
			Path outPath = new Path("/data/tfidf/output/weibo2");
			if (outPath.getFileSystem(conf).exists(outPath))
				outPath.getFileSystem(conf).delete(outPath, true);
			FileOutputFormat.setOutputPath(job, outPath);
			
			job.setMapperClass(SecondMapper.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			job.setReducerClass(SecondReducer.class);
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

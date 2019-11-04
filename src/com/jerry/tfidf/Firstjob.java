package com.jerry.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Firstjob {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.set("mapreduce.app-submission.coress-paltform", "true");
		conf.set("mapreduce.framework.name", "local");
		try {
			Job job = Job.getInstance(conf);
			job.setJobName("weibo1");
			
			FileInputFormat.addInputPath(job, new Path("/data/tfidf/input/"));
			Path outPath = new Path("/data/tfidf/output/weibo1");
			if(outPath.getFileSystem(conf).exists(outPath))
				outPath.getFileSystem(conf).delete(outPath, true);
			FileOutputFormat.setOutputPath(job, outPath);
			
			job.setMapperClass(FirstMapper.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			job.setNumReduceTasks(4);
			job.setPartitionerClass(FirstPartition.class);
			
			job.setReducerClass(FirstReduce.class);
			job.waitForCompletion(true);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

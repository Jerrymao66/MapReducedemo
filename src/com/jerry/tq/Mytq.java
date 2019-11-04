package com.jerry.tq;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 找出每个月气温最高的2天
 * 1949-10-01 14:21:02	34c
 */
public class Mytq {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//在win10本地跑的设置
		Configuration conf = new Configuration(true);
		conf.set("mapreduce.app-submission.corss-paltform", "true");
		conf.set("mapreduce.framework.name", "local");
		
		Job job = Job.getInstance(conf);
		job.setJobName("tq");
		//设置输入输出路径
		Path inPath = new Path("/tq/input");
		FileInputFormat.addInputPath(job, inPath);
		Path outPath = new Path("/tq/output");
		if(outPath.getFileSystem(conf).exists(outPath))
			outPath.getFileSystem(conf).delete(outPath, true);
		FileOutputFormat.setOutputPath(job, outPath);
		
		job.setMapperClass(Tmapper.class);
		job.setOutputKeyClass(Tq.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setSortComparatorClass(TsortComparator.class);		
		job.setNumReduceTasks(2);
		job.setPartitionerClass(TPartitioner.class);
		job.setGroupingComparatorClass(TGroupingComparator.class);
		
		job.setReducerClass(Treducer.class);
		job.waitForCompletion(true);
	}

}
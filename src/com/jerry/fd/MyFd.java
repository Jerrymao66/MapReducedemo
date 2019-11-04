package com.jerry.fd;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyFd {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(MyFd.class);

		Path inPath = new Path("/fd/input");
		FileInputFormat.addInputPath(job, inPath);
		Path outPath = new Path("/fd/output");
		if (outPath.getFileSystem(conf).exists(outPath))
			outPath.getFileSystem(conf).delete(outPath, true);
		FileOutputFormat.setOutputPath(job, outPath);

		job.setMapperClass(FMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setReducerClass(FReduce.class);
		job.waitForCompletion(true);
	}

}

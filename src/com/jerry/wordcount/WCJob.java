package com.jerry.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WCJob {

	public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.set("mapreduce.app-submission.coress-paltform", "true");
		conf.set("mapreduce.framework.name", "local");
		
		try {
			Job job = Job.getInstance(conf);
			job.setJobName("wordcount");
			
			FileInputFormat.addInputPath(job,new Path("/input"));
			Path outPath = new Path("/output");
			if(outPath.getFileSystem(conf).exists(outPath))
				outPath.getFileSystem(conf).delete(outPath, true);
			FileOutputFormat.setOutputPath(job, outPath);
			job.setMapperClass(WCMapper.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			job.setCombinerClass(WCReducer.class);
			job.setReducerClass(WCReducer.class);
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	static class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			for (String string : split) {
				context.write(new Text(string), new IntWritable(1));
			}
		}
		
	}
	static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> vals,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : vals) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
		
	}
}

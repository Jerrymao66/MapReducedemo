package com.jerry.itemcf;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//清洗重复数据
public class Step1 {

	public static boolean run(Configuration conf, Map<String, String> paths) {
		try {
			Job job = Job.getInstance(conf);
			job.setJobName("step1");

			FileInputFormat.addInputPath(job, new Path(paths.get("Step1Input")));
			Path outpath = new Path(paths.get("Step1Output"));
			if (outpath.getFileSystem(conf).exists(outpath))
				outpath.getFileSystem(conf).delete(outpath, true);
			FileOutputFormat.setOutputPath(job, outpath);

			job.setMapperClass(Step1_Mapper.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			job.setReducerClass(Step1_Reducer.class);
			boolean f = job.waitForCompletion(true);
			return f;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	static class Step1_Mapper extends Mapper<LongWritable, Text, Text, NullWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (key.get() != 0)
				context.write(value, NullWritable.get());
		}

	}

	static class Step1_Reducer extends Reducer<Text, IntWritable, Text, NullWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> itr, Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}

	}

}

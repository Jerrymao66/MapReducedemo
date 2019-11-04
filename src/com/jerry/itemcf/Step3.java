package com.jerry.itemcf;

import java.io.IOException;
import java.util.Map;

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

//建立同现矩阵
public class Step3 {
	private static final Text K = new Text();
	private static final IntWritable V = new IntWritable(1);

	public static boolean run(Configuration conf, Map<String, String> paths) {
		try {
			Job job = Job.getInstance(conf);
			job.setJobName("step3");

			FileInputFormat.addInputPath(job, new Path(paths.get("Step3Input")));
			Path outPath = new Path(paths.get("Step3Output"));
			if (outPath.getFileSystem(conf).exists(outPath))
				outPath.getFileSystem(conf).delete(outPath, true);
			FileOutputFormat.setOutputPath(job, outPath);

			job.setMapperClass(Step3_Mapper.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			job.setReducerClass(Step3_Reducer.class);
			boolean b = job.waitForCompletion(true);
			if(b)
				return true;
		} catch (Exception e) {
			e.printStackTrace();
		}

		return false;
	}

	// u24 i64:1,i218:1,i185:1,
	// u25 i64:1,i218:1,i185:1,
	static class Step3_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			String[] items = tokens[1].split(",");
			for (int i = 0; i < items.length; i++) {
				String itemA = items[i].split(":")[0];
				for (int j = 0; j < items.length; j++) {
					String itemB = items[j].split(":")[0];
					K.set(itemA + ":" + itemB);
					context.write(K, V);
				}
			}
		}
	}

	static class Step3_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> vals, Context context)
				throws IOException, InterruptedException {
			int sum =0 ;
			for (IntWritable val : vals) {
				sum+=val.get();
			}
			V.set(sum);
			context.write(key, V);
		}

	}

}

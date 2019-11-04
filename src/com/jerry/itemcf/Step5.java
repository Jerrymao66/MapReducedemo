package com.jerry.itemcf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step5 {

	public static boolean run(Configuration conf, Map<String, String> paths) {
		try {
			FileSystem fs = FileSystem.get(conf);
			Job job = Job.getInstance(conf);
			job.setJobName("step5");
			job.setJarByClass(StartRun.class);
			job.setMapperClass(Step5_Mapper.class);
			job.setReducerClass(Step5_Reducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			FileInputFormat
					.addInputPath(job, new Path(paths.get("Step5Input")));
			Path outpath = new Path(paths.get("Step5Output"));
			if (fs.exists(outpath)) {
				fs.delete(outpath, true);
			}
			FileOutputFormat.setOutputPath(job, outpath);

			boolean f = job.waitForCompletion(true);
			return f;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
		
	}
	
	static class Step5_Mapper extends Mapper<LongWritable, Text, Text, Text>{

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] tokens = Pattern.compile("[\t,]").split(value.toString());
			Text k = new Text(tokens[0]);// 用户为key
			Text v = new Text(tokens[1] + "," + tokens[2]);
			context.write(k, v);
		}
		
	}
	
	static class Step5_Reducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> iterable, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
				Map<String, Double> map = new HashMap<>();
				for (Text line : iterable) {
					String[] tokens = line.toString().split(",");
					String itemId = tokens[0];
					Double score = Double.parseDouble(tokens[1]);
					if(map.containsKey(itemId)) {
						map.put(itemId, map.get(itemId)+score);
					}else {
						map.put(itemId, score);
					}
				}
		
				Iterator<String> iterator = map.keySet().iterator();
				while(iterator.hasNext()) {
					String itemid = iterator.next();
					Double score = map.get(itemid);
					context.write(key, new Text(itemid+","+score));
				}
		}
		
	}
}

package com.jerry.itemcf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//得到用户的喜爱度数据
//u21 i266:1, u24 i64:1,i218:1,i185:1,
public class Step2 {

	public static boolean run(Configuration conf, Map<String, String> paths) {
		try {
			Job job = Job.getInstance(conf);
			job.setJobName("step2");

			FileInputFormat.addInputPath(job, new Path(paths.get("Step2Input")));
			Path outPath = new Path(paths.get("Step2Output"));
			if (outPath.getFileSystem(conf).exists(outPath))
				outPath.getFileSystem(conf).delete(outPath, true);
			FileOutputFormat.setOutputPath(job, outPath);

			job.setMapperClass(Step2_Mapper.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setReducerClass(Step2_Reducer.class);
			boolean b = job.waitForCompletion(true);
			if (b)
				return true;
		} catch (Exception e) {
			e.printStackTrace();
		}

		return false;
	}

	// item_id,user_id,action,vtime
	// i161,u2625,click,2014/9/18 15:03
	// 输出 u2625 i161:1
	static class Step2_Mapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] strings = value.toString().split(",");
			String item = strings[0];
			String user = strings[1];
			String action = strings[2];
			Integer rv = StartRun.R.get(action);
			context.write(new Text(user), new Text(item + ":" + rv.intValue()));
		}

	}

	// u2625 i161:1
	// u2625 i161:2
	// 输出 u24 i64:1,i218:1,i185:1,
	static class Step2_Reducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> vals, Context context) throws IOException, InterruptedException {
			Map<String, Integer> map = new HashMap<String, Integer>();
			for (Text val : vals) {
				String[] s = val.toString().split(":");
				String item = s[0];
				int action = Integer.parseInt(s[1]);
				action = (map.get(item) == null ? 0 : map.get(item)) + action;
				map.put(item, action);
			}
			StringBuffer sBuffer = new StringBuffer();
			for (Entry<String, Integer> entry : map.entrySet()) {
				sBuffer.append(entry.getKey() + ":" + entry.getValue().intValue() + ",");
			}
			context.write(key, new Text(sBuffer.toString()));
		}

	}
}

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step4 {

	public static boolean run(Configuration conf, Map<String, String> paths) {
		try {
			FileSystem fs = FileSystem.get(conf);
			Job job = Job.getInstance(conf);
			job.setJobName("step4");
			job.setJarByClass(StartRun.class);
			job.setMapperClass(Step4_Mapper.class);
			job.setReducerClass(Step4_Reducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			// FileInputFormat.addInputPath(job, new
			// Path(paths.get("Step4Input")));
			FileInputFormat.setInputPaths(job,
					new Path[] { new Path(paths.get("Step4Input1")), new Path(paths.get("Step4Input2")) });
			Path outpath = new Path(paths.get("Step4Output"));
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

	static class Step4_Mapper extends Mapper<LongWritable, Text, Text, Text> {
		private String flag;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			flag = inputSplit.getPath().getParent().getName();
		}

//itemA的同现商品数
//用户对itemA的喜爱度		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = Pattern.compile("[\t,]").split(value.toString());
			if (flag.equals("step3")) {
				String[] items = tokens[0].split(":");
				String itemId1 = items[0];
				String itemId2 = items[1];
				String num = tokens[1];
				context.write(new Text(itemId1), new Text("A:" + itemId2 + "," + num));
			} else if (flag.equals("step2")) {
				String userId = tokens[0];
				for (int i = 1; i < tokens.length; i++) {
					String[] strings = tokens[i].split(":");
					String itemId = strings[0];
					String pref = strings[1];
					context.write(new Text(itemId), new Text("B:" + userId + "," + pref));
				}
			}
		}

	}

	static class Step4_Reducer extends Reducer<Text, Text, Text, Text> {
		

		@Override
		protected void reduce(Text key, Iterable<Text> vals, Context context) throws IOException, InterruptedException {
			 Map<String, Integer> mapA = new HashMap<>();
			 Map<String, Integer> mapB = new HashMap<>();
			for (Text line : vals) {
			String val = line.toString();
			String[] kv = Pattern.compile("[\t,]").split(val.substring(2));
				if(val.startsWith("A:")) {
					mapA.put(kv[0], Integer.parseInt(kv[1]));
				}else if(val.startsWith("B:")) {
					mapB.put(kv[0], Integer.parseInt(kv[1]));
				}
			}
			double result=0;
			Iterator<String> iterator = mapA.keySet().iterator();
			while(iterator.hasNext()) {
				String itemID = iterator.next();
				int tongXianNum	= mapA.get(itemID);		
				Iterator<String> iterb = mapB.keySet().iterator();
				while(iterb.hasNext()) {
					String userID = iterb.next();
					int pref = mapB.get(userID).intValue();
					result=tongXianNum*pref;
					context.write(new Text(userID), new Text(itemID+","+result));
				}
			}
			
		}

	}
}

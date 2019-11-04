package com.jerry.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRankJob {
	public static enum Mycounter{
		my
	}

	public static void main(String[] args) {

		Configuration conf = new Configuration();
		conf.set("mapreduce.app-submission.corss-paltform", "true");
		conf.set("mapreduce.framework.name", "local");
		int i = 0;
		double d=0.0000001;
		try {
			while (true) {
				i++;
				conf.setInt("runcount", i);
				Job job = Job.getInstance(conf);
				job.setJarByClass(PageRankJob.class);
				job.setJobName("pr" + i);

				FileSystem fs = FileSystem.get(conf);
				if(i==1) {
				Path inpath = new Path("/data/pagerank/input");
				FileInputFormat.addInputPath(job, inpath);
				}else {
					Path inpath = new Path("/data/pagerank/out/pr"+(i-1));
					FileInputFormat.addInputPath(job, inpath);
				}
				Path outPath = new Path("/data/pagerank/out/pr"+i);
				if(fs.exists(outPath))
					fs.delete(outPath, true);
				FileOutputFormat.setOutputPath(job, outPath);
				
				job.setMapperClass(PageRankMapper.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				job.setReducerClass(PageRankReduce.class);
				//设置新的输入格式
				job.setInputFormatClass(KeyValueTextInputFormat.class);
				boolean flag = job.waitForCompletion(true);
				if(flag) {
					long sum = job.getCounters().findCounter(Mycounter.my).getValue();
					double avgd = sum / 4000.0;
					if (avgd < d) {
						break;
					}
				}
				
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

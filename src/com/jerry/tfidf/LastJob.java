package com.jerry.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class LastJob {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.set("mapreduce.app-submission.coress-paltform", "true");
		conf.set("mapreduce.framework.name", "local");
		try {
			Job job = Job.getInstance(conf);
			job.setJobName("weibo3");
			
			job.addCacheFile(new Path("/data/tfidf/output/weibo1/part-r-00003").toUri());
			job.addCacheFile(new Path("/data/tfidf/output/weibo2/part-r-00000").toUri());
			
			FileInputFormat.addInputPath(job, new Path("/data/tfidf/output/weibo1"));
			Path outPath = new Path("/data/tfidf/output/weibo3");
			if(outPath.getFileSystem(conf).exists(outPath))
				outPath.getFileSystem(conf).delete(outPath, true);
			FileOutputFormat.setOutputPath(job, outPath);
			
			job.setMapperClass(LastMapper.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setReducerClass(LastReducer.class);
			
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

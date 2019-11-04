package com.jerry.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> vals, Context context)
			throws IOException, InterruptedException {
		int sum =0;
		for (IntWritable val : vals) {
			sum += val.get();
		}
		context.write(key, new IntWritable(sum));
		
	}

}

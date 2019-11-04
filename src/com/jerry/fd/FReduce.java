package com.jerry.fd;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable tval = new IntWritable();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> vals, Context context)
			throws IOException, InterruptedException {
		//对相同朋友个数直接累加，如果直接认识就过滤，不推荐
		int sum = 0;
		for (IntWritable val : vals) {
			if (val.get() == 0)
				return;

			sum ++;
			
		}
		tval.set(sum);
		context.write(key, tval);
	}

}

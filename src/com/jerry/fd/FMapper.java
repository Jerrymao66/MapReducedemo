package com.jerry.fd;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class FMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text tkey = new Text();
	private IntWritable tval = new IntWritable();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] words = StringUtils.split(value.toString(), ' ');
		for (int i = 1; i < words.length; i++) {
			//直接认识设置0
			tkey.set(getString(words[0], words[i]));
			tval.set(0);
			context.write(tkey, tval);
			//不直接认识设置1
			for (int j = i + 1; j < words.length; j++) {
				tkey.set(getString(words[i], words[j]));
				tval.set(1);
				context.write(tkey, tval);
			}
		}

	}
	//将朋友与朋友之间的关系，按字典排序因为 tom:hadoop	1 == hadoop:tom	1
	private String getString(String a, String b) {
		return a.compareTo(b) > 0 ? b + ":" + a : a + ":" + b;
	}

}

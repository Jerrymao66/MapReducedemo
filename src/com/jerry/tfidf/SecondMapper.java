package com.jerry.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class SecondMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		FileSplit inputSplit = (FileSplit) context.getInputSplit();
		if(!inputSplit.getPath().getName().contains("part-r-00003")) {
				String[] strings = value.toString().trim().split("\t");
				if(strings.length>=2) {
					String[] split = strings[0].split("_");
					String word = split[0];
					context.write(new Text(word), new IntWritable(1));
				}else {
					System.out.println(value.toString() + "-------------");
				}
		}
	}

}

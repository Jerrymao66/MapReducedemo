package com.jerry.tfidf;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

public class FirstMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] strings = value.toString().trim().split("\t");
		if(strings.length>=2) {
			String id = strings[0];
			String content = strings[1];
			
			StringReader reader = new StringReader(content);
			IKSegmenter ikSegmenter = new IKSegmenter(reader, true);
			Lexeme word = null;
			while((word=ikSegmenter.next())!=null) {
				String wString = word.getLexemeText();
				
				context.write(new Text(wString+"_"+id), new IntWritable(1));
			}
			context.write(new Text("count"), new IntWritable(1));
		}
		
	}

}

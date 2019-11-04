package com.jerry.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LastReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> vals, Context context) throws IOException, InterruptedException {
		StringBuffer buffer = new StringBuffer();
		for (Text val : vals) {
			buffer.append(val.toString()+"\t");
		}
		context.write(key, new Text(buffer.toString()));
	}

}

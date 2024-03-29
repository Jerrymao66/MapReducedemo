package com.jerry.tfidf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class FirstPartition extends HashPartitioner<Text, IntWritable>{

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		if(key.equals(new Text("count")))
			return 3;
		else
			return super.getPartition(key, value, numPartitions-1);
	}
	

}

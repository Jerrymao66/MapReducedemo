package com.jerry.tq;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
/**
 * 自定义分组器，定义分组规则
 * @author JerryMao
 *
 */
public class TPartitioner extends Partitioner<Tq, IntWritable>{

	@Override
	public int getPartition(Tq key, IntWritable value, int numPartitions) {
		return key.getYear()%numPartitions;
	}

}

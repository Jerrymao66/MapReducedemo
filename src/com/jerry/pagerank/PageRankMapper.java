package com.jerry.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//keyout A  valueout pr B D
//keyout A 	valueout  
public class PageRankMapper extends Mapper<Text, Text, Text, Text> {

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		int runcount = context.getConfiguration().getInt("runcount",1);
		Node node = null;
		if(runcount==1) {
			node=Node.formMr("1.00", value.toString());
		}else {
			node=Node.formMr(value.toString());
		}
		//A 1 B D
		context.write(key, new Text(node.toString()));
		if(node.containsAdjacentNodes()) {
			double outValue = node.getPageRank()/node.getAdjacentNodeNames().length;
			for (String s : node.getAdjacentNodeNames()) {
				// B 0.5
				context.write(new Text(s), new Text(outValue+""));
			}
		}
	}

}

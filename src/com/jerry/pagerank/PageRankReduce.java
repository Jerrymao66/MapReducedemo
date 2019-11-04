package com.jerry.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//A  1 B C
//A 0.5
public class PageRankReduce extends Reducer<Text, Text, Text, Text>{

	@Override
	protected void reduce(Text key, Iterable<Text> vals,Context context)
			throws IOException, InterruptedException {
		Node sourceNode = null;
		double sum=0;
		for (Text val : vals) {
			
			Node node = Node.formMr(val.toString());
			if(node.containsAdjacentNodes())
				sourceNode=node;
			else 
				sum+=node.getPageRank();
		}
		double newPR = (0.15 / 4.0) + (0.85 * sum);
		System.out.println("*********** new pageRank value is " + newPR);

		double d = newPR - sourceNode.getPageRank();
		int j = (int) (d * 1000.0);
		j = Math.abs(j);
		System.out.println(j + "___________");
		context.getCounter(PageRankJob.Mycounter.my).increment(j);
		sourceNode.setPageRank(newPR);
		context.write(key, new Text(sourceNode.toString()));
	}
	
	
}

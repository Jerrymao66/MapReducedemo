package com.jerry.tfidf;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class LastMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Map<String, Integer> count = null;
	private Map<String, Integer> df = null;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		if ((count == null || count.size() == 0) && (df == null || df.size() == 0)) {
			URI[] ss = context.getCacheFiles();
			if (ss != null) {
				for (int i = 0; i < ss.length; i++) {
					if (ss[i].getPath().endsWith("part-r-00003")) {
						Path path = new Path(ss[i].getPath());
						BufferedReader reader = new BufferedReader(new FileReader(path.getName()));
						count = new HashMap<>();
						String readLine = reader.readLine();
						if (readLine.startsWith("count")) {
							String[] split = readLine.split("\t");
							count.put("count", Integer.parseInt(split[1].trim()));
						}
						reader.close();
					} else if (ss[i].getPath().endsWith("part-r-00000")) {
						Path path = new Path(ss[i].getPath());
						BufferedReader reader = new BufferedReader(new FileReader(path.getName()));
						df = new HashMap<>();
						String line;
						while((line=reader.readLine())!=null) {
							String[] split = line.split("\t");
							df.put(split[0], Integer.parseInt(split[1].trim()));
						}
						reader.close();
					}
				}
			}
		}
	}
	//0.03_3824246315213843	1
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			FileSplit split = (FileSplit) context.getInputSplit();
			if(!split.getPath().getName().contains("part-r-00003")) {
				String[] strings = value.toString().trim().split("\t");
				if(strings.length>=2) {
					int tf = Integer.parseInt(strings[1].trim());
					String[] s = strings[0].split("_");
					if(s.length>=2) {
						String w = s[0];
						String id = s[1];
						double ss= tf*Math.log(count.get("count")/df.get(w));
						NumberFormat nfFormat= NumberFormat.getInstance();
						nfFormat.setMaximumFractionDigits(5);
						String format = nfFormat.format(ss);
						context.write(new Text(id), new Text(w+":"+format));
					}
				}else {
					System.out.println(value.toString()+"-------------");
				}
			}
		
	}

}

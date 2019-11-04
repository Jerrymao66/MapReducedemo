package com.jerry.itemcf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step6 {

	public static boolean run(Configuration conf, Map<String, String> paths) {
		try {
			FileSystem fs = FileSystem.get(conf);
			Job job = Job.getInstance(conf);
			job.setJobName("step6");

			FileInputFormat.addInputPath(job, new Path(paths.get("Step6Input")));
			Path outpath = new Path(paths.get("Step6Output"));
			if (fs.exists(outpath)) {
				fs.delete(outpath, true);
			}
			FileOutputFormat.setOutputPath(job, outpath);

			job.setMapperClass(Step6_Mapper.class);
			job.setSortComparatorClass(NumSort.class);
			job.setOutputKeyClass(PairWritable.class);
			job.setOutputValueClass(Text.class);
			job.setGroupingComparatorClass(UserGroup.class);
			
			job.setReducerClass(Step6_Reducer.class);
			boolean f = job.waitForCompletion(true);
			return f;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	static class Step6_Mapper extends Mapper<LongWritable, Text, PairWritable, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = Pattern.compile("[\t,]").split(value.toString());
			PairWritable k = new PairWritable();
			k.setUid(tokens[0]);
			k.setNum(Double.parseDouble(tokens[2]));
			context.write(k, new Text(tokens[1] + ":" + tokens[2]));
		}

	}
	// item:8
	static class Step6_Reducer extends Reducer<PairWritable, Text, Text, Text>{

		@Override
		protected void reduce(PairWritable key, Iterable<Text> itr,Context context) throws IOException, InterruptedException {
			int i=0;
			StringBuffer sBuffer =new StringBuffer();
			for (Text line : itr) {
				if(i==10)
					break;
				i++;
				sBuffer.append(line.toString());
			}
			context.write(new Text(key.getUid()), new Text(sBuffer.toString()));
		}
		
	}

	static class NumSort extends WritableComparator {
		public NumSort() {
			super(PairWritable.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			PairWritable o1 = (PairWritable) a;
			PairWritable o2 = (PairWritable) b;
			int c = o1.getUid().compareTo(o2.getUid());
			if(c==0) {
						return -Double.compare(o1.getNum(), o2.getNum());
					}
			return c;
			}

	}
	static class UserGroup extends WritableComparator{

		
		public UserGroup() {
			super(PairWritable.class,true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			PairWritable o1 =(PairWritable) a;
			PairWritable o2 =(PairWritable) b;
			return o1.getUid().compareTo(o2.getUid());
		}
		
	}
	static class PairWritable implements WritableComparable<PairWritable> {

		private String uid;
		private double num;

		public String getUid() {
			return uid;
		}

		public void setUid(String uid) {
			this.uid = uid;
		}

		public double getNum() {
			return num;
		}

		public void setNum(double num) {
			this.num = num;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(uid);
			out.writeDouble(num);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.uid = in.readUTF();
			this.num = in.readDouble();
		}

		@Override
		public int compareTo(PairWritable o) {
			int c = this.uid.compareTo(o.getUid());
			if (c == 0) {
				return Double.compare(this.num, o.getNum());
			}
			return c;
		}

	}
}

package com.jerry.tq;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * 天气reducer
 * @author JerryMao
 *
 */
public class Treducer extends Reducer<Tq, IntWritable, Text, IntWritable> {
	private Text tkey=new Text();
	private IntWritable tval = new IntWritable();

	@Override
	protected void reduce(Tq key, Iterable<IntWritable> vals, Context context)
			throws IOException, InterruptedException {
		int flag = 0;//判读是不是这个月的第一个数据
		int day = 0;
		//因为是找出每个月气温最高的2天，但有可能是一天出现两个最高温，
		for (IntWritable val : vals) {
			if (flag == 0) {
				tkey.set(key.toString());
				tval.set(val.get());
				context.write(tkey, tval);
				flag++;
				day = key.getDay();
			}
			if (flag != 0 && day != key.getDay()) {//不是第一个数据，判断是不是相同天
				tkey.set(key.toString());
				tval.set(val.get());
				context.write(tkey, tval);
				return;
			}
		}
	}

}

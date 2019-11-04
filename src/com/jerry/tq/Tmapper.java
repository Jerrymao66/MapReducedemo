package com.jerry.tq;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
/**
 * 天气mapper类
 * @author JerryMao
 *
 */
public class Tmapper extends Mapper<LongWritable, Text, Tq, IntWritable> {
	private Tq tkey = new Tq();
	private IntWritable tval = new IntWritable();
	
	//1949-10-01 14:21:02	34c
	//1949-10-01 19:21:02	38c
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String[] words = StringUtils.split(value.toString(), '\t');//对每一行进行切割
		//格式化天气，把时分去了
		String pattern = "yyyy-MM-dd";
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);
		try {
			Date date = sdf.parse(words[0]);
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			tkey.setYear(cal.get(Calendar.YEAR));
			tkey.setMonth(cal.get(Calendar.MONTH)+1);
			tkey.setDay(cal.get(Calendar.DAY_OF_MONTH));
	
			int wd = Integer.parseInt(words[1].substring(0,words[1].lastIndexOf("c")));
			tkey.setWd(wd);
			//形成天气和温度的key，value映射
			tval.set(wd);
			context.write(tkey, tval);
			
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		
	}

}

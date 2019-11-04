package com.jerry.tq;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
/**
 * 自定义天气类需要实现WritableComparable接口
 * @author JerryMao
 *
 */
public class Tq implements WritableComparable<Tq>{
	private int year;
	private int month;
	private int day;
	private int wd;
	public Tq() {
	}
	public int getYear() {
		return year;
	}
	public void setYear(int year) {
		this.year = year;
	}
	public int getMonth() {
		return month;
	}
	public void setMonth(int month) {
		this.month = month;
	}
	public int getDay() {
		return day;
	}
	public void setDay(int day) {
		this.day = day;
	}
	public int getWd() {
		return wd;
	}
	public void setWd(int wd) {
		this.wd = wd;
	}
	@Override
	//重写序列化和反序列化方法
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.year);
		out.writeInt(this.month);
		out.writeInt(this.day);
		out.writeInt(this.wd);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.setYear(in.readInt());
		this.setMonth(in.readInt());
		this.setDay(in.readInt());
		this.setWd(in.readInt());
	}
	@Override
	public int compareTo(Tq o) {
		int	a = Integer.compare(this.year, o.getYear());
		if(a==0) {
			int b = Integer.compare(this.month, o.getMonth());
				if(b==0) {
					return Integer.compare(this.day, o.getDay());
				}
			return b;
		}
		
		return a;
	}
	@Override
	public String toString() {
		return year + "-" + month + "-" + day;
	}

	
}

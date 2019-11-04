package com.jerry.tq;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
/**
 * 自定义组排序类，因为map端传来的key是Tq是按天排序，让reducer端，相同的月作为一组key，
 * @author JerryMao
 *
 */
public class TGroupingComparator extends WritableComparator{
	Tq tq1=null;
	Tq tq2 =null;
	
	//这里注意要实现父类构造器方法，目的是创建两个compare对象
	public TGroupingComparator() {
		super(Tq.class, true);
	}

	//
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		tq1 = (Tq)a;
		tq2 = (Tq)b;
		int c1 = Integer.compare(tq1.getYear(),tq2.getYear());
		if(c1==0) {
			return Integer.compare(tq1.getMonth(), tq2.getMonth());
		}
		return c1;
	}
	
}

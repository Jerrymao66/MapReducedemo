package com.jerry.tq;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
/**
 * 自定义排序器，map端shuffe阶段排序
 * @author JerryMao
 *
 */
public class TsortComparator extends WritableComparator {

	Tq tq1 = null;
	Tq tq2 = null;
	
	public TsortComparator() {
		super(Tq.class, true);
	}
	@Override
	//相同月份按照温度降序
	public int compare(WritableComparable a, WritableComparable b) {
		tq1 = (Tq)a;
		tq2 = (Tq)b;
		int c1 = Integer.compare(tq1.getYear(),tq2.getYear());
		if(c1==0) {
			int c2 = Integer.compare(tq1.getMonth(), tq2.getMonth());
			if(c2==0) {
				return -Integer.compare(tq1.getWd(),tq2.getWd());
			}
			return c2;
		}
		return c1;
	}

}

package com.spark.KNNTest.sparkapp;

import java.io.Serializable;
import java.util.Comparator;

public class YComparator implements Comparator<Point> ,Serializable{

	@Override
	public int compare(Point o1, Point o2) {
		// TODO Auto-generated method stub
		if(o1.getY()>o2.getY())
			return 1;
		else if(o1.getY()<o2.getY())
			return -1;
		else {
			return 0;
		}
	}

}

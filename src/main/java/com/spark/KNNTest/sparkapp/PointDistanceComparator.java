package com.spark.KNNTest.sparkapp;

import java.util.Comparator;

public class PointDistanceComparator implements Comparator<Point> {
	Point queryCenter;
	
	public PointDistanceComparator(Point queryCenter)
	{
		this.queryCenter=queryCenter;
	}
	@Override
	public int compare(Point p1, Point p2) {
		double distance1 = p1.distance(queryCenter);
		double distance2 = p2.distance(queryCenter);
		if (distance1 > distance2) {
			return 1;
		} else if (distance1 == distance2) {
			return 0;
		}
		return -1;
		
	}

}

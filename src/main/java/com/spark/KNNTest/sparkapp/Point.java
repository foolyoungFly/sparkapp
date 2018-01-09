package com.spark.KNNTest.sparkapp;

import java.io.Serializable;



public class Point implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 4902022702746614570L;
	private double x;
	private double y;
	private String landuse;
	
	public Point(double x,double y,String landuse){
		this.x = x;
		this.y = y;
		this.landuse = landuse;
		
	}
	
	public double getX() {
		return x;
	}
	public void setX(double x) {
		this.x = x;
	}
	public double getY() {
		return y;
	}
	public void setY(double y) {
		this.y = y;
	}
	public String getLanduse() {
		return landuse;
	}
	public void setLanduse(String landuse) {
		this.landuse = landuse;
	}
	
	public double distance(Point c) {
	    double dx = x - c.getX();
	    double dy = y - c.getY();
	    return Math.sqrt(dx * dx + dy * dy);
	  }

}

package com.spark.KNNTest.sparkapp;



import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;



public class ClassifyByKNN {
	//计算点在网格中的位置
	 public static int locationMapping (double axisMin, double axisLocation,double axisMax)
	  {
		  Double gridLocation;
		  int gridResolution=Short.MAX_VALUE;
		  gridLocation=(axisLocation-axisMin)*gridResolution/(axisMax-axisMin);
		  return gridLocation.intValue();
	  }
	 
	 //计算点的编码值
	 public static int computeHValue(int n, int x, int y) {//变量n是网格在X方向或者Y方向上单元的个数
			int h = 0;
			for (int s = n/2; s > 0; s/=2) {
			  int rx = (x & s) > 0 ? 1 : 0;
			  int ry = (y & s) > 0 ? 1 : 0;
			  h += s * s * ((3 * rx) ^ ry);

			  // Rotate
			  if (ry == 0) {
				if (rx == 1) {
				  x = n-1 - x;
				  y = n-1 - y;
				}

				//Swap x and y
				int t = x; x = y; y = t;
			  }
			}
			return h;
		  }	
	 
	 //计算点对应的网格ID
	 public static int gridID(double Xmin,double XMax,double YMin,double YMax,Point point,int[] partitionBounds) {
			int x=locationMapping(Xmin,XMax,point.getX());
			int y=locationMapping(YMin,YMax,point.getY());
			int gridResolution=Short.MAX_VALUE;
			int hValue = computeHValue(gridResolution+1,x,y);
			int partition = Arrays.binarySearch(partitionBounds, hValue);
			if (partition < 0)
			  partition = -partition - 1;
			return partition;
		}
	 
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//第一步，创建sparkconf对象
		SparkConf conf = new SparkConf().setAppName("ClassifyByKNN");
		//System.out.println("1.完成第一步........");
		//第二步，创建sparkcontext对象
		//sparkcontext是spark程序的入口
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//第三步，读取文件
		//JavaRDD<String> TaxiData = sc.textFile("hdfs://192.168.200.97:9000/Trips/trip_data_3days_transform.txt");
		/*
		String borough_Bk_latlong = "hdfs://192.168.200.97:9000/Borough/BK.csv";
		String borough_BX_latlong= "hdfs://192.168.200.97:9000/Borough/BX.csv";
		String borough_MN_latlong = "hdfs://192.168.200.97:9000/Borough/MN.csv";
		String borough_QN_latlong = "hdfs://192.168.200.97:9000/Borough/QN.csv";	
		String borough_SI_latlong= "hdfs://192.168.200.97:9000/Borough/SI.csv";	
		*?
		/*
		String borough_Bk_latlong = "E://学习/毕设/数据/BK.txt";
		String borough_BX_latlong= "E://学习/毕设/数据/BX.txt";
		String borough_MN_latlong = "E://学习/毕设/数据/MN.txt";
		String borough_QN_latlong = "E://学习/毕设/数据/QN.txt";	
		String borough_SI_latlong= "E://学习/毕设/数据/SI.txt";	
		*/
		//排除第一行，排除29 landtype不明确的，75,75排除XCoord和YCoord 
		/*JavaRDD<String> Bks_tmp = sc.textFile(borough_Bk_latlong).filter(new Function<String, Boolean>() {
			
			public Boolean call(String line)
			{
				List<String> lineSplitList;
				lineSplitList = Arrays.asList(line.split(","));
				return  (lineSplitList.get(0).equals("Borough")==false && lineSplitList.get(75).equals("")==false && lineSplitList.get(76).equals("")==false && lineSplitList.get(29).equals("")==false);
			}
		});

		
		
		JavaRDD<String> BXs_tmp = sc.textFile(borough_BX_latlong).filter(new Function<String, Boolean>() {
			public Boolean call(String line)
			{
				List<String> lineSplitList;
				lineSplitList = Arrays.asList(line.split(","));
				return  (lineSplitList.get(0).equals("Borough")==false && lineSplitList.get(75).equals("")==false && lineSplitList.get(76).equals("")==false && lineSplitList.get(29).equals("")==false);
			}
		});
				
		JavaRDD<String> MNs_tmp = sc.textFile(borough_MN_latlong).filter(new Function<String, Boolean>() {
			public Boolean call(String line)
			{
				List<String> lineSplitList;
				lineSplitList = Arrays.asList(line.split(","));
				return  (lineSplitList.get(0).equals("Borough")==false && lineSplitList.get(75).equals("")==false && lineSplitList.get(76).equals("")==false && lineSplitList.get(29).equals("")==false);
			}
		});	
		
		JavaRDD<String> QNs_tmp = sc.textFile(borough_QN_latlong).filter(new Function<String, Boolean>() {
			public Boolean call(String line)
			{
				List<String> lineSplitList;
				lineSplitList = Arrays.asList(line.split(","));
				return  (lineSplitList.get(0).equals("Borough")==false && lineSplitList.get(75).equals("")==false && lineSplitList.get(76).equals("")==false && lineSplitList.get(29).equals("")==false);
			}
		});	
		
		  JavaRDD<String> SIs_tmp = sc.textFile(borough_SI_latlong).filter(new Function<String, Boolean>() {
			public Boolean call(String line)
			{
				List<String> lineSplitList;
				lineSplitList = Arrays.asList(line.split(","));
				return  (lineSplitList.get(0).equals("Borough")==false && lineSplitList.get(75).equals("")==false && lineSplitList.get(76).equals("")==false) && lineSplitList.get(29).equals("")==false;
			}
		});	
		*/
		//JavaRDD<String> NewYork_data =  Bks_tmp.union(BXs_tmp).union(MNs_tmp).union(QNs_tmp).union(SIs_tmp);
		//String borough = "hdfs://192.168.200.97:9000/Borough";
		String borough = args[0];
		JavaRDD<String> NewYork_data = sc.textFile(borough).filter(new Function<String, Boolean>() {
			public Boolean call(String line)
			{
				List<String> lineSplitList;
				lineSplitList = Arrays.asList(line.split(","));
				return  (lineSplitList.get(0).equals("Borough")==false && lineSplitList.get(75).equals("")==false && lineSplitList.get(76).equals("")==false) && lineSplitList.get(29).equals("")==false;
			}
		});	
        System.out.println("读取纽约数据成功。。。。。。。");
		//我们只关心坐标和土地利用数据。[point,landuse]
	    JavaRDD<Point> NewYork_point = NewYork_data.map(new Function<String,Point> () {
			public Point call(String line) {
				List<String> lineSplitList;
				lineSplitList = Arrays.asList(line.split(","));	
				Point pt = new Point(Double.parseDouble(lineSplitList.get(75)), Double.parseDouble(lineSplitList.get(76)), lineSplitList.get(29));
				return pt;
			}
		});
	    ////////////////建立索引///////////////////////
	    //计算区域范围
	    final double XMin = NewYork_point.min(new XComparator()).getX();
	    final double XMax = NewYork_point.max(new XComparator()).getX();
	    final double YMin = NewYork_point.min(new YComparator()).getY();
	    final double YMax = NewYork_point.max(new YComparator()).getY();
	    //计算每个点的编码值
	    JavaRDD<Integer> hValues_tmp = 	NewYork_point.map(new Function<Point, Integer> (){
			@Override
			public Integer call(Point poi) {
			int gridResolution=Short.MAX_VALUE;
			int x=locationMapping(XMin,XMax,poi.getX());
			int y=locationMapping(YMin,YMax,poi.getY());
			return computeHValue(gridResolution+1,x,y);
			}
		});
	    //转化为数组
	    ArrayList<Integer> tmparr = (ArrayList<Integer>)(hValues_tmp.collect());
	   // List<Point> NewYork_List = NewYork_point.collect();
	    final Integer numPartitions = 3000;  //整个区域将映射到的分块数量,分块数量影响负载均衡，影响可扩展性
		Integer[] splits = new Integer[numPartitions];
		int maxH = 0x7fffffff;
		Integer[] hValues = tmparr.toArray(new Integer[0]);
		Arrays.sort(hValues);
		for (int i = 0; i < splits.length; i++) {
			int quantile = (int) ((long)(i + 1) * hValues.length / numPartitions);
			splits[i] = quantile == hValues.length ? maxH : hValues[quantile];
			
		}
		final Broadcast<Integer[]> brodcast_spilts = sc.broadcast(splits);
		//计算每个点对应的网格ID
		JavaPairRDD<Integer, Point> grids =  NewYork_point.mapToPair(new PairFunction<Point, Integer, Point>(){
			public Tuple2<Integer, Point> call(Point i){
				int [] partitionBounds = new int[numPartitions];
				for(int j=0;j < numPartitions; j++){
					partitionBounds[j] = brodcast_spilts.value()[j];
				}
				int partitionID = gridID(XMin,XMax,YMin,YMax, i, partitionBounds);
				return new Tuple2<Integer, Point>(partitionID, i);
			}	
		});
		////对每一个点做网格化操作，最终形成的是1个网格编号对应多个点的局面
		JavaPairRDD<Integer, Iterable<Point> > grardlized_tmp = grids.groupByKey(numPartitions);
		JavaPairRDD<Integer, ArrayList<Point> >  grardlized = grardlized_tmp.mapToPair(new PairFunction < Tuple2<Integer, Iterable<Point> >, Integer, ArrayList<Point>  > () {
			public Tuple2<Integer, ArrayList<Point> > call( Tuple2<Integer, Iterable<Point> > pair){
				ArrayList<Point> tmparr = new ArrayList<Point>();
				Iterable<Point> tempiter = pair._2();
				Iterator<Point> itf = tempiter.iterator();	
				while(itf.hasNext())
				{
					Point tmpff =itf.next();
					tmparr.add(tmpff);
				}
				return new Tuple2<Integer, ArrayList<Point> > (pair._1(), tmparr); 
			}
		});
	    //在做KNN之前必须广播变量出去
	    //final Broadcast<List<Point>> brosdcast_NewYork =  sc.broadcast(NewYork_List);
		Map<Integer, ArrayList<Point> > borouogh_minvector = (Map<Integer, ArrayList<Point> >)grardlized.collectAsMap();
		Map<Integer,  ArrayList<Point>> borouogh_minvector_tmp = new HashMap<Integer,  ArrayList<Point> >(borouogh_minvector);	
		final	Broadcast< Map<Integer, ArrayList<Point> > > broadcasting_borouogh = sc.broadcast(borouogh_minvector_tmp);
		//////////////////////////索引建立结束/////////////////////////
		
	    //读取taxiTrip文件，坐标位置（23,24,25,27）
	    JavaRDD<String> TaxiData = sc.textFile(args[1]).filter(new Function<String, Boolean>() {
	    	public Boolean call(String line)
			{
				List<String> lineSplitList;
				lineSplitList = Arrays.asList(line.split(","));
				return  (lineSplitList.get(0).equals("FID_")==false && lineSplitList.get(23).equals("")==false && 
					lineSplitList.get(24).equals("")==false &&  lineSplitList.get(25).equals("")==false	&& lineSplitList.get(26).equals("")==false);
			}
		});
	    
	  //记录开始位置和结束位置的pair对 
		JavaPairRDD<Point, Point> TaxiData_point = TaxiData.mapToPair(new PairFunction<String, Point, Point> () {
			public Tuple2<Point, Point> call(String line) {
				List<String> lineSplitList;
				lineSplitList = Arrays.asList(line.split(","));
				Point pt_start = new Point(Double.parseDouble(lineSplitList.get(23)), Double.parseDouble(lineSplitList.get(24)), "");
				Point pt_end = new Point(Double.parseDouble(lineSplitList.get(25)), Double.parseDouble(lineSplitList.get(26)), "");
				
				return new Tuple2<Point ,Point> (pt_start, pt_end);
			}
		});
	    System.out.println("读取Taxi数据成功。。。。。。。");
	///////////////////////////////////////////    
	//KNN算法
	//计算每个TaxiData_point与Newyork的点的距离
	//找出最近的5个点
    //PriorityQueue用于5个点的排序
	//////////////////////////////////////
		JavaPairRDD<String, String> knnResult = TaxiData_point.mapToPair(new PairFunction<Tuple2<Point,Point>, String, String> () {
			public Tuple2<String, String> call(Tuple2<Point,Point> pair) {
				
				Integer k=5; 								//k表示最近点的数量
				//找出离起始点最近的五个point
				PriorityQueue<Point> pq = new PriorityQueue<Point>(k, new PointDistanceComparator(pair._1()));
				//找到索引位置
				int [] partitionBounds = new int[numPartitions];
				for(int j=0;j < numPartitions; j++){
					partitionBounds[j] = brodcast_spilts.value()[j];
				}
				int partitionID1 = gridID(XMin,XMax,YMin,YMax, pair._1(), partitionBounds);
				ArrayList<Point> PointPart1 = broadcasting_borouogh.value().get(partitionID1);
				//进行迭代计算
				Iterator<Point> input =  PointPart1.iterator();
				while (input.hasNext()) {
					if (pq.size() < k) {
						pq.offer(input.next());
					} else {
						Point curpoint = input.next();
						double distance = curpoint.distance(pair._1());
						double largestDistanceInPriQueue = pq.peek().distance(pair._1());
						if (largestDistanceInPriQueue > distance) {
							pq.poll();
							pq.offer(curpoint);
						}
					}
				}
	            //记录最近5个点
				ArrayList<Point> res1 = new ArrayList<Point>();
				for (int i = 0; i < k; i++) 
				{
					res1.add(pq.poll());
				}
				
				//找出离终点最近的五个point
				PriorityQueue<Point> pq2 = new PriorityQueue<Point>(k, new PointDistanceComparator(pair._2()));
				//找出索引位置
				int partitionID2 = gridID(XMin,XMax,YMin,YMax, pair._2(), partitionBounds);
				ArrayList<Point> PointPart2 = broadcasting_borouogh.value().get(partitionID2); 
				//进行迭代计算
				Iterator<Point> input2 =  PointPart2.iterator();
				while (input2.hasNext()) {
					if (pq2.size() < k) {
						pq2.offer(input2.next());
					} else {
						Point curpoint = input2.next();
						double distance = curpoint.distance(pair._2());
						double largestDistanceInPriQueue = pq2.peek().distance(pair._2());
						if (largestDistanceInPriQueue > distance) {
							pq2.poll();
							pq2.offer(curpoint);
						}
					}
				}
				ArrayList<Point> res2 = new ArrayList<Point>();
				for (int i = 0; i < k; i++) {
					res2.add(pq2.poll());
				}
				
				return new Tuple2<String ,String> (res1.get(0).getLanduse(),res2.get(0).getLanduse());
			}
		}); 	
		knnResult.saveAsTextFile(args[2]);
		System.out.println("count : "+knnResult.count());
		sc.close();
	}
	
	

}

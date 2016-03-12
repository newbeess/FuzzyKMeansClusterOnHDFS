package com.elephant.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * 用于读取将要聚类的数据
 */
public class ReadGroupLensData {
	/**
	 * 从本地读取GroupLens电影数据u.item
	 *
	 * @param 	filePath	String		数据文件路径
	 * @return 	points		double[][]	二维数组
	 */
	public  double[][] getItemVectorFromLocal(String filePath){
		double[][] points = new double[1682][19];
		try {
			Scanner scanner = new Scanner(new FileReader(filePath));//如果new File()只读540行，不知道为什么
			int i=0;
			while (scanner.hasNext()){
				String line=scanner.nextLine();
				String[]words=line.split("([|]+)");
				for (int j=0;j<words.length-4;j++){
					points[i][j]=Double.parseDouble(words[j+4]);
				}
				i++;
			}
		} catch(IOException e){
			System.out.println("Error reading file '" + filePath + "'");
		}
		return points;
	}

	/**
	 *  * 从HDFS中读取GroupLens电影数据u.item
	 *
	 * @param coreFilePath	Pth			core-site.xml的位置
	 * @param path			Path		文件路径
	 *
	 * @return	points		double[][]
	 */
	public double[][] getItemVectorFromHDFS(Path coreFilePath,Path path) throws IOException{
		double[][] points=new double[1682][19];
		// 1. create FileSystem Object
		Configuration conf=new Configuration();
		conf.addResource(coreFilePath);
		FileSystem fs=FileSystem.get(conf);
		// 2.判断文件是否存在
		if (!fs.exists(path)){
			System.out.println("File doesn't exists!");
		}
		// 3. 读取文件数据
		FSDataInputStream in=fs.open(path);
		String line;
		int i=0;
		while ((line=in.readLine()) != null){
			String[]words=line.split("([|]+)");
			for (int j=0;j<words.length-4;j++){
				points[i][j]=Double.parseDouble(words[j+4]);
			}
			i++;
		}
		in.close();

		return points;
	}

	/**
	 * 将得二维数组转换成List
	 *
	 * @param	points	double[][]
	 * @return	list	List<Vector>
	 */
	public  List<Vector> getPoints(double[][] points) {
		List<Vector> list = new ArrayList<Vector>();
		for (int i = 0; i < points.length; i++) {
			double[] row = points[i];
			Vector vector = new RandomAccessSparseVector(row.length);
			vector.assign(row);
			list.add(vector);
		}
		return list;
	}

	public static void main(String[] args) throws IOException{
		ReadGroupLensData rg=new ReadGroupLensData();
		Path coreFilePath=new Path("/Users/elephant/dev/hadoop/hadoop-2.6.0/etc/hadoop/core-site.xml");
		Path path=new Path("/FuzzyKMeans/GroupLensData/u.item");
		double[][] points=rg.getItemVectorFromHDFS(coreFilePath,path);
		List<Vector> list_Vector_GroupLens=rg.getPoints(points);

		for (Vector vector : list_Vector_GroupLens)
			System.out.println(vector);

	}
}

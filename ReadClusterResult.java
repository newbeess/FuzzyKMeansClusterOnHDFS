package com.elephant.test;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import java.io.*;
import java.util.*;

/**
 * 用于读取聚类结果
 */
public class ReadClusterResult {
	/**
	 * 获取模糊聚类结果中的Vector向量
	 *
	 * @param clusterPointsPath	Path  				模糊聚类输出文件目录： clusteredPoints/part-m-00000
	 * @return arrayList		ArrayList<Vector>
	 */
	public  ArrayList<Vector> getClusterVector(Path clusterPointsPath) {

		ArrayList<Vector> arrayList = new ArrayList<Vector>();
		try {
			Configuration config = new Configuration();
			SequenceFile.Reader reader = new SequenceFile.Reader(config, SequenceFile.Reader.file(clusterPointsPath));
			IntWritable key = new IntWritable();
			WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
			double[] array=new double[19];
			while (reader.next(key, value)) {
				for (int i = 0; i < value.getVector().size(); i++) {
					array[i]=value.getVector().get(i);
				}
				Vector vector=new RandomAccessSparseVector(value.getVector().size());
				vector.assign(array);
				arrayList.add(vector);
			}
			reader.close();
		} catch (IOException e) {

		}
		return arrayList;
	}
	/**
	 * 根据给定的Vector，从聚类结果中查询该向量所属的 Cluster_id 和 Weight
	 *
	 * @param 	clusterPointsPath	Path 				模糊聚类输出文件目录： clusteredPoints/part-m-00000
	 * @param 	vector				mahout.math.Vector	需要查询到向量
	 * @return 	hashSet				HashSet<String>
	 */
public  HashSet<String> getClusterIdAndWeight(Path clusterPointsPath,Vector vector){
	HashSet<String> hashSet=new HashSet<String>();
	try {
		Configuration config = new Configuration();
		SequenceFile.Reader reader = new SequenceFile.Reader(config, SequenceFile.Reader.file(clusterPointsPath));
		IntWritable key = new IntWritable();
		WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
		while (reader.next(key, value)) {
			Vector vector1=value.getVector();
			if (vector1.equals(vector)){
				String idAndWeight=key.get()+","+value.getWeight();
				hashSet.add(idAndWeight);
			}
		}
		reader.close();
	} catch (IOException e) {
			System.out.print(e+"In ReadClusterResult.java getClusterIdAndWeight");
	}
	return hashSet;
}

	/**
	 *  * 建立 vector -> cid,weight 的索引
	 *
	 * @param	clusterPointsPath	模糊聚类输出文件目录： clusteredPoints/part-m-00000
	 * @return	hashMap				HashMap<Vector,HashSet<String>>
	 */

	public HashMap<Vector,HashSet<String>> vectorToCID_WeightIndex(Path clusterPointsPath){
		HashMap<Vector,HashSet<String>> hashMap=new HashMap<Vector, HashSet<String>>();
		try {
			Configuration config = new Configuration();
			config.addResource(	new Path("/Users/elephant/dev/hadoop/hadoop-2.6.0/etc/hadoop/core-site.xml"));
			SequenceFile.Reader reader = new SequenceFile.Reader(config, SequenceFile.Reader.file(clusterPointsPath));
			IntWritable key = new IntWritable();
			WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
			while (reader.next(key, value)) {
				HashSet<String> hashSet=new HashSet<String>();
				Vector vector=value.getVector();
				String idAndWeight=key.get()+","+value.getWeight();
				if (hashMap.containsKey(vector)){
					HashSet<String> hashSet1=hashMap.get(vector);
					hashSet1.add(idAndWeight);
					hashMap.put(vector,hashSet1);
				}
				else {
					hashSet.add(idAndWeight);
					hashMap.put(vector,hashSet);
				}
			}
			reader.close();
		} catch (IOException e) {
			System.out.print(e+"In ReadClusterResult.java vectorToCID_WeightIndex");
		}

		return hashMap;
	}



	public static void main(String[] args){
		ReadClusterResult rcr=new ReadClusterResult();
		Path path = new Path("clustering/fuzzykmeansoutput/clusteredPoints/part-m-00000");

		ArrayList<Vector> list=rcr.getClusterVector(path);
		int count=0;
		for (Vector vector : list)
			count++;
		System.out.println("聚类后向量的数量为： "+count);

		Vector vector=list.get(1);

		HashSet<String> id=rcr.getClusterIdAndWeight(path, vector);

		for (String s : id){
			System.out.print(s+";;;");
		}
	}
}

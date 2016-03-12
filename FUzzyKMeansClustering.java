package com.elephant.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.clustering.fuzzykmeans.FuzzyKMeansDriver;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * 对Grouplens中的电影属性模糊聚类
 * 将	item_id <-> cluster_id	<->	weight	的对应关系写入HDFS文件系统中
 */

public class FUzzyKMeansClustering {

	/**
	 * 将vectors以向量的形式写入HDFS
	 *
	 * @param vectors	List<Vector>	电影属性向量
	 * @param path		Path			将要写入的文件位置
	 * @param fs		FileSystem
	 * @param conf		Configuration
	 * @throws IOException
	 */
	public static void writePointsToHDFS(List<Vector> vectors, Path path, FileSystem fs, Configuration conf) throws IOException {
        SequenceFile.Writer writer =SequenceFile.createWriter(conf, SequenceFile.Writer.file(path),
				SequenceFile.Writer.keyClass(IntWritable.class),SequenceFile.Writer.valueClass(VectorWritable.class));
        int recNum = 0;
        VectorWritable vec = new VectorWritable();
        for (Vector point : vectors) {
            vec.set(point);
            writer.append(new IntWritable(recNum++), vec);
        }
        writer.close();
    }


	/**
	 * 	主函数
	 *
	 * @param args	null
	 * @throws Exception
	 */

    public static void main(String args[]) throws Exception {
		long beginTime=System.currentTimeMillis();
		/** 第一步: 读取u.item中的数据		*/
		Path coreFilePath=new Path("/Users/elephant/dev/hadoop/hadoop-2.6.0/etc/hadoop/core-site.xml");
		Path dataPath=new Path("/FuzzyKMeans/GroupLensData/u.item");
		ReadGroupLensData readGroupLens=new ReadGroupLensData();
		final double[][] points= readGroupLens.getItemVectorFromHDFS(coreFilePath,dataPath);
        List<Vector> vectors = readGroupLens.getPoints(points);


		/**	第二步: 将数据以向量的形式写入HDFS	*/
		Configuration conf=new Configuration();
		conf.addResource(coreFilePath);
		FileSystem fs=FileSystem.get(conf);
		Path vectorPath=new Path("/FuzzyKMeans/ItemVector/vector");

		FSDataOutputStream out=fs.create(vectorPath);
		writePointsToHDFS(vectors, vectorPath, fs, conf);

// initialize cluster
		Path canopyinput=new Path("hdfs://localhost:9000/FuzzyKMeans/ItemVector/vector");
		//Path vectorwritablefile=new Path("hdfs://localhost:9000/fuzzykmeans/vectorwritable/part-r-00000");
		Path canopyoutput=new Path("hdfs://localhost:9000/FuzzyKMeans/Canopyoutput/");
		EuclideanDistanceMeasure measure=new EuclideanDistanceMeasure();
		double t1=4.1;
		double t2=3.0;
		boolean overwrite=true;
		boolean runSequential=true;
		CanopyDriver.run(canopyinput, canopyoutput, measure, t1, t2, overwrite, 0.01, runSequential);
// Run FuzzyKMeans
		Path fuzzykmeansinputdataset=new Path("hdfs://localhost:9000/FuzzyKMeans/ItemVector/vector");
		Path fuzzykmeansinitialcluster=new Path("hdfs://localhost:9000/FuzzyKMeans/Canopyoutput/clusters-0-final/");
		Path fuzzykmeansoutput=new Path("hdfs://localhost:9000/FuzzyKMeans/FuzzyKMeansoutput/");
		double convergence=0.001;
		int max_iterations=10;
		float fuzzy_factor=1.1f;
		double threshold=0.01;
		boolean runCluster=false;
		FuzzyKMeansDriver.run(fuzzykmeansinputdataset,fuzzykmeansinitialcluster,fuzzykmeansoutput,convergence,max_iterations,fuzzy_factor,
				true,
				false,
				threshold,
				runCluster);
//
		Path file=new Path("hdfs://localhost:9000/FuzzyKMeans/Result");
		WriteResultToFile wr=new WriteResultToFile();
		HashMap<Integer,HashSet<String>> hashMap=wr.getData();
		wr.writeDataToHDFS(coreFilePath,file,hashMap);

		long endTime=System.currentTimeMillis();
		System.out.println("程序运行时间："+(endTime-beginTime)/1000.0+"秒");
	}
}
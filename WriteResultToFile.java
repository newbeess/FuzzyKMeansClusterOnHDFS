package com.elephant.test;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Vector;

/**
 * 先从u.item中，按照顺序读取向量，在clusteredPoints中，寻找对应的cluster_id
 *
 * 读取sequencefile文件，并将读取的内容以HashMap的方式写入文件
 * 写入的规则为:(每一行的形式)
 * item_id,(cluster_id,weight),(cluster_id,weight)...
 * item_id,(cluster_id,weight),(cluster_id,weight)...
 * key=item_id
 * value=Set<String>
 *    String=cluster_id,weight
 */

public class WriteResultToFile {

	/**
	 * 通过比较，得到将要写入文件的数据
	 *
	 * @return item_cluster_weight	HashMap
	 */
	public  HashMap<Integer,HashSet<String>> getData() throws IOException{

		HashMap<Integer, HashSet<String>> item_cluster_weight = new HashMap<Integer, HashSet<String>>();

			// 1.读取源文件u.item,
			// item_vector ==> list_Item_GroupLens
			ReadGroupLensData rgd=new ReadGroupLensData();
			Path coreFilePath=new Path("/Users/elephant/dev/hadoop/hadoop-2.6.0/etc/hadoop/core-site.xml");
			Path path=new Path("hdfs://localhost:9000/FuzzyKMeans/GroupLensData/u.item");
			double[][] item_GroupLens = rgd.getItemVectorFromHDFS(coreFilePath,path);
			List<Vector> list_Vector_GroupLens=rgd.getPoints(item_GroupLens);

			// 2.读取clusteredPoints文件中的数据
			// 建立 vector -> cid_weight的索引
			ReadClusterResult rcr=new ReadClusterResult();
			Path path1 = new Path("hdfs://localhost:9000/FuzzyKMeans/FuzzyKMeansoutput/clusteredPoints/part-m-00000");
			HashMap<Vector,HashSet<String>> index=rcr.vectorToCID_WeightIndex(path1);
			ArrayList<Vector> list_Vector_Cluster=rcr.getClusterVector(path1);


			// 3.比较
			//	每一个grouplens的向量，循环一遍cluster向量
			int item_id=0;
			for (Vector vector_G : list_Vector_GroupLens){
				item_id=item_id+1;
				for (Vector vector_C : list_Vector_Cluster){
					if (vector_G.equals(vector_C)){
						HashSet hashSet=index.get(vector_C);
						item_cluster_weight.put(item_id,hashSet);
					}
				}
			}

		return item_cluster_weight;
	}

	/**
	 * 将HashMap写入本地
	 * @param hashMap	HashMap	将要写入的数据
	 */
	public void writeDataToLocal(HashMap<Integer,HashSet<String>> hashMap) throws IOException{
		//	第一种写入方式
		File fout = new File("out.txt");
		FileOutputStream fos = new FileOutputStream(fout);

		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
		for (HashMap.Entry entry : hashMap.entrySet()){
			bw.write(entry.getKey()+"="+entry.getValue());
			bw.newLine();
		}

		bw.close();
		//	第二种写入方式
		FileWriter fw=new FileWriter("out1.txt");
		for (HashMap.Entry entry : hashMap.entrySet())
			fw.write(entry.getKey()+"="+entry.getValue()+"\n");
		fw.close();

	}

	/**
	 * 将HashMap写入HDFS
	 *
	 * @param coreFilePath 	Path	coreFilePath
	 * @param outputPath	Path	outputPath
	 * @param hashMap		HashMap	Data will be written
	 * @throws IOException
	 */

	public  void  writeDataToHDFS(Path coreFilePath,Path outputPath,HashMap<Integer,HashSet<String>> hashMap) throws IOException{

		// 1. create FileSystem Object
		Configuration conf=new Configuration();
		conf.addResource(coreFilePath);
		FileSystem fs=FileSystem.get(conf);
		// 2. check if already exist
		if (fs.exists(outputPath)){
			System.out.println("Output already exists");
			System.exit(1);
		}
		//3. write to file

		FSDataOutputStream out=fs.create(outputPath);
		for (HashMap.Entry entry : hashMap.entrySet()){
			out.writeBytes(entry.getKey()+"="+entry.getValue()+"\n");
		}
		out.close();
	}


	public static void main(String[] args) throws IOException,IllegalAccessException,InstantiationException{
		WriteResultToFile wr=new WriteResultToFile();
		HashMap<Integer,HashSet<String>> hashMap=wr.getData();
	//	for (Map.Entry entry : hashMap.entrySet()){
	//		System.out.println(entry.getKey()+",,,"+entry.getValue());
	//	}
	//	Path path=new Path("hdfs://localhost:9000/input_txt/text.txt");
	//	wr.writeDataToHDFS(path,hashMap);
		Path coreFilePath=new Path("/Users/elephant/dev/hadoop/hadoop-2.6.0/etc/hadoop/core-site.xml");
		Path outputPath=new Path("/input_txt/a.txt");
		wr.writeDataToLocal(hashMap);
		wr.writeDataToHDFS(coreFilePath,outputPath,hashMap);
	}
}


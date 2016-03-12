package com.elephant.test;

// 从u.item中读取后19列到points数组中，为聚类使用


import java.io.*;
import java.util.*;

public class ReadData {
	public static double[][] readData(){
		String file = "./data/u.item";
		double[][] points = new double[1682][19];
		try {
			Scanner scanner = new Scanner(new FileReader(file));//如果new File()只读540行，不知道为什么
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
			System.out.println("Error reading file '" + file + "'");
		}
		return points;
	}

	public static void main(String[] args) throws IOException {
		ReadData read=new ReadData();
		double points[][]=read.readData();
		for (int i=0;i<points.length;i++){
			for (int j=0;j<points[i].length;j++)
				System.out.print(points[i][j] + " ");
			System.out.println();
		}
			System.out.println(points.length+", "+points[5].length);


	}
}
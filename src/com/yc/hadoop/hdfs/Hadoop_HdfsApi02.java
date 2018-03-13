package com.yc.hadoop.hdfs;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * 获取hdfs指定路径下的文件信息
 * @company 源辰信息
 * @author navy
 */
public class Hadoop_HdfsApi02 {
	private static Logger log = Logger.getLogger(Hadoop_HdfsApi02.class); // 创建日志记录器

	public static void main(String[] args) {
		FileSystem fs = null;
		try {

			Configuration conf = new Configuration();// 加載配制文件
			URI uri = new URI("hdfs://192.168.30.130:9000/"); //要连接的资源位置

			fs = FileSystem.get(uri,conf,"navy");  //创建文件系统实例对象


			//FileStatus[] files = fs.listStatus(new Path("/input/"));  // 列出文件
			FileStatus[] files = fs.listStatus(new Path("/user/navy/"));  // 列出文件，hadoop的hdfs的根目录
			System.out.println("当前目录下的文件信息如下：");
			for (FileStatus f : files) {
				System.out.println("\t" + f.getPath().getName());
			}
		} catch (Exception e) {
			log.error("hdfs操作失败!!!", e);
		}
	}
}

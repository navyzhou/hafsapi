package com.yc.hadoop.hdfs;

import java.net.URI;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * 修改hdfs上指定文件的文件名
 * @company 源辰信息
 * @author navy
 */
public class Hadoop_HdfsApi03 {
	private static Logger log = Logger.getLogger(Hadoop_HdfsApi03.class); // 创建日志记录器

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		Scanner input = new Scanner(System.in);
		FileSystem fs = null;

		try {

			Configuration conf = new Configuration();// 加載配制文件
			URI uri = new URI("hdfs://192.168.30.130:9000/"); // 要连接的资源位置

			fs = FileSystem.get(uri,conf,"navy");  // 创建文件系统实例对象

			FileStatus[] files = fs.listStatus(new Path("/user/navy/"));  // 列出文件，hadoop的hdfs的根目录
			System.out.println("当前目录下的文件信息如下：");
			for (FileStatus f : files) {
				System.out.println("\t" + f.getPath().getName());
			}
			
			// 修改文件
			System.out.print("请输入要修改的文件的文件名：");
			Path file = new Path(input.next()); // hadoop文件对象, 类似java的File类 
			
			System.out.print("请输入修改后的文件名："); 
			Path newName = new Path(input.next());
			
			// 修改名字
			boolean isResult = fs.rename(file,newName); 
			
			log.debug("修改文件名" + (isResult ? "成功..." :	"失败！！！") );
		} catch (Exception e) {
			log.error("hdfs操作失败!!!", e);
		}
	}
}

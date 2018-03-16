package com.yc.hadoop.hdfs;

import java.net.URI;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * 上传本地文件到hdfs上
 * @company 源辰信息
 * @author navy
 */
public class Hadoop_HdfsApi07 {
	private static Logger log = Logger.getLogger(Hadoop_HdfsApi07.class);  // 创建日志记录器

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		Scanner input = new Scanner(System.in);
		FileSystem fs = null;
		try {
			Configuration conf = new Configuration(); // 加载配置文件
			URI uri = new URI("hdfs://192.168.30.130:9000/");   // 连接资源位置
			fs = FileSystem.get(uri, conf); // 创建文件系统实例对象
			
			Path uploadPath = new Path("/user/navy/"); // 上传文件保存的目录
		
			
			System.out.print("请输入要上传的文件：");
			Path p = new Path(input.next());
			
			fs.copyFromLocalFile(p,uploadPath); // 上传文件到指定目录
			log.debug("上传成功....");
			
			FileStatus[] files = fs.listStatus(uploadPath);  // 列出文件
			System.out.println("上传目录中的文件：");
			
			for (FileStatus f : files) {
				System.out.println("\t" + f.getPath().getName());
			}
		} catch (Exception e) {
			log.error("hdfs操作失败!!!", e);
		}
	}
}

package com.yc.hadoop.hdfs;

import java.net.URI;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * 下载hdfs上的指定文件
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
			URI uri = new URI("hdfs://192.168.30.130:9000/");  // 连接资源位置
			fs = FileSystem.get(uri, conf ,"navy");  // 创建文件系统实例对象

			FileStatus[] files = fs.listStatus(new Path("/input/"));  // 列出文件
			
			System.out.println("可以选择的文件名：");
			for (FileStatus f : files) {
				System.out.println("\t" + f.getPath().getName());
			}
			System.out.print("请输入要下载的文件名：");
			Path p= new Path("/input/"+input.next());
			
			System.out.print("下载文件存放目录：");
			Path dst=new Path(input.next());
			
			// 注意：此时必须在window里面配置hadoop环境，必须定位winutils.exe文件
			fs.copyToLocalFile(false,p,dst,true); // 最后一个参数表示不用原始的本地文件系统，改用java的io流 
			
			log.debug("下载成功....");
			
		} catch (Exception e) {
			log.error("hdfs操作失败!!!", e);
		}
	}
}

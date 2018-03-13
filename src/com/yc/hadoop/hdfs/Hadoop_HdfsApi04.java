package com.yc.hadoop.hdfs;

import java.net.URI;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * 删除hdfs上指定文件的文件名
 * @company 源辰信息
 * @author navy
 */
public class Hadoop_HdfsApi04 {
	private static Logger log = Logger.getLogger(Hadoop_HdfsApi04.class); // 创建日志记录器

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		Scanner input = new Scanner(System.in);
		FileSystem fs = null;

		try {

			Configuration conf = new Configuration();// 加載配制文件
			URI uri = new URI("hdfs://192.168.30.130:9000/"); // 要连接的资源位置

			fs = FileSystem.get(uri,conf,"navy");  // 创建文件系统实例对象

			FileStatus[] files = fs.listStatus(new Path("/user/navy/"));  // 列出文件，hadoop的hdfs的根目录
			if(files==null || files.length<=0){
				System.out.println("该目录下没有任何文件...");
				return;
			}
			
			System.out.println("当前目录下的文件信息如下：");
			for (FileStatus f : files) {
				System.out.println("\t" + f.getPath().getName());
			}
			
			String fileName;
			
			while(true){
				System.out.print("请输入要删除的文件的文件名：");
				fileName=input.next();
				Path path= new Path(fileName);
			
				if( fs.delete(path, false) ){  // 文件删除文件
					log.debug("文件 "+fileName+" 删除成功...");
					break;
				}else{
					log.debug("文件 "+fileName+" 不存在, 请重新输入...");
				}
			}
		} catch (Exception e) {
			log.error("hdfs操作失败!!!", e);
		}
	}
}

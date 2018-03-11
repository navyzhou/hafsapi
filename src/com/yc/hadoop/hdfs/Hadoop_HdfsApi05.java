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
public class Hadoop_HdfsApi05 {
	private static Logger log = Logger.getLogger(HdfsApi03.class); // 创建日志记录器

	public static void main(String[] args) {
		Scanner input = new Scanner(System.in);
		FileSystem fs = null;
		try {
			// 加載配制文件
			Configuration conf = new Configuration();
			URI uri = new URI("hdfs://192.168.0.234:9000/"); // 连接资源位置
			fs = FileSystem.get(uri, conf); // 创建文件系统实例对象

			FileStatus[] files = fs
					.listStatus(new Path("/user/Administrator/"));  //列出文件
			System.out.println("可以选择的文件名：");
			for (FileStatus f : files) {
				System.out.println("\t" + f.getPath().getName());
			}

			System.out.print("请输入要输入要查看的文件名：");
			Path p = new Path(input.next());

			System.out.println("要查看的文件路径" + fs.getFileStatus(p).getPath());
			//不可以java文件流读取数据hdfs上的数据
			//	System.out.println(fs.getFileStatus(p).getPath().toUri());
			//	File remoteFile = new File(fs.getFileStatus(p).getPath().toUri());
			//	InputStream in = new FileInputStream(remoteFile);
			//	FSDataInputStream fsin = new FSDataInputStream(in);

			FSDataInputStream fsin = fs.open(fs.getFileStatus(p).getPath());
			byte[] bs = new byte[1024 * 1024];
			int len = 0;
			while((len = fsin.read(bs)) != -1){
				System.out.print(new String(bs, 0, len));
			}
			System.out.println();
			fsin.close();
			input.close();
		} catch (Exception e) {
			log.error("hdfs操作失败!!!", e);
		}
	}
}

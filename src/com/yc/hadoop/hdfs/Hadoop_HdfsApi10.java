package com.yc.hadoop.hdfs;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * 获取、显示指定的文件信息
 * @company 源辰信息
 * @author navy
 */
public class Hadoop_HdfsApi10 {
	private static Logger log = Logger.getLogger(Hadoop_HdfsApi10.class);  // 创建日志记录器

	public static void main(String[] args) {
		FileSystem fs = null;
		try {
			Configuration conf = new Configuration();  // 加载配置文件
			URI uri = new URI("hdfs://192.168.30.130:9000/"); // 连接资源位置
			fs = FileSystem.get(uri, conf); // 创建文件系统实例对象

			Path file = new Path("yc.txt");
			FileStatus fstatus = fs.getFileStatus(file);

			//获取文件的块对象实例集合
			BlockLocation[] bls = fs.getFileBlockLocations(file, 0, fstatus.getLen());

			System.out.println("块的数量 : " + bls.length);
			String[] hosts;
			String[] names;
			String[] paths;
			for (BlockLocation bl : bls) {
				hosts = bl.getHosts();
				names = bl.getNames();
				paths = bl.getTopologyPaths();

				for (int i = 0,len = paths.length; i < len; i++) {
					// 文件的块对象实例的信息
					System.out.println("\t" + hosts[i] + "\t" + names[i] + "\t" + paths[i] + "\t" +  bl.getLength() + "\t" + bl.getOffset());
				}
			}
		} catch (Exception e) {
			log.error("hdfs操作失败!!!", e);
		}
	}
}

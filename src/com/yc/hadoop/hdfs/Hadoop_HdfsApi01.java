package com.yc.hadoop.hdfs;

import java.net.URI;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

/**
 * 文件的创建以及向文件中写入内容
 * @company 源辰信息
 * @author navy
 */
public class Hadoop_HdfsApi01 {
	public static void main(String[] args) {
		Logger log = Logger.getLogger(Hadoop_HdfsApi01.class); // 创建日志记录器
		
		try {
			// 加載配制文件
			Configuration conf = new Configuration();
			URI uri = new URI("hdfs://192.168.30.130:9000/"); // 连接资源位置
			FileSystem fs = FileSystem.get(uri, conf); // 创建文件系统实例对象

			Path file = new Path("test"); // hadoop文件对象, 类似java的File类

			if (fs.exists(file)) { // 判断文件是否存在
				log.debug(file + "存在...");
				//文件存在， 换一个名字
				file = new Path(file.getName() + "_" + System.currentTimeMillis());
			} else {
				log.debug(file + "不存在!!!");
			}
			log.debug("创建文件" + file + "成功...");
			
			// 创建文件
			FSDataOutputStream fdos = fs.create(file, new Progressable() {
				@Override
				public void progress() {
					// 执行进度显示
					System.out.print(">>");
				}
			}); // 文件系统数据输出流
			
			Scanner input = new Scanner(System.in);  // 扫描器对象
			System.out.print("请输入内容:");
			String word = input.nextLine();
			fdos.write(word.getBytes());  // 向文件系统中的文件中写入内容
			fdos.flush();
			fdos.close();
			input.close();
			//查看文件指令 hadoop fs -ls  可以看到这个下面有test文件
			//查看文件内容 hadoop fs -cat test  可以看到文件中的内容
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

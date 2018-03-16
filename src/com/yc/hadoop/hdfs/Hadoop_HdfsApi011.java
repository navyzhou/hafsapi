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
public class Hadoop_HdfsApi011 {
	public static void main(String[] args) {
		Logger log = Logger.getLogger(Hadoop_HdfsApi011.class); // 创建日志记录器

		try {
			Configuration conf = new Configuration(); // 加载配置文件
			URI uri = new URI("hdfs://192.168.30.130:9000/");  // 连接资源位置
			FileSystem fs = FileSystem.get(uri, conf); // 创建文件系统实例对象

			// 创建目录   删除目录  hadoop fs -rm -f <目录名>
			Path dir = new Path("files"); // 要创建的目录。Path为hadoop文件对象, 类似java的File类

			if (fs.exists(dir)) { // 判断文件是否存在
				log.debug(dir + " 存在...");
				//文件存在， 换一个名字
			} else {
				log.debug(dir + " 不存在，开始创建...");
				if (fs.mkdirs(dir)){
					log.debug(dir + " 目录创建成功...");
				} else {
					log.debug(dir + " 目录创建失败...");
				}
			}
			
			// 创建文件
			Path file = new Path(dir, "data.txt");
			if (fs.exists(file)) { // 文件是否存在
				log.debug(file + " 文件存在...");
			} else {
				if (fs.createNewFile(file)) { // 创建文件
					log.debug(file + " 文件创建成功...");
				} else {
					log.debug(file + " 文件创建失败...");
				}
			}

			// 在文件中添加 内容
			FSDataOutputStream fdos = null; // 文件系统数据输出流
			
			if (fs.exists(file)){ // 如果文件存在
				fdos = fs.append(file, 4096, new Progressable() {
					@Override
					public void progress() {
						System.out.println(">>"); // 进度提示
					}
				});
			} else {  // 文件不存在
				fdos = fs.create(file, true, 4096, new Progressable() {
					@Override
					public void progress() {
						System.out.println("..."); // 进度提示
					}
				});
			}
			
			// InputStream in = new FileInputStream("yc.txt");
			// IOUtils.copyBytes(in, out, 4096, true);

			Scanner input = new Scanner(System.in);  // 扫描器对象
			System.out.print("请输入内容:");
			String word = input.nextLine();
			fdos.write(word.getBytes());  // 向文件系统中的文件中写入内容
			fdos.flush();
			fdos.close();
			input.close();
			System.out.println("写入数据完成...");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

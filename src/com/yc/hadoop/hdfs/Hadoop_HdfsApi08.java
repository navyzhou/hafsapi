package com.yc.hadoop.hdfs;

import java.net.URI;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.swing.text.NumberFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * 获取、显示指定的文件信息
 * @company 源辰信息
 * @author navy
 */
public class Hadoop_HdfsApi08 {
	private static Logger log = Logger.getLogger(Hadoop_HdfsApi08.class);  // 创建日志记录器

	public static void main(String[] args) {
		FileSystem fs = null;
		
		try {
			Configuration conf = new Configuration(); // 加载配置文件
			URI uri = new URI("hdfs://192.168.30.130:9000/");   // 连接资源位置
			
			fs = FileSystem.get(uri, conf);    // 创建文件系统实例对象
			Path file = new Path("yc.txt"); // 创建文件对象

			FileStatus fstatus = fs.getFileStatus(file);  
			
			Path filePath = fstatus.getPath();   // 获取文件路径
			long size = fstatus.getLen();  // 获取文件大小
			boolean isdir = fstatus.isDirectory(); // 是否是目录
			short blockReplication = fstatus.getReplication(); // 获取文件的副本数
			long blocksize = fstatus.getBlockSize(); // 获取文件的数据块大小
			long modificationTime = fstatus.getModificationTime(); // 获取文件的最后修改时间
			long accessTime = fstatus.getAccessTime();  // 获取文件的最后访问时间
			String premission = fstatus.getPermission().toString(); // 文本的权限
			String owner = fstatus.getOwner(); // 文本的拥有者
			String group = fstatus.getGroup(); // 文件所属组
			
			// Path symlink = fstatus.getSymlink(); // 文件连接

			System.out.println("文件路径  : " + filePath 
					+ "\n 文件大小  : " + sizeToConvert(size) 
					+ "\n 是否是目录  : " + isdir 
					+ "\n 文件副本数  : " + blockReplication
					+ "\n 数据块大小  : "	 + sizeToConvert(blocksize)
					+ "\n 最后修改时间  : " + timeToConvert(modificationTime)
					+ "\n 最后访问时间  : " + timeToConvert(accessTime)
					+ "\n 文件权限 : " + premission 
					+ "\n 文件拥有者 : " + owner 
					+ "\n 文件所属组 : " + group );

		} catch (Exception e) {
			log.error("hdfs操作失败!!!", e);
		}
	}

	/**
	 * 日期格式转换器
	 * @param time
	 * @return
	 */
	private static String timeToConvert(long time) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sdf.format(new Date(time));
	}

	/**
	 * 文件大小格式转换器
	 * @param size
	 * @return
	 */
	private static String sizeToConvert(long size) {
		// 1.小于1024 , 单位为 B
		// 2. 大于1024, 小于1024 * 1024 , 单位为 K
		// 3. 大于1024 * 1024, 小于1024 * 1024 * 1024 , 单位为M
		// 4. 大于1024 * 1024 * 1024, 小于1024 * 1024 * 1024 * 1024 , 单位为G
		// 5. 大于1024 * 1024 * 1024 * 1024, 小于1024 * 1024 * 1024 * 1024 * 1024 ,
		// 单位为T
		// 7. 大于1024 * 1024 * 1024 * 1024 * 1024, 小于1024 * 1024 * 1024 * 1024 *
		// 1024 * 1024 , 单位为P
		// 6. 大于1024 * 1024 * 1024 * 1024 * 1024 * 1024 , 单位为E
		NumberFormatter nf = new NumberFormatter(new DecimalFormat("#.##"));
		try {
			if (size < 1024) {
				return size + "B";
			} else if (size < Math.pow(1024, 2)) {

				return nf.valueToString(size / Math.pow(1024, 1)) + "KB";

			} else if (size < Math.pow(1024, 3)) {
				return size / Math.pow(1024, 2) + "MB";
			} else if (size < Math.pow(1024, 4)) {
				return size / Math.pow(1024, 3) + "GB";
			} else if (size < Math.pow(1024, 5)) {
				return size / Math.pow(1024, 4) + "TB";
			} else if (size < Math.pow(1024, 6)) {
				return size / Math.pow(1024, 5) + "PB";
			} else {
				return size / Math.pow(1024, 6) + "EB";
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
}

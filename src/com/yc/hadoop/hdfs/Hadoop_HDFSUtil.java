package com.yc.hadoop.hdfs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

/**
 * 获取、显示指定的文件信息
 * @company 源辰信息
 * @author navy
 */
public class Hadoop_HDFSUtil {
	private static final Logger LOG = Logger.getLogger(Hadoop_HDFSUtil.class); // 日志记录器对象
	private static FileSystem fileSystem; // 文件系统对象

	static {
		Configuration conf = new Configuration(); // hadoop的配制文件对象
		URI uri = URI.create("hdfs://master:9000/"); // hadoop分布式文件系统master所在的uri
		try {
			fileSystem = FileSystem.get(uri, conf, "hadoop"); // 根据配制和远程地址创建文件系统
			LOG.debug("创建文件系统对象成功...");
		} catch (IOException | InterruptedException e) {
			LOG.error("创建文件系统对象失败！！！", e);
		}
	}

	public static FileSystem getFileSystem() {
		return fileSystem;
	}

	// 查询操作
	/**
	 * 打开Hdfs上的文件， 导出内容到指定位置
	 * 
	 * @param inHdfsPath  Hdfs上的文件
	 * @param out 导出的指定位置
	 */
	public static void readFile(String inHdfsPath, OutputStream out) {
		// 操作系统中的文件是 File, 文件系统中的文件 Path
		FSDataInputStream in = null;
		try {
			in = fileSystem.open(new Path(inHdfsPath)); // 打开文件取到文件数据流
			LOG.debug("打开文件系统上的文件成功...");
		} catch (IllegalArgumentException | IOException e) {
			LOG.error("打开文件系统上的文件失败！！！", e);
			throw new RuntimeException("打开文件系统上的文件失败！！！");
		}

		try {
			IOUtils.copyBytes(in, out, 4096, true);
		} catch (IOException e) {
			LOG.error("拷贝文件流数据失败！！！", e);
		}
	}

	/**
	 * 取到文件系统上的文件元数据信息
	 * 
	 * @param inHdfsPath Hdfs上的文件
	 * @return 返回元数据信息对象
	 */
	public static FileStatus getFileStatus(String inHdfsPath) {
		try {
			return fileSystem.getFileStatus(new Path(inHdfsPath));
		} catch (IllegalArgumentException | IOException e) {
			LOG.error("取到文件系统上的文件信息失败！！！", e);
			throw new RuntimeException("取到文件系统上的文件信息失败！！！");
		}
	}

	/**
	 * 取到文件系统上的文件元数据信息的具体详情
	 * 
	 * @param inHdfsPath Hdfs上的文件
	 * @return 返回元数据信息的具体详情对象
	 */
	public static BlockLocation[] getBlockLocations(String inHdfsPath) {
		FileStatus fstatus = getFileStatus(inHdfsPath); // 文件信息对象
		try {
			return fileSystem.getFileBlockLocations(fstatus, 0, fstatus.getLen()); // 文件详情对象
		} catch (IOException e) {
			LOG.error("取到文件系统上的文件详情信息失败！！！", e);
			throw new RuntimeException("取到文件系统上的文件详情信息失败！！！");
		}
	}

	/**
	 * 
	 * @param dirPath  要创建文件目录路径
	 * @return 是否创建成功
	 */
	public static boolean createDir(String dirPath) {
		Path dir = new Path(dirPath); // 要创建的目录 对象， 一定要使用绝对 路径
		try {
			if (fileSystem.exists(dir)) { // 文件是否存在
				LOG.debug("在文件系统上 目录存在...");
				return true;
			} else {
				return fileSystem.mkdirs(dir);// 创建目录
			}
		} catch (IOException e) {
			LOG.error("在文件系统上 创建目录失败！！！", e);
			throw new RuntimeException("在文件系统上 创建目录失败！！！");
		}
	}

	/**
	 * 保存数据到文件系统
	 * @param saveFilePath 保存文件的路径
	 * @param content  //要保存的数据
	 * @param isAppend //是否追加
	 * @return 是否成功
	 */
	public static boolean save(String saveFilePath, String content, boolean isAppend) {
		try {
			Path file = new Path(saveFilePath);
			FSDataOutputStream out = null;
			if (fileSystem.exists(file) && isAppend) {
				// 文件存在 ， 追加内容
				out = fileSystem.append(file, 4096, new Progressable() {
					@Override // 执行提示
					public void progress() {
						System.out.print(">");
					}
				});
			} else {
				// 文件不存在
				out = fileSystem.create(file, true, 4096, new Progressable() {
					@Override // 执行提示
					public void progress() {
						System.out.print(".");
					}
				});
			}

			ByteArrayInputStream in = new ByteArrayInputStream(content.getBytes());
			IOUtils.copyBytes(in, out, 4096, true);
			return true;
		} catch (IOException e) {
			LOG.error("保存数据到文件系统上 失败！！！", e);
			throw new RuntimeException("保存数据到文件系统上 失败！！！");
		}
	}

	/**
	 * 删除文件
	 * @param filePath 要删除的文件
	 * @param recursive 删除目录文件
	 * @return 是否成功
	 */
	public static boolean del(String filePath, boolean recursive){
		try {
			return fileSystem.delete(new Path(filePath) , recursive);
		} catch (IllegalArgumentException | IOException e) {
			LOG.error("删除 文件系统上的文件失败！！！", e);
			throw new RuntimeException("删除 文件系统上的文件失败！！！");
		}
	}


	/**
	 * 从文件系统下载文件
	 * @param srcPath  要下载的文件
	 * @param dstPath  要下载到本地的路径
	 * @return 是否成功
	 */
	public static boolean download(String srcPath, String dstPath){
		try {
			fileSystem.copyToLocalFile(new Path(srcPath), new Path(dstPath));
			return true;
		} catch (IllegalArgumentException | IOException e) {
			LOG.error("从文件系统上下载文件失败！！！", e);
			throw new RuntimeException("从文件系统上下载文件失败！！！");
		}
	}

	/**
	 * 上传文件到文件系统
	 * @param srcPath  要上传的文件
	 * @param dstPath  要上传到的文件系统路径
	 * @param overwrite 是否覆盖文件
	 * @return 是否成功
	 */
	public static boolean upload(String srcPath, String dstPath, boolean overwrite){
		try {
			fileSystem.copyFromLocalFile(false, overwrite, new Path(srcPath), new Path(dstPath));
			return true;
		} catch (IllegalArgumentException | IOException e) {
			LOG.error("上传文件到文件系统上失败！！！", e);
			throw new RuntimeException("上传文件到文件系统上失败！！！");
		}
	}


	/**
	 * 打印文件系统上文件的文件信息
	 * @param inHdfsPath 文件的路径
	 */
	public static void showFileStatus(String inHdfsPath) {
		FileStatus fileInfo = getFileStatus(inHdfsPath);
		System.out.println("文件大小：" + showSize(fileInfo.getLen()));
		System.out.println("文件是否是目录 ：" + fileInfo.isDirectory());
		System.out.println("文件的相同份数 ：" + fileInfo.getReplication());
		System.out.println("文件的拆分的块的大小 ：" + showSize(fileInfo.getBlockSize()));
		System.out.println("文件的最后修改时间 ：" + showDate(fileInfo.getModificationTime()));
		System.out.println("文件的操作权限 ：" + fileInfo.getPermission());
		System.out.println("文件的拥有者 ：" + fileInfo.getOwner());
		System.out.println("文件的所属组 ：" + fileInfo.getGroup());
		try {
			System.out.println("文件的关联 ：" + (fileInfo.isSymlink() ? fileInfo.getSymlink() : "没有关联文件"));
		} catch (IOException e) {
		}
	}

	/**
	 *  字节数据的单位转换
	 * @param num  字节数大小
	 * @return 带单位的字节数大小
	 */
	public static String showSize(long num) {
		// 1024 KB 1024 MB 1024GB 1024TB 1024 PB 1024 ZB 1024EB
		if (num < 1024) {
			return num + ".0Byte";
		} else if (1024 <= num && num < (long) Math.pow(1024, 2)) {
			return num / 1024.0 + "KB";
		} else if ((long) Math.pow(1024, 2) <= num && num < (long) Math.pow(1024, 3)) {
			return num / Math.pow(1024, 2) + "MB";
		} else if ((long) Math.pow(1024, 3) <= num && num < (long) Math.pow(1024, 4)) {
			return num / Math.pow(1024, 3) + "GB";
		} else if ((long) Math.pow(1024, 4) <= num && num < (long) Math.pow(1024, 5)) {
			return num / Math.pow(1024, 4) + "TB";
		} else if ((long) Math.pow(1024, 5) <= num && num < (long) Math.pow(1024, 6)) {
			return num / Math.pow(1024, 5) + "PB";
		} else if ((long) Math.pow(1024, 6) <= num && num < (long) Math.pow(1024, 7)) {
			return num / Math.pow(1024, 6) + "ZB";
		} else {
			return num / Math.pow(1024, 7) + "EB";
		}
	}

	/**
	 * 转换毫秒数成日期字符串
	 * @param time 毫秒数
	 * @return 日期字符串
	 */
	public static String showDate(long time) {
		Date date = new Date(time);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sdf.format(date);
	}
}

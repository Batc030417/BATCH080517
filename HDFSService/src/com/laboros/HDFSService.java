package com.laboros;

import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//hadoop fs -put WordCount.txt /user/edureka
//java o.a.h.fs.FsShell -put WordCount.txt /user/edureka

//java -cp somejars c.l.HDFSService WordCount.txt /user/edureka -D KEY1=Value1 -D Key2=Value2

public class HDFSService extends Configured implements Tool {

	public static void main(String[] args) 
	{
		
		System.out.println("IN MAIN METHOD");
	 //3 Steps
		//step- 1 : Validations
		if(args.length<2)
		{
			System.out.println("JAVA USAGE "+HDFSService.class.getName()+" /path/to/edgenode/local/file /path/to/hdfs/dest/dir");
			return;
		}
		//step: 2
		Configuration conf = new Configuration(Boolean.TRUE);
		conf.set("fs.defaultFS", "hdfs://localhost:8020");
		//step:3 
		try {
			int i=ToolRunner.run(conf, new HDFSService(), args);
			if(i==0)
			{
				System.out.println("SUCCESS");
			}else{
				System.out.println("FAILURE");
			}
		} catch (Exception e) {
			System.out.println("FAILURE");
			e.printStackTrace();
		}
	}
	@Override
	public int run(String[] args) throws Exception 
	{
		System.out.println("IN RUN METHOD");
	//File Write = Create metadata + Add data
		//Step: 1 : Create metadata
		//Create a Path
		//Invoke HDFS.create(path)
		System.out.println("CREATING METADATA");
		Configuration conf =super.getConf();
		FileSystem hdfs = FileSystem.get(conf);
		//create path
		final String inputEdgeNodeFileName = args[0]; //WordCount.txt
		final String hdfsDestDir = args[1]; // /user/edureka
//		convert into path
		Path hdfsDestDirWithFileName = new Path(hdfsDestDir, inputEdgeNodeFileName);
		
		FSDataOutputStream fsdos = hdfs.create(hdfsDestDirWithFileName);
		
		//Add Data
		//Get the input stream
		System.out.println("ADDING DATA");
		InputStream is = new FileInputStream(inputEdgeNodeFileName);
		
		IOUtils.copyBytes(is, fsdos, conf, Boolean.TRUE);
		
		return 0;
	}
}
package com.laboros;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//hadoop fs -put WordCount.txt /user/edureka
//java o.a.h.fs.FsShell -put WordCount.txt /user/edureka

//java -cp somejars c.l.HDFSService WordCount.txt /user/edureka -D KEY1=Value1 -D Key2=Value2

public class HDFSService extends Configured implements Tool {

	public static void main(String[] args) 
	{
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
	public int run(String[] args) throws Exception {
		return 0;
	}



}

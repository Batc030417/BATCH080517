package com.laboros;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountDriver extends Configured implements Tool {
	
	
	public static void main(String[] args) 
	{
		
		System.out.println("IN MAIN METHOD");
	 //3 Steps
		//step- 1 : Validations
		if(args.length<2)
		{
			System.out.println("JAVA USAGE "+WordCountDriver.class.getName()+" /path/to/edgenode/local/file /path/to/hdfs/dest/dir");
			return;
		}
		//step: 2
		Configuration conf = new Configuration(Boolean.TRUE);
		//step:3 
		try {
			int i=ToolRunner.run(conf, new WordCountDriver(), args);
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
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

}

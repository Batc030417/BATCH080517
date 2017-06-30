package com.laboros.driver;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.laboros.mapper.WordCountMapper;
import com.laboros.reducer.WordCountReducer;

public class WordCountDriver extends Configured implements Tool {
	
	
	public static void main(String[] args) 
	{
		
		System.out.println("IN MAIN METHOD");
		
		System.out.println("argument length:"+args.length);
		
		
		
	 //3 Steps
		//step- 1 : Validations
		if(args.length<2)
		{
			System.out.println("JAVA USAGE "+WordCountDriver.class.getName()+" /path/to/edgenode/local/file /path/to/hdfs/dest/dir");
			return;
		}
		//step: 2
		Configuration conf = new Configuration(Boolean.TRUE);
//		printConf(conf);
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
	public int run(String[] args) throws Exception 
	{
		System.out.println("IN RUN METHOD");
		System.out.println("argument length:"+args.length);
		
		//Step-1 : Load Configuration
		Configuration conf = super.getConf();
		
		//step-2 : Create job instance
		
		Job wordCountDriver = Job.getInstance(conf, WordCountDriver.class.getName());
		
		//step-3 : Setting the classpath for the driver to load the mapper class
		wordCountDriver.setJarByClass(WordCountDriver.class);
		
		//step-4 : Setting input
		final String hdfsInput=args[0];
		//Every file or directory represented as URI
		final Path hdfsInputPath=new Path(hdfsInput);
		TextInputFormat.addInputPath(wordCountDriver, hdfsInputPath);
		wordCountDriver.setInputFormatClass(TextInputFormat.class);
		
		//step-5: Setting output
		
		final String hdfsOutputDir=args[1];
		//Every file or directory represented as URI
		final Path hdfsOutputDirPath=new Path(hdfsOutputDir);
		TextOutputFormat.setOutputPath(wordCountDriver, hdfsOutputDirPath);
		wordCountDriver.setOutputFormatClass(TextOutputFormat.class);
		
		//step-6 : Setting Mapper
		wordCountDriver.setMapperClass(WordCountMapper.class);
		//step-7: Setting mapper output key class and output value class
		wordCountDriver.setMapOutputKeyClass(Text.class);
		wordCountDriver.setMapOutputValueClass(IntWritable.class);
		
		//step-8 : Setting Reducer
		wordCountDriver.setReducerClass(WordCountReducer.class);
		//step-9: Setting reducer output key class and output value class
		wordCountDriver.setOutputKeyClass(Text.class);
		wordCountDriver.setOutputValueClass(IntWritable.class);
		
		//step-10: Trigger method
		wordCountDriver.waitForCompletion(Boolean.TRUE);
//		printConf(conf);
		//Total steps = 10
		return 0;
	}
	
	public static void printConf(final Configuration conf)
	{
		int iCount=0;
		for (Map.Entry<String, String> entry : conf) 
		{
			iCount++;
			System.out.println(entry.getKey()+"======="+entry.getValue());
		}
		System.out.println(iCount);
	}

}

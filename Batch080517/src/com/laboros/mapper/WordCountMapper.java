package com.laboros.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		// key --0
		// value -- DEER RIVER RIVER

		final String inputLine = value.toString();
		if (StringUtils.isNotEmpty(inputLine)) 
		{
			final String wordSeperator = " ";
			final String[] words = StringUtils.splitPreserveAllTokens(
					inputLine, wordSeperator);
			
			Text input=new Text();
			
			final IntWritable ONE=new IntWritable(1);
			
			for (String word : words) 
			{
				input.set(word);
				context.write(input, ONE);
			}
		}
	};
}
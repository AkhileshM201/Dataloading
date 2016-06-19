package com.mapreduce.capgemini;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BikeStatusMapper extends Mapper<Object, Text, NullWritable, Text>{
	
	private String record=null; 
	private String finalvalue = "-";
	@Override
	protected void map(Object key, Text value,Context context)
			throws IOException, InterruptedException {
		
		String[] str = value.toString().split("\\s+");
		if (str.length==5)
		{
			record = str[0]+" "+str[1]+" "+str[3]+" "+str[4];
		}
		if(str.length==6)
		{
			record = str[0]+" "+str[1]+" "+str[4]+" "+str[5];	
		}else
		{
			record = str[0]+" "+str[1]+" "+str[3]+" "+str[4];
					
		}
		

		context.write(NullWritable.get(),new Text(record));

	}
	
	
}

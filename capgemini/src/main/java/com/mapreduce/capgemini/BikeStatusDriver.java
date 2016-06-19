package com.mapreduce.capgemini;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class BikeStatusDriver {
	public static void main(String[] args) throws Exception {
		
	
	Configuration conf = new Configuration();
	System.out.println("This is experiment doing in eclipse");
	Job job1 = new Job(conf,"Bike Status");
	job1.setJarByClass(BikeStatusDriver.class);

	
	job1.setNumReduceTasks(0);
	job1.setMapperClass(BikeStatusMapper.class);
	job1.setMapOutputKeyClass(NullWritable.class);
	
	job1.setMapOutputValueClass(Text.class);
	job1.setOutputFormatClass(TextOutputFormat.class);
	
	FileInputFormat.addInputPath(job1, new Path(args[0]));
	FileOutputFormat.setOutputPath(job1, new Path(args[1]));
	System.exit(job1.waitForCompletion(true)? 0:1);
	}
}

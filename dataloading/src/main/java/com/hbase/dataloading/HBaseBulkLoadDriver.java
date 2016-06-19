package com.hbase.dataloading;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HBaseBulkLoadDriver extends Configured implements Tool{

	private static final String DATA_SEPERATOR = ",";
	private static final String TABLE_NAME="users";
	private static final String COLUMN_FAMILY_1="personalDetails";
	private static final String COLUMN_FAMILY_2= "contactDetails";
	
	
	public static void main(String[] args){
	
		try{
			int response = ToolRunner.run(HBaseConfiguration.create(), 
								new HBaseBulkLoadDriver(), args);
			if(response == 0){
				System.out.println("job is successfully completed");
				
			}else{
				System.out.println("job failed");
			}
					
										
					
		}catch(Exception e){
			e.printStackTrace();
		}
		
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		int result = 0;
		String outputPath = args[1];
		Configuration conf = getConf();
		conf.set("data.seperator", DATA_SEPERATOR);
		conf.set("hbase.table.name", TABLE_NAME);
		conf.set("COLUMN_FAMILY_1", COLUMN_FAMILY_1);
		conf.set("COLUMN_FAMILY_2", COLUMN_FAMILY_2);
		conf.set("hbase.zookeeper.quorum","localhost");
		conf.set("hbase.zookeeper.property.clientPort","2181");
		Job job=new Job(conf,"bilk loading");
		job.setJarByClass(HBaseBulkLoadDriver.class);
		job.setJobName("Bulk Loading Hbase Table::"+TABLE_NAME);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapperClass(HBaseBulkLoadMapper.class);
		FileInputFormat.addInputPaths(job, args[0]);
		FileSystem.getLocal(getConf()).delete(new Path(outputPath), true);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setMapOutputValueClass(Put.class);
		HFileOutputFormat.configureIncrementalLoad(job, new HTable(conf,TABLE_NAME));				
		job.waitForCompletion(true);
		if(job.isSuccessful()){
			HBaseBulkLoad.doBulkLoad(outputPath,TABLE_NAME);
			
		}else{
			result =-1;
		}
	return result;
	
	
	}
	
	
	
	
	
	
	
}

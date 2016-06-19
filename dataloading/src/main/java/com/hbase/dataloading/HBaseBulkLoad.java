package com.hbase.dataloading;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

public class HBaseBulkLoad {
	
	public static void doBulkLoad(String pathToFile, String tableName){
		
	try {
		Configuration conf = new Configuration();
		System.out.println("This is bulk loading");
		conf.set("mapreduce.child.java.opts","-Xmxlg");
		HBaseConfiguration.addHbaseResources(conf);
		LoadIncrementalHFiles loadFiles=
				new LoadIncrementalHFiles(conf);
		HTable htable =new HTable(conf, tableName);
		loadFiles.doBulkLoad(new Path(pathToFile),htable);
		System.out.println("Bulk load completed...");
		
			
		
	}catch(Exception e){
		e.printStackTrace();
	}
		
	}

}

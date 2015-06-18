package org.bulkload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class Test {
//	HDFS数据：
//	rowkey3,f1,f1c1,f1c1 values
//	rowkey3,f1,f1c2,f1c2 values
//	rowkey3,f1,f1c3,f1c3 values
//	rowkey3,f2,f2c1,f2c1 values
//	rowkey3,f2,f2c2,f2c2 values
//	rowkey3,f2,f2c3,f2c3 values
//	rowkey4,f1,f1c1,f1c1 values
//	rowkey4,f1,f1c2,f1c2 values
//	rowkey4,f1,f1c3,f1c3 values
//	rowkey4,f2,f2c1,f2c1 values
//	rowkey5,f3,f3c1,f3c1 values
//	rowkey5,f3,f3c2,f3c2 values
	static Configuration config = null;
	static {
		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "slave1,slave2,slave3");
		config.set("hbase.zookeeper.property.clientPort", "2181");
	}
	public static void createTab(String tabName) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(config);
		if (admin.tableExists(tabName)) {
			System.out.println(tabName + " exists!");
			admin.close();
			return;
		}
		HTableDescriptor table = new HTableDescriptor(tabName);
		table.addFamily(new HColumnDescriptor("f1"));
		table.addFamily(new HColumnDescriptor("f2"));
		table.addFamily(new HColumnDescriptor("f3"));
		table.getFamily(Bytes.toBytes("f1"));
		admin.createTable(table);
		admin.close();
	}
	public static void main(String[] args) {
		try {
			Test.createTab("testtable1");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

package org.endpoint;

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.hbase.HBaseConfiguration;  
import org.apache.hadoop.hbase.HTableDescriptor;  
import org.apache.hadoop.hbase.MasterNotRunningException;  
import org.apache.hadoop.hbase.client.HBaseAdmin;  
import org.apache.hadoop.hbase.util.Bytes;  
  
public class SetCoprocessor {  
  
    /** 
     * @param args 
     * @throws Exception 
     * @throws MasterNotRunningException 
     */  
    public static void main(String[] args) throws MasterNotRunningException,  
            Exception {  
        // TODO Auto-generated method stub  
        byte[] tableName = Bytes.toBytes("testtable");  
        Configuration conf = HBaseConfiguration.create();  
        HBaseAdmin admin = new HBaseAdmin(conf);  
        admin.disableTable(tableName);  
  
        HTableDescriptor htd = admin.getTableDescriptor(tableName);  
        htd.addCoprocessor("org.MyEndpointImpl", new Path(  
                "hdfs://master:9000/test.jar"), 1001,  
                null);  
        admin.modifyTable(tableName, htd);  
        admin.enableTable(tableName);  
        admin.close();  
    }  
  
}  

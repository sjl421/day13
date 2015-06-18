package org.endpoint;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class Test {
	public static void main(String[] args) throws Throwable {
		// 我们建了一个测试表testtable,里面info列族上有两个列，一个salecount销售量，一个salemoney销售额,我们通过上面自定义的cp,返回总销售量和总销售额
		final String[] columns = new String[] { "salecount", "salemoney" };
		Configuration conf = HBaseConfiguration.create();
		final Scan scan;
		scan = new Scan();
		scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("salecount"));
		scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("salemoney"));
		MyEndpointClient client = new MyEndpointClient(conf);
		MyMutiSum mutiSum = client.mutiSum("testtable", "info", columns, scan);
		for (int i = 0; i < columns.length; i++) {
			System.out.println(columns[i] + " sum is :" + mutiSum.getSum(i));
		}
	}
}

package org.endpoint;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;

public class MyEndpointClient {
	protected static Log log = LogFactory.getLog(MyEndpointClient.class);

	private Configuration conf;

	public MyEndpointClient(Configuration conf) {
		this.conf = conf;
	}

	public MyMutiSum mutiSum(String tableName, String cf,
			final String[] columns, final Scan scan) throws Throwable {
		class MutiSumCallBack implements Batch.Callback<MyMutiSum> {

			MyMutiSum sumVal = null;

			public MyMutiSum getSumResult() {
				return sumVal;
			}

			@Override
			public void update(byte[] region, byte[] row, MyMutiSum result) {
				sumVal = add(sumVal, result);
			}

			public MyMutiSum add(MyMutiSum l1, MyMutiSum l2) {
				if (l1 == null ^ l2 == null) {
					return (l1 == null) ? l2 : l1; // either of one is null.
				} else if (l1 == null) // both are null
					return null;
				MyMutiSum mutiSum = new MyMutiSum(columns.length);
				for (int i = 0; i < columns.length; i++) {
					mutiSum.setSum(i, l1.getSum(i) + l2.getSum(i));
				}

				return mutiSum;
			}
		}
		MutiSumCallBack sumCallBack = new MutiSumCallBack();
		HTable table = null;
		for (int i = 0; i < columns.length; i++) {
			scan.addColumn(Bytes.toBytes(cf), Bytes.toBytes(columns[i]));
		}
		try {
			table = new HTable(conf, tableName);
			// 根据startRow和stopRow确定regionserver,即RPC
			// SERVER,在startRow~stopRow范围的region上执行rpc调用
			// 所以这个方法其实发起了多个RPC调用，每个RPC调用返回后都会调用sumCallBack的update方法，将自己执行的结果通过update方法累加到sumCallBack的sumVal上
			table.coprocessorExec(MyCoprocessorProtocol.class,
					scan.getStartRow(), scan.getStopRow(),
					new Batch.Call<MyCoprocessorProtocol, MyMutiSum>() {
						@Override
						public MyMutiSum call(MyCoprocessorProtocol instance)
								throws IOException {
							// TODO Auto-generated method stub
							// instance.getMutiSum会转化成对region上的指定的MyCoprocessorProtocol的实现类的该方法的rpc调用
							return instance.getMutiSum(columns, scan);
						}
					}, sumCallBack);
		} finally {
			if (table != null) {
				table.close();
			}
		}
		// 返回的是sumCallBack的sumVal，即所有region结果通过update的累加。
		return sumCallBack.getSumResult();
	}

}
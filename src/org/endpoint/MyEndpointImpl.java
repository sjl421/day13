package org.endpoint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

public class MyEndpointImpl extends BaseEndpointCoprocessor implements
		MyCoprocessorProtocol {
	protected static Log log = LogFactory.getLog(MyEndpointImpl.class);

	@Override
	public MyMutiSum getMutiSum(String[] columns, Scan scan) throws IOException {
		MyMutiSum result = new MyMutiSum(columns.length);
		InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
				.getRegion().getScanner(scan);

		List<KeyValue> keyValues = new ArrayList<KeyValue>();
		try {
			boolean hasMoreRows = false;
			do {
				// 循环每一个row的待sum的列，
				hasMoreRows = scanner.next(keyValues);
				for (int i = 0; i < columns.length; i++) {

					String column = columns[i];
					for (KeyValue kv : keyValues) {
						if (column.equals(Bytes.toString(kv.getQualifier()))) {
							byte[] value = kv.getValue();
							if (value == null || value.length == 0) {
							} else {
								Long tValue = Bytes.toLong(value);
								// 如果是待sum的列，就将该列的值累加到之前的sum值上去。
								result.setSum(i, result.getSum(i) + tValue);

							}
							break;
						}
					}
				}

				keyValues.clear();
			} while (hasMoreRows);
		} finally {
			scanner.close();
		}
		log.debug("Sum from this region is "
				+ ((RegionCoprocessorEnvironment) getEnvironment()).getRegion()
						.getRegionNameAsString() + ": ");

		for (int i = 0; i < columns.length; i++) {
			log.debug(columns[i] + " " + result.getSum(i));
		}
		// 将sum后的自定义writable对象返回
		return result;
	}

}
package org;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

public class TestCoprocessor extends BaseRegionObserver {

	private HTable table = null;

	@Override
	public void start(CoprocessorEnvironment env) throws IOException {
		table = new HTable(env.getConfiguration(), "testtable");
	}

	@Override
	public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e,
			final Put put, final WALEdit edit, final boolean writeToWAL)
			throws IOException {
		List<KeyValue> kv = put.get("colf".getBytes(), "col".getBytes());
		Iterator<KeyValue> kvItor = kv.iterator();
		while (kvItor.hasNext()) {
			KeyValue tmp = kvItor.next();
			Put indexPut = new Put(tmp.getValue());
			indexPut.add("colf".getBytes(), "col".getBytes(), tmp.getRow());
			table.put(indexPut);
			table.flushCommits();
		}
	}

	@Override
	public void stop(CoprocessorEnvironment env) throws IOException {
		table.close();
	}
}

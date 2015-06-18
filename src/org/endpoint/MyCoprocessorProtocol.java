package org.endpoint;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

public interface MyCoprocessorProtocol extends CoprocessorProtocol {

	MyMutiSum getMutiSum(String[] columns, Scan scan) throws IOException;
}

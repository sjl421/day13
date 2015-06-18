package org.endpoint;
import java.io.DataInput;  
import java.io.DataOutput;  
import java.io.IOException;  
import java.util.ArrayList;  
import java.util.List;  
  
import org.apache.hadoop.io.Writable;  
  
public class MyMutiSum implements Writable {  
  
    private List<Long> resultList = new ArrayList<Long>();  
  
    public MyMutiSum() {  
    }  
  
    public MyMutiSum(int resultSize) {  
        for (int i = 0; i < resultSize; i++) {  
            resultList.add(0L);  
        }  
    }  
  
    public Long getSum(int i) {  
        return resultList.get(i);  
    }  
  
    public void setSum(int i, Long sum) {  
        resultList.set(i, sum);  
    }  
  
    public int getResultSize() {  
        return resultList.size();  
    }  
  
    @Override  
    public void write(DataOutput out) throws IOException {   
        out.writeInt(resultList.size());  
        for (Long v : resultList) {  
            out.writeLong(v);  
        }  
    }  
  
    @Override  
    public void readFields(DataInput in) throws IOException {   
        int size = in.readInt();  
        for (int i = 0; i < size; i++) {  
            resultList.add(in.readLong());  
        }  
    }  
  
}  
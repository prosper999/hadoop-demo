package etg.hadoop.demo;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {
    private long upFlow;
    private long dnFlow;

    public FlowBean() {
        super();
    }

    public FlowBean(long upFlow, long dnFlow) {
        super();
        this.upFlow = upFlow;
        this.dnFlow = dnFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDnFlow() {
        return dnFlow;
    }

    public void setDnFlow(long dnFlow) {
        this.dnFlow = dnFlow;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(dnFlow);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong();
        this.dnFlow = dataInput.readLong();
    }

    @Override
    public String toString() { //不加toStirng函数，最后输出内存的地址
        return upFlow + "\t" + dnFlow;
    }
}

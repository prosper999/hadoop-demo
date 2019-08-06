package etg.hadoop.demo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values,
                          Reducer<Text, FlowBean, Text, FlowBean>.Context context)
            throws IOException, InterruptedException {
        long sum_upFlow = 0;
        long sum_dnflow = 0;
        int i = 0;
        for (FlowBean flowBean : values) {
            i++;
            System.out.println("Key:" + key + "，累加第" + i + "次");
            sum_dnflow += flowBean.getDnFlow();
            sum_upFlow += flowBean.getUpFlow();
        }
        context.write(key, new FlowBean(sum_upFlow, sum_dnflow));
    }

}

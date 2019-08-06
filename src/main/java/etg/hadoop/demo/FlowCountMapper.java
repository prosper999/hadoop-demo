package etg.hadoop.demo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(" ");
        String phoneNbr = fields[1];
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long dnFlow = Long.parseLong(fields[fields.length - 2]);

        context.write(new Text(phoneNbr), new FlowBean(upFlow, dnFlow));
    }

}

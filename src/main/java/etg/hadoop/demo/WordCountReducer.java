package etg.hadoop.demo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * KEY IN 对应mapper输出的KEYOUT
 * VALUE IN 对应mapper输出的VALUEOUT
 *
 * KEY OUT 是用户自定义逻辑处理完成之后输出数据中的key,word count中是单词，Text
 * VALUE OUT是用户自定义逻辑处理完成之后输出数据中的value,word count中是单词次数，IntWriable
 *
 * 因为所有数据都需要被序列化，而Java的Serializable序列化会包含很多类的冗余信息，所以不适用Java中的类型，
 * 而是使用hadoop封装的可序列化类型
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     *参数1：Key是一组相同单词<K,V>对的Key
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Reducer<Text, IntWritable, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }
        context.write(key, new IntWritable(count));
    }
}

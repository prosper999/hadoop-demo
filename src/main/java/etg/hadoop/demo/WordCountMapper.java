package etg.hadoop.demo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEY IN 默认情况下，是MR框架所读到的一行文本的起始偏移量，LongWritable
 * VALUE IN 默认情况下，是MR框架所读到的一行文本的内容，Text
 * <p>
 * KEY OUT 是用户自定义逻辑处理完成之后输出数据中的Key，Word count中是单词，Text
 * VALUE OUT 是用户自定义逻辑处理完成之后输出数据中的value,word count中是单词次数，IntWritable
 * <p>
 * 因为所有数据都需要被序列化，而Java的Serializable序列化会包含很多类的冗余信息，所以不适用Java中的类型，
 * 而是使用Hadoop封装的可序列化类型
 */

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     * map阶段的业务逻辑就写在自定义的map()方法中
     * maptask会对每一行输入数据调用一次我们自定义的map()方法
     */
    @Override
    protected void map(LongWritable key, Text value
            , Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //将maptask传给我们的文本内容先转换成String
        String line = value.toString();
        //根据空格将这一行切分成单词
        String[] words = line.split(" ");
        //将单词输出为<K,V>
        for (String word : words) {
            //将单词作为Key，将次数1作为Value，以便后续的数据分发，可以根据单词分发，以便于相同单词汇总到相同的reduce task
            context.write(new Text(word), new IntWritable(1));
        }
    }
}

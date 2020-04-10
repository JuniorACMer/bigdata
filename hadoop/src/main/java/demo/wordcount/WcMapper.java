package demo.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Spark
 */
public class WcMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
    Text wordMap = new Text();
    LongWritable oneValue =  new LongWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word : words) {
            this.wordMap.set(word);
            this.oneValue.set(1);
            context.write(wordMap, oneValue);
        }
    }
}

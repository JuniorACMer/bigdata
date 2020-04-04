package demo.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Spark
 */
public class WcReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    LongWritable countResult = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (LongWritable value : values) {
            sum += value.get();
        }
        countResult.set(sum);
        context.write(key, countResult);
    }
}

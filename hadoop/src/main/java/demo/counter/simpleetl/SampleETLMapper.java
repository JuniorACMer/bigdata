package demo.counter.simpleetl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author inspur
 * @version 1.0
 * @date 2020/4/8 10:59
 */
public class SampleETLMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(" ");
        int size = 11;
        if (line.length > size) {
            context.write(value, NullWritable.get());
            context.getCounter("ETL", "True").increment(1);
        } else {
            context.getCounter("ETL", "False").increment(1);
        }
    }
}

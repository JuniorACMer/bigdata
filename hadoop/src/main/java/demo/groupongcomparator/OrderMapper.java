package demo.groupongcomparator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author inspur
 * @version 1.0
 * @date 2020/4/7 10:43
 */
public class OrderMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {
    private OrderBean orderBean = new OrderBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\t");
        orderBean.setOrderId(words[0]);
        orderBean.setProductId(words[1]);
        orderBean.setPrice(Double.parseDouble(words[3]));
        context.write(orderBean,NullWritable.get());

    }
}

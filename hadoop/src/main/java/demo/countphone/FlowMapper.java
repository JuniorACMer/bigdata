package demo.countphone;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Spark
 */
public class FlowMapper extends Mapper<LongWritable, Text,Text,FlowBean> {
    private Text phone = new Text();
    private FlowBean flowBean = new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields =  value.toString().split(" ");
        phone.set(fields[0]);
        long upFlow = Long.parseLong(fields[fields.length-3]);
        long downFlow = Long.parseLong(fields[fields.length-2]);
        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        context.write(phone,flowBean);
    }
}

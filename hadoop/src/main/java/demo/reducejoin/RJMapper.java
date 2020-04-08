package demo.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author inspur
 * @version 1.0
 * @date 2020/4/8 8:58
 */
public class RJMapper extends Mapper<LongWritable, Text, RJBean, NullWritable> {
    private RJBean rjBean = new RJBean();
    private String fileName;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        fileName = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        if (fileName.equals("order.txt")) {
            rjBean.setId(fields[0]);
            rjBean.setPid(fields[1]);
            rjBean.setAmount(Integer.parseInt(fields[2]));
            rjBean.setpName(fields[3]);
        } else {
            rjBean.setId(fields[0]);
            rjBean.setpName(fields[1]);
            rjBean.setPid("");
            rjBean.setAmount(0);
        }
        context.write(rjBean,NullWritable.get());
    }
}

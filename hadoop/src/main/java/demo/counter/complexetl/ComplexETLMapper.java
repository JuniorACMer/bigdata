package demo.counter.complexetl;

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
public class ComplexETLMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private LogBean logBean = new LogBean();
    private Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        parseLog(line);
        if (logBean.isValid()) {
            text.set(logBean.toString());
            context.write(text, NullWritable.get());
            context.getCounter("ETL", "True").increment(1);
        } else {
            context.getCounter("ETL", "False").increment(1);
        }
    }

    private void parseLog(String line) {
        String[] fields = line.split(" ");
        if (fields.length > 11) {
            logBean.setRemote_addr(fields[0]);
            logBean.setRemote_user(fields[1]);
            //....
            if (fields.length > 12) {
                logBean.setHttp_user_agent(fields[11] + " " + fields[12]);
            } else {
                logBean.setHttp_user_agent(fields[11]);
            }
            if(Integer.parseInt(logBean.getStatus())>=400){
                logBean.setValid(false);
            }
        }else {
            logBean.setValid(true);
        }
    }
}

package demo.countphone;

import demo.partition.UserDefinePartition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author inspur
 * @version 1.0
 * @date 2020/4/2 14:47
 */
public class PartitionerFlowCount {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setNumReduceTasks(5);
        job.setPartitionerClass(UserDefinePartition.class);
        job.setJarByClass(PartitionerFlowCount.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}

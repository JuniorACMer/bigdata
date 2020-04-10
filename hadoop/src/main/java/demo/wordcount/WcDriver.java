package demo.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author Spark
 */
public class WcDriver {
    public static void main(String[] args) throws Exception {
        System.setProperty("user.name","hdfs");

        Configuration configuration = new Configuration();
        configuration.set("dfs.defaultFS","hdfs://10.180.210.93:8020");
        configuration.set("yarn.resourcemanager.address","hdfs://10.180.210.93:8050");
        Job job = Job.getInstance(configuration);

        job.setNumReduceTasks(10);
        //设置作业提交主类
        job.setJarByClass(WcDriver.class);
        //设置mapper喝reducer
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReducer.class);
        //设置mapper输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //设置reducer输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //设置 输入输出路径
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //等待驱动程序调用
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}

package demo.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author inspur
 * @version 1.0
 * @date 2020/4/8 10:59
 */
public class SampleETLDriver {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        //map-reduce中间进行压缩
        configuration.set("mapreduce.map.output.compress","true");
        configuration.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);
        Job job = Job.getInstance(configuration);
        job.setJarByClass(SampleETLDriver.class);
        job.setMapperClass(SampleETLMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //输出端进行压缩，如果本身是压缩文件，hadoop可以识别
        FileOutputFormat.setCompressOutput(job,true);
        FileOutputFormat.setOutputCompressorClass(job,BZip2Codec.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

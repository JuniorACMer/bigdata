package demo.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author inspur
 * @version 1.0
 * @date 2020/4/7 14:45
 */
public class MyRecordWriter extends RecordWriter<LongWritable, Text> {
    private FSDataOutputStream inspur;
    private FSDataOutputStream other;

    public void initialize(TaskAttemptContext job) throws IOException {
        String outDir = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        FileSystem fileSystem = FileSystem.get(job.getConfiguration());
        inspur = fileSystem.create(new Path(outDir + "/inspur.log"));
        other = fileSystem.create(new Path(outDir + "/other.log"));
    }

    @Override
    public void write(LongWritable key, Text value) throws IOException, InterruptedException {
        String out = value.toString() + "\n";
        if (out.contains("inspur")) {
            inspur.write(out.getBytes());
        } else {
            other.write(out.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(inspur);
        IOUtils.closeStream(other);
    }
}

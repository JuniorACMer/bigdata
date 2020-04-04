package demo.split;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author inspur
 * @version 1.0
 * @date 2020/4/2 17:23
 * 自定义RR，处理一个文件，把一个文件直接读成一个key-value
 */
public class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {

    private boolean notRead = true;
    private Text key = new Text();
    private BytesWritable value = new BytesWritable();
    private FSDataInputStream inputStream;
    private Path path;
    private FileSplit fs;


    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        //转换切片类型为文件切片
        fs = (FileSplit) split;
        //通过切片获取路径
        path = fs.getPath();
        //通过路径获取文件系统
        FileSystem fileSystem = path.getFileSystem(context.getConfiguration());
        //开流
        inputStream = fileSystem.open(path);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (notRead) {
            key.set(path.toString());
            byte[] buffer = new byte[(int) fs.getLength()];
            inputStream.read(buffer);
            value.set(buffer, 0, buffer.length);
            notRead = false;
            return true;
        } else {
            return false;
        }

    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {

        return null;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {

        return notRead ? 0 : 1;
    }

    @Override
    public void close() throws IOException {
        if (null != inputStream) {
            IOUtils.closeStream(inputStream);
        }
    }
}

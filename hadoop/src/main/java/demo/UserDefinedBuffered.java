package demo;

import org.apache.hadoop.mapred.MapTask;

import java.io.IOException;

/**
 * @author inspur
 * @version 1.0
 * @date 2020/4/8 16:11
 */
public class UserDefinedBuffered extends MapTask.MapOutputBuffer {
    public UserDefinedBuffered() {
        super();
    }

    @Override
    public void init(Context context) throws IOException, ClassNotFoundException {
        super.init(context);
    }

    @Override
    public synchronized void collect(Object key, Object value, int partition) throws IOException {
        super.collect(key, value, partition);
    }

    @Override
    public int compare(int mi, int mj) {
        return super.compare(mi, mj);
    }

    @Override
    public void swap(int mi, int mj) {
        super.swap(mi, mj);
    }

    @Override
    public void flush() throws IOException, ClassNotFoundException, InterruptedException {
        super.flush();
    }

    @Override
    public void close() {
        super.close();
    }
}

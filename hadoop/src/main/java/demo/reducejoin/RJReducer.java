package demo.reducejoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author inspur
 * @version 1.0
 * @date 2020/4/8 8:58
 */
public class RJReducer extends Reducer<RJBean, NullWritable, RJBean, NullWritable> {
    private RJBean rjBean = new RJBean();

    @Override
    protected void reduce(RJBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<NullWritable> iterator = values.iterator();
        iterator.next();
        String pname = key.getpName();
        while (iterator.hasNext()){
            iterator.next();
            key.setpName(pname);
            context.write(key,NullWritable.get());
        }
    }
}

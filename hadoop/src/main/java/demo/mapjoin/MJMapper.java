package demo.mapjoin;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author inspur
 * @version 1.0
 * @date 2020/4/8 9:44
 */
public class MJMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Map<String, String> map = new HashMap<>();
    private Text text = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFile = context.getCacheFiles();
        String path = cacheFile[0].getPath();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
        String line;
        while (StringUtils.isNotEmpty(line = bufferedReader.readLine())) {
            String[] fields = line.split("\t");
            map.put(fields[0], fields[1]);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        String pName = map.get(fields[1]);
        if (null == pName) {
            pName = "NULL";
        }
        text.set(fields[0] + "\t" + pName + "\t" + fields[2]);
        context.write(text, NullWritable.get());
    }
}

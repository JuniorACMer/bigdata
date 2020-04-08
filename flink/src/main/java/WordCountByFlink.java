import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author Spark
 */
public class WordCountByFlink {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8899);


        lines.flatMap((String line, Collector<Tuple2<String,Integer>> collector)->{
            Arrays.stream(line.split(" ")).forEach(w->{
                collector.collect(Tuple2.of(w,1));
            });
        });
        env.execute("");

    }
}

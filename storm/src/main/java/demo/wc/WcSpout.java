package demo.wc;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.ThreadLocalRandom.current;

/**
 * @author inspur
 * @version 1.0
 * @date 2020/4/7 17:12
 */
public class WcSpout extends BaseRichSpout {

    private final static Map<Integer, String> map = new HashMap<Integer, String>();
    private SpoutOutputCollector collector;

    public WcSpout() {
        map.put(1, "北京");
        map.put(2, "上海");
        map.put(3, "天津");
        map.put(4, "郑州");
        map.put(5, "成都");
        map.put(6, "长沙");
        map.put(7, "杭州");
        map.put(8, "苏州");
    }

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        System.out.println("===========================打开水龙头==========================");
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        collector.emit(new Values(map.get(current().nextInt(9))));
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("city-name"));
    }
}

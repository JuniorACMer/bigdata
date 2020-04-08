package demo.wc;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * @author inspur
 * @version 1.0
 * @date 2020/4/7 17:06
 */
public class StarBolt extends BaseBasicBolt {


    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        final String value = tuple.getStringByField("city-name");
        System.out.println("***********************" + value + "***************************");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}

package demo.wc;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * @author inspur
 * @version 1.0
 * @date 2020/4/7 17:12
 */
public class WcTopology {
    public static void main(String[] args) throws Exception {
        final TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("city-stream",new WcSpout(),2);
        topologyBuilder.setBolt("star-bolt",new StarBolt(),3).setNumTasks(3).fieldsGrouping("city-stream",new Fields("city-name"));
        topologyBuilder.setBolt("sharp-bolt",new SharpBolt(),3).setNumTasks(3).shuffleGrouping("city-stream");
        Config config = new Config();
        config.setNumWorkers(2);
        config.setDebug(true);
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("print_city_name", config,topologyBuilder.createTopology());
    }
}

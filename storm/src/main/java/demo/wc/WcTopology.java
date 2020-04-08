package demo.wc;

import org.apache.storm.topology.TopologyBuilder;

/**
 * @author inspur
 * @version 1.0
 * @date 2020/4/7 17:12
 */
public class WcTopology {
    public static void main(String[] args) {
        final TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("city-stream",new WcSpout());
        topologyBuilder.setBolt("star-bolt",new StarBolt());
        topologyBuilder.setBolt("sharp-bolt",new SharpBolt());

    }
}

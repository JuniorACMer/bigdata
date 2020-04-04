package demo.partition;

import demo.countphone.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author inspur
 * @version 1.0
 * @date 2020/4/3 15:22
 */
public class UserDefinePartition extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        String phone = text.toString();
        switch (phone.substring(0,3)){
            case "136": return 0;
            case "137": return 1;
            case "138": return 2;
            default: return 3;
        }
    }
}

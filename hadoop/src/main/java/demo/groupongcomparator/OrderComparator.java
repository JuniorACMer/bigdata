package demo.groupongcomparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author inspur
 * @version 1.0
 * @date 2020/4/7 10:54
 */
public class OrderComparator extends WritableComparator {
    public OrderComparator() {
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean oa = (OrderBean) a;
        OrderBean ob = (OrderBean) b;
        return oa.getOrderId().compareTo(ob.getOrderId());
    }
}

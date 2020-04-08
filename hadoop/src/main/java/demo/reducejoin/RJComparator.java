package demo.reducejoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author inspur
 * @version 1.0
 * @date 2020/4/8 9:25
 */
public class RJComparator extends WritableComparator {
    public RJComparator() {
        super(RJBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        RJBean oa = (RJBean) a;
        RJBean ob = (RJBean) b;
        return oa.getPid().compareTo(ob.getPid());
    }
}

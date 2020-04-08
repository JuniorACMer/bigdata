package demo.reducejoin;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author inspur
 * @version 1.0
 * @date 2020/4/8 9:00
 */
public class RJBean implements WritableComparable<RJBean> {

    private String id;
    private String pid;
    private int amount;
    private String pName;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getpName() {
        return pName;
    }

    public void setpName(String pName) {
        this.pName = pName;
    }

    @Override
    public String toString() {
        return id + "\t" + pName + "\t" + amount + "\t";
    }

    @Override
    public int compareTo(RJBean o) {
        int compare = this.pid.compareTo(o.pid);
        if (compare == 0) {
            return o.pid.compareTo(this.pid);
        } else {
            return compare;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(pName);
        out.writeInt(amount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.pName = in.readUTF();
        this.amount = in.readInt();
    }
}

package demo;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author inspur
 * @version 1.0
 * @date 2020/4/10 15:00
 */
public class ZookeeperCRUD {
    private ZooKeeper zkClient;
    private static final String CONNECT_STRING = "10.180.210.93:2181,10.180.210.94:2181,10.180.210.95:2181";
    private static final int TIMEOUT = 20000;

    @Before
    public void initialize() throws IOException {
//      System.setProperty("user.name", "hdfs");
        zkClient = new ZooKeeper(CONNECT_STRING, TIMEOUT, (event) -> {
            System.out.println("默认回调函数！");
        });
    }

    @Test
    public void ls() throws KeeperException, InterruptedException {
        List<String> list = zkClient.getChildren("/", true);
        for (String s : list) {
            System.out.println("输出结果：" + s);
        }
    }

    @Test
    public void create() throws KeeperException, InterruptedException {
        String result = zkClient.create("/LULULU", "WENWEN".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        System.out.println("创建结果：" + result);

    }

    @Test
    public void delete() throws KeeperException, InterruptedException {
        zkClient.delete("/ddd", 0);
    }

    @Test
    public void set() throws KeeperException, InterruptedException {
        Stat stat = zkClient.setData("/LULULU0000000032", "王者荣耀".getBytes(), 0);
        System.out.println(stat);
    }

    @Test
    public void get() throws KeeperException, InterruptedException {
        Stat stat = new Stat();
        byte[] bytes = zkClient.getData("/LULULU0000000032", true, stat);
        String result = new String(bytes);
        System.out.println("获得数据：" + result);
    }

    @Test
    public void isExist() {
        try {
            Stat stat = zkClient.exists("/LULULU0000000032", true);
            if (stat == null) {
                System.out.println("该目录不存在！");
            } else {
                System.out.println(stat.getDataLength());
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testRegister() throws KeeperException, InterruptedException {
        register();
    }

    public void register() throws KeeperException, InterruptedException {
        byte[] data = zkClient.getData("/LULULU0000000032", (watcher) -> {
            try {
                register();
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }, null);
        System.out.println(new String(data));
    }
}

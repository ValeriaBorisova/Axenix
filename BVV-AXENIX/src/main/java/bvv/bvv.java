package bvv;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class bvv {
    private static final Logger LOGGER = LoggerFactory.getLogger(bvv.class);
    private static final ZooKeeper zooKeeper;

    static {
        try {
            zooKeeper = new ZooKeeper("localhost:2181", 20000, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws InterruptedException, KeeperException {

        zooKeeper.create("/C:/kafka_2.13-3.7.0/bin/windows", "data".getBytes(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zooKeeper.create("/C:/kafka_2.13-3.7.0/bin/windows/child", "child".getBytes(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zooKeeper.create("/C:/kafka_2.13-3.7.0/bin/windows/child1", "child".getBytes(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zooKeeper.create("/C:/kafka_2.13-3.7.0/bin/windows/child1/child12", "child".getBytes(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zooKeeper.create("/C:/kafka_2.13-3.7.0/bin/windows/child2", "child".getBytes(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zooKeeper.create("/C:/kafka_2.13-3.7.0/bin/windows/child2/child21", "child".getBytes(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);


        zooKeeper.getChildren("/", false).forEach(child -> {
            try {
                LOGGER.info(getChildType(child));
            } catch (InterruptedException | KeeperException e) {
                throw new RuntimeException(e);
            }
            try {
                clippIN(child, "");
            } catch (InterruptedException | KeeperException e) {
                throw new RuntimeException(e);
            }
        });


        zooKeeper.delete("/C:/kafka_2.13-3.7.0/bin/windows/child2/child21", -1);
        zooKeeper.delete("/C:/kafka_2.13-3.7.0/bin/windows/child2", -1);
        zooKeeper.delete("/C:/kafka_2.13-3.7.0/bin/windows/child1/child12", -1);
        zooKeeper.delete("/C:/kafka_2.13-3.7.0/bin/windows/child1", -1);
        zooKeeper.delete("/C:/kafka_2.13-3.7.0/bin/windows/child", -1);
        zooKeeper.delete("/C:/kafka_2.13-3.7.0/bin/windows/apache-zookeeper" ,-1);
    }

    private static String getChildType(String path, String childName) throws InterruptedException, KeeperException {
        String childWithType = childName;
        if (!zooKeeper.getChildren("/" + path, false).isEmpty()) {
            childWithType += " [Directory]";
        } else {
            childWithType += " [File]";
        }
        return childWithType;
    }

    // Метод для присвоения типа ноды "Директория или файл"
    private static String getChildType(String path) throws InterruptedException, KeeperException {
        String childWithType = path;
        if (!zooKeeper.getChildren("/" + path, false).isEmpty()) {
            childWithType += " [Directory]";
        } else {
            childWithType += " [File]";
        }
        return childWithType;
    }


    private static void clippIN(String path, String clipping) throws InterruptedException, KeeperException {
        clipping += "   ";

        if (!zooKeeper.getChildren("/" + path, false).isEmpty()) {
            String finalClipp = clipping;
            zooKeeper.getChildren("/" + path,
                    false).forEach(childInner -> {
                try {
                    LOGGER.info(finalClipp + getChildType(path + "/" + childInner, childInner));
                } catch (InterruptedException | KeeperException e) {
                    throw new RuntimeException(e);
                }
                try {
                    clippIN(path + "/" + childInner, finalClipp);
                } catch (InterruptedException | KeeperException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }
}


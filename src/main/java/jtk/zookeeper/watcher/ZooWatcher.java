package jtk.zookeeper.watcher;

import jdk.nashorn.internal.ir.WhileNode;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

/**
 * Created by jubin on 7/17/16.
 */

public class ZooWatcher implements Watcher{

    public static Logger logger = LoggerFactory.getLogger(ZooWatcher.class);

    private ZooKeeper zk;

    private String hostPort;

    private String serverId;

    public ZooWatcher(String hostPort){
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort,15000,this);
    }

    void stopZK() throws InterruptedException {
        zk.close();
    }

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    @Override
    public void process(WatchedEvent event) {
        logger.info(String.valueOf(event));
    }

    public void runForMaster() throws KeeperException, InterruptedException {
        zk.create("/master",this.serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);

    }

    public static void main(String[] args) throws IOException, InterruptedException {
        ZooWatcher watcher = new ZooWatcher(args[0]);
        watcher.setServerId(args[1]);

        try {
            watcher.runForMaster();
        }catch (Exception e){
            logger.error("Can't be a master");
        }
        watcher.startZK();
       //while(true) {
            Thread.sleep(60000);
       //}
       watcher.stopZK();
    }
}

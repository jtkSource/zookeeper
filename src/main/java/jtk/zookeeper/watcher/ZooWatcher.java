package jtk.zookeeper.watcher;

import jdk.nashorn.internal.ir.WhileNode;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
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

    private boolean isLeader = false;

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

    public boolean checkMaster(String serverId) throws KeeperException, InterruptedException {
        while(true){
            try{
                Stat stat = new Stat();
                byte[] data = zk.getData("/restMaster",false,stat);
                isLeader = new String(data).equals(serverId);
                logger.info("data from zookeeper " + new String(data));
                return true;

            }catch (KeeperException.NoNodeException e){
                return false;
            }catch (KeeperException.ConnectionLossException e){

            }
        }

    }
    public void runForMaster(String serverId) throws InterruptedException, KeeperException {
        while (true) {

            try {
                zk.create("/restMaster", this.serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL);
                isLeader = true;
                break;
            } catch (KeeperException.NoNodeException e){
                isLeader = false;
                break;
            }catch (KeeperException.ConnectionLossException e){

            } catch (KeeperException e) {
                e.printStackTrace();
            }
            if(checkMaster(serverId))
                break;
        }


    }

    public static void main(String[] args) throws IOException, InterruptedException {
        ZooWatcher watcher = new ZooWatcher(args[0]);

        watcher.setServerId(args[1]);
        watcher.startZK();

        try {

            watcher.runForMaster(watcher.getServerId());

        }catch (Exception e){
            logger.error("Can't be a master ", e);
        }
       //while(true) {
            Thread.sleep(60000);
       //}
       watcher.stopZK();
    }
}

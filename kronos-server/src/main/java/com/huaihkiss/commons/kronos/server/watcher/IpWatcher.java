package com.huaihkiss.commons.kronos.server.watcher;

import com.huaihkiss.commons.kronos.server.prop.Constants;
import com.huaihkiss.commons.kronos.server.listenner.ServiceListenner;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.ConcurrentHashMap;

public class IpWatcher implements Watcher {
    private ZooKeeper zooKeeper;
    private String ip;
    private String jobId;

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }
    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    @Override
    public void process(WatchedEvent event) {
        try {
            ConcurrentHashMap<String, ConcurrentHashMap<String, String>> idMap = ServiceListenner.idMap;
            ConcurrentHashMap<String, String> ipMap = idMap.get(jobId);
            if(ipMap!=null&&!ipMap.isEmpty()){
                if(event.getType()==Event.EventType.NodeDeleted){
                    String data = ipMap.get(ip);
                    if(data!=null&&data.length()>0){
                        ipMap.remove(ip);
                    }
                }else{
                    String nodePath = Constants.ZK_PLAN_PATH+"/"+jobId+"/"+ip;
                    if(event.getType()==Event.EventType.NodeCreated){
                        byte[] data = zooKeeper.getData(nodePath, false, null);
                        ipMap.put(ip,new String(data));
                    }
                    zooKeeper.exists(nodePath,this);
                }
            }


        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

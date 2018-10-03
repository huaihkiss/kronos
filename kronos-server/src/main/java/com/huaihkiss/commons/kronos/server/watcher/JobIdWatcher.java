package com.huaihkiss.commons.kronos.server.watcher;

import com.huaihkiss.commons.kronos.server.listenner.ServiceListenner;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class JobIdWatcher implements Watcher {
    private String jobIdPath;
    private String jobId;
    private ZooKeeper zooKeeper;
    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }


    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getJobIdPath() {
        return jobIdPath;
    }

    public void setJobIdPath(String jobIdPath) {
        this.jobIdPath = jobIdPath;
    }

    @Override
    public void process(WatchedEvent event) {
        if(event.getType()==Event.EventType.NodeChildrenChanged){
            ConcurrentHashMap<String, ConcurrentHashMap<String, String>> idMap = ServiceListenner.idMap;
            try {
                //watch jobid节点 如果该节点下的东西为空 则删除该jobid节点
                List<String> children = zooKeeper.getChildren(jobIdPath, false);
                if(children==null||children.size()==0){
                    zooKeeper.delete(jobIdPath,0);
                    idMap.remove(jobId);
                    return;
                }else{
                    ConcurrentHashMap<String, String> ipMap = idMap.get(jobId);
                    if(ipMap!=null&&!ipMap.isEmpty()){
                        List<String> list = zooKeeper.getChildren(jobIdPath, false);
                        if(list!=null&&!list.isEmpty()){
                            Enumeration<String> keys = ipMap.keys();
                            for(String ip:list){
                                String data = ipMap.get(ip);
                                if(data==null||data.length()<1){
                                    byte[] dataArr = zooKeeper.getData(jobIdPath + "/" + ip, false, null);
                                    if(dataArr!=null&&dataArr.length>0){
                                        data = new String(dataArr);
                                        ipMap.put(ip,data);
                                    }
                                }
                            }
                            /*while(keys.hasMoreElements()){
                                String ip = keys.nextElement();
                                boolean flag = list.remove(ip);
                                if(list.remove(ip)){
                                    continue;
                                }else{
                                    for(){

                                    }
                                }
                            }*/
                        }
                    }
                }
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        try {
            zooKeeper.getChildren(jobIdPath,this );
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
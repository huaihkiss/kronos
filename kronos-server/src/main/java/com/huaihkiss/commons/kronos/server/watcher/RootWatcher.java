package com.huaihkiss.commons.kronos.server.watcher;

import com.huaihkiss.commons.kronos.server.client.JedisClients;
import com.huaihkiss.commons.kronos.server.prop.Constants;
import com.huaihkiss.commons.kronos.server.listenner.ServiceListenner;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class RootWatcher implements Watcher{
    private ZooKeeper zooKeeper;
    private JedisClients jedisClients;

    public JedisClients getJedisClients() {
        return jedisClients;
    }

    public void setJedisClients(JedisClients jedisClients) {
        this.jedisClients = jedisClients;
    }

    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    @Override
    public void process(WatchedEvent event) {
        if(event.getType() == Watcher.Event.EventType.NodeChildrenChanged){
            ConcurrentHashMap<String, ConcurrentHashMap<String, String>> idMap = ServiceListenner.idMap;
            Enumeration<String> keys = idMap.keys();
            try {
                List<String> list = zooKeeper.getChildren(Constants.ZK_PLAN_PATH, false);
                for(String jobId:list){
                    ConcurrentHashMap<String, String> map = idMap.get(jobId);
                    if(map==null||map.isEmpty()){
                        ConcurrentHashMap<String, String> ipMap = new ConcurrentHashMap<>();
                        String jobIdPath = Constants.ZK_PLAN_PATH + "/" + jobId;
                        List<String> children = zooKeeper.getChildren(jobIdPath, false);
                        for(String ip:children){
                            byte[] data = zooKeeper.getData(jobIdPath+"/"+ip, false, null);
                            ipMap.put(ip,"");
                            if(data!=null&&data.length>0){
                                String prop = new String(data);
                                ipMap.put(ip,prop);
                                JobIdWatcher jobIdWatcher=new JobIdWatcher();
                                jobIdWatcher.setJobId(ip);
                                jobIdWatcher.setJobIdPath(jobIdPath);
                                jobIdWatcher.setZooKeeper(zooKeeper);
                                //监听job下的临时节点
                                zooKeeper.getChildren(jobIdPath,jobIdWatcher);
                            }
                        }
                        idMap.put(jobId,ipMap);
                    }
                }
                /*while(keys.hasMoreElements()){
                    String e = keys.nextElement();
                    if(list!=null&&!list.isEmpty()){
                        list.remove(e);
                        for(String jobId:list){
                            ConcurrentHashMap<String, String> map = idMap.get(jobId);
                            if(map==null||map.isEmpty()){
                                ConcurrentHashMap<String, String> ipMap = new ConcurrentHashMap<>();
                                String jobIdPath = Constants.ZK_PLAN_PATH + "/" + jobId;
                                List<String> children = zooKeeper.getChildren(jobIdPath, false);
                                for(String ip:children){
                                    byte[] data = zooKeeper.getData(jobIdPath+"/"+ip, false, null);
                                    ipMap.put(ip,"");
                                    if(data!=null&&data.length>0){
                                        String prop = new String(data);
                                        ipMap.put(ip,prop);
                                        JobIdWatcher jobIdWatcher=new JobIdWatcher();
                                        jobIdWatcher.setJobId(ip);
                                        jobIdWatcher.setJobIdPath(jobIdPath);
                                        jobIdWatcher.setZooKeeper(zooKeeper);
                                        //监听job下的临时节点
                                        zooKeeper.getChildren(jobIdPath,jobIdWatcher);
                                    }
                                }
                                idMap.put(jobId,ipMap);
                            }
//
                        }
                    }


                }*/
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

        try {
            zooKeeper.getChildren(Constants.ZK_PLAN_PATH,this);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}

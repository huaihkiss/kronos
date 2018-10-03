package com.huaihkiss.commons.kronos.server.listenner;

import com.alibaba.fastjson.JSON;
import com.huaihkiss.commons.kronos.server.prop.Constants;
import com.huaihkiss.commons.kronos.server.watcher.JobIdWatcher;
import com.huaihkiss.commons.kronos.server.watcher.RootWatcher;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class ServiceListenner {
    public static ConcurrentHashMap<String,ConcurrentHashMap<String,String>> idMap = new ConcurrentHashMap<String,ConcurrentHashMap<String,String>>();
    public static ConcurrentHashMap<String,ConcurrentHashMap<String,String>> idQueue = new ConcurrentHashMap<String,ConcurrentHashMap<String,String>>();

    public static void ma2in(String[] args) {
        ConcurrentHashMap<String,Integer> map = new ConcurrentHashMap<String,Integer>();
        map.put("1",13);
        map.put("2",23);
        Enumeration<String> keys = map.keys();
        /*while(keys.hasMoreElements()){
            String e = keys.nextElement();
            System.out.println(e);
            if(e.equalsIgnoreCase("1")){
                map.remove("1");
            }
        }*/
        List<String> list = new LinkedList<String>(){{add("1");add("2");}};
        System.out.println(JSON.toJSONString(map.keys()));
    }

    public static void initKronosPath(ZooKeeper zooKeeper) throws KeeperException, InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Stat stat = zooKeeper.exists(Constants.ZK_PLAN_PATH, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if(event.getType()==Event.EventType.NodeCreated){
                    latch.countDown();
                }
            }
        });
        if(stat==null){
            zooKeeper.create(Constants.ZK_PLAN_PATH,Constants.OK.getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            latch.await();
        }
    }
    public static ZooKeeper connectionZooKeeper(String custerIp) throws InterruptedException, IOException {
        class ZkWatcher implements Watcher {
            public CountDownLatch latch;

            public CountDownLatch getLatch() {
                return latch;
            }

            public void setLatch(CountDownLatch latch) {
                this.latch = latch;
            }

            @Override
            public void process(WatchedEvent event) {
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    latch.countDown();
                }
                if(event.getState()==Event.KeeperState.Disconnected){

                }
            }
        }
        ZkWatcher watcher = new ZkWatcher();
        watcher.setLatch(new CountDownLatch(1));
        ZooKeeper zooKeeper = new ZooKeeper(custerIp, Constants.ZK_TIMEOUT,watcher );
        watcher.getLatch().await();
        return zooKeeper;
    }
    public static void main(String ... args) throws IOException, KeeperException, InterruptedException {
        //连接zookeeper
        ZooKeeper zooKeeper = connectionZooKeeper("localhost:2181");
        //初始化kronos目录
        initKronosPath(zooKeeper);
        CountDownLatch latch = new CountDownLatch(1);
        //监听新节点

        RootWatcher rootWatcher = new RootWatcher();
        rootWatcher.setZooKeeper(zooKeeper);
        zooKeeper.getChildren(Constants.ZK_PLAN_PATH,rootWatcher , new AsyncCallback.ChildrenCallback() {
            @Override
            public void processResult(int i,final String s, Object o, List<String> list) {
                //监听现有的所有jobid子节点 并添加watcher
                list.parallelStream().forEach(jobId->{
                    try {
                        String jobIdPath = s + "/" + jobId;
                        JobIdWatcher watcher = new JobIdWatcher();
                        watcher.setZooKeeper(zooKeeper);
                        watcher.setJobIdPath(jobIdPath);
                        watcher.setJobId(jobId);
                        zooKeeper.getChildren(jobIdPath,watcher , new ChildrenCallback() {
                            @Override
                            public void processResult(int i, String s, Object o, List<String> list) {
                                if(list!=null&&list.size()>0){
                                    list.parallelStream().forEach(ip->{
                                        String nodePath = jobIdPath+"/"+ip;
                                        Watcher nodeWatcher = new Watcher() {
                                            @Override
                                            public void process(WatchedEvent event) {
                                                try {
                                                    ConcurrentHashMap<String, String> ipMap = idMap.get(jobId);
                                                    if(ipMap!=null&&!ipMap.isEmpty()){
                                                        if(event.getType()==Event.EventType.NodeDeleted){
                                                            String data = ipMap.get(ip);
                                                            if(data!=null&&data.length()>0){
                                                                ipMap.remove(ip);
                                                            }
                                                        }else{
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
                                        };
                                        try {
                                            zooKeeper.exists(nodePath,nodeWatcher);
                                        } catch (KeeperException e) {
                                            e.printStackTrace();
                                        } catch (InterruptedException e) {
                                            e.printStackTrace();
                                        }
                                    });
                                }
                            }
                        },"");

                    } /*catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }*/finally {

                    }
                });

            }
        },"");

        new CountDownLatch(1).await();
    }
}

package com.huaihkiss.commons.kronos.client.configure;

import com.alibaba.fastjson.JSON;
import com.huaihkiss.commons.kronos.client.prop.ClientsProperties;
import com.huaihkiss.commons.kronos.client.prop.Constants;
import com.huaihkiss.commons.kronos.client.annotation.EnabledPlanJob;
import com.huaihkiss.commons.kronos.client.prop.NodeProperties;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

@Configuration
@EnableConfigurationProperties(value = ClientsProperties.class)
@ConditionalOnProperty(prefix = "kronos.job", value = "enable", matchIfMissing = true)
public class CommonJobConfiguration {
    @Autowired
    private ClientsProperties clientsProperties;
    @Autowired
    private ApplicationContext applicationContext;
//    public static void main(String[] args) throws UnknownHostException {
//        System.out.println(InetAddress.getLocalHost().getHostAddress());
//    }
    @Bean
    public ZooKeeper zooKeeper() throws IOException, KeeperException, InterruptedException {
        ZooKeeper zooKeeper = null;
//        ZooKeeper zooKeeper = new ZooKeeper();
        if(clientsProperties!=null&&!StringUtils.isEmpty(clientsProperties.getZookeeperClusterAddress())){
            zooKeeper = new ZooKeeper(clientsProperties.getZookeeperClusterAddress(), 30 * 1000, new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                }
            });
            Stat stat = zooKeeper.exists(Constants.ZK_PLAN_PATH, true);
            if(stat==null){
                throw new IllegalArgumentException("plan-server is not found");
            }

        }else{
            throw new NullPointerException("zookeeperClusterAddress");
        }

        return zooKeeper;
    }
    @Bean
    public Boolean scanStatus(ZooKeeper zooKeeper){
        String packages = clientsProperties.getScanPackage();
        if(StringUtils.isEmpty(packages)){
            throw new NullPointerException("scanPackage is null!");
        }
        Reflections reflections = new Reflections(packages,new SubTypesScanner(false));
        reflections.getAllTypes().parallelStream().forEach(className-> {
            try {
                Class<?> clazz = Class.forName(className);
                if(clazz!=null){
                    Method[] methods = clazz.getMethods();
                    if(methods!=null&&methods.length>0){

                        RequestMapping clazzMapping = clazz.getAnnotation(RequestMapping.class);
                        String clazzPath = "";
                        if(clazzMapping!=null&&clazzMapping.path()!=null&&clazzMapping.path().length>0){
                            clazzPath = clazzMapping.path()[0];
                        }
                        final String tempClazzPath = clazzPath;
                        Arrays.asList(methods).parallelStream().forEach(method -> {
                            EnabledPlanJob planJob = method.getAnnotation(EnabledPlanJob.class);
                            RequestMapping requestMapping = method.getAnnotation(RequestMapping.class);
                            PostMapping postMapping = method.getAnnotation(PostMapping.class);
                            GetMapping getMapping = method.getAnnotation(GetMapping.class);

                            if(planJob!=null){
//                                if(requestMapping==null&&postMapping==null&&getMapping==null){
                                NodeProperties nodeProperties = new NodeProperties();

                                if(requestMapping==null){
                                    throw new IllegalArgumentException("Method Annotation RequestMapping is not found!");
                                }else{
                                    if(planJob.id()==null){
                                        throw new NullPointerException("Job Id is Null!");
                                    }
//                                    zooKeeper.create()
                                    String[] path = requestMapping.path();
                                    if(!(path!=null&&path.length>0)){
                                        path = requestMapping.value();
                                    }
                                    if(path==null){
                                        throw new NullPointerException("RequestMapping path is null!");
                                    }
                                    String urlPath = tempClazzPath+"/"+path[0];
                                    nodeProperties.setPath(urlPath);
                                }
                                nodeProperties.setSync(planJob.isSync());
                                nodeProperties.setExecuteTime(planJob.executeTime());
                                nodeProperties.setId(planJob.id());
                                try {
                                    String idPath = Constants.ZK_PLAN_PATH + "/" + nodeProperties.getId();
                                    Stat stat = zooKeeper.exists(idPath, false);
                                    if(stat==null){
                                        zooKeeper.create(idPath,new byte[0],ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
                                    }
                                    String ipPath = idPath+InetAddress.getLocalHost().getHostAddress();
                                    zooKeeper.create(ipPath, JSON.toJSONString(nodeProperties).getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
//                                    zooKeeper.getData(Constants.ZK_PLAN_PATH+"/"+nodeProperties.getPath(),false,null);
                                } catch (KeeperException e) {
                                    e.printStackTrace();
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                } catch (UnknownHostException e) {
                                    e.printStackTrace();
                                }
//                                System.out.println(planJob.executeTime());
//                                System.out.println(planJob.id());
//                                System.out.println(planJob.isSync());
                            }
                        });
                    }
                }

            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        });
        return false;
    }
    public static void m2ain(String[] args) throws IOException, KeeperException, InterruptedException {
        class ZkWatcher implements Watcher{
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
        ZooKeeper zooKeeper = new ZooKeeper("localhost:2181", 30000,watcher );

        watcher.getLatch().await();
        CountDownLatch latch = new CountDownLatch(1);

        final String idPath = "/kronos";
        Watcher childWatcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if(event.getType() == Event.EventType.NodeChildrenChanged){
                    System.out.println(event.getPath());
                    System.out.println(event.getWrapper().getPath());
                }
                if (event.getType() == Event.EventType.NodeDeleted) {
                    //删除重建 实现节点被删能连接上
                    String thisPath = event.getPath();
                    if (thisPath.length() > Constants.ZK_PLAN_PATH.length()) {
                        try {
                            List<String> list = zooKeeper.getChildren(Constants.ZK_PLAN_PATH, false);
                            String[] dirName = thisPath.split("/");
                            if (dirName.length == 3) {
                                int i = thisPath.lastIndexOf("/");
                                String s = thisPath.substring(0, i);
                                System.out.println(s);
                                zooKeeper.delete(s, 0);
                            }

                        } catch (KeeperException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
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
        };
        zooKeeper.getChildren(Constants.ZK_PLAN_PATH,childWatcher , new AsyncCallback.ChildrenCallback() {
            @Override
            public void processResult(int i,final String s, Object o, List<String> list) {
                list.parallelStream().forEach(str->{
                    try {
                        String jobIdPath = s + "/" + str;
                        Watcher childWatcher = new Watcher() {
                            @Override
                            public void process(WatchedEvent event) {
                                if(event.getType()==Event.EventType.NodeChildrenChanged){
                                    try {
                                        List<String> children = zooKeeper.getChildren(jobIdPath, false);
                                        if(children==null||children.size()==0){
                                            zooKeeper.delete(jobIdPath,0);
                                            return;
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
                        };
                        zooKeeper.getChildren(jobIdPath,childWatcher );

                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });

                System.out.println(i+":"+s+":"+o.toString()+":"+list.toString());
            }
        },"");
        new CountDownLatch(1).await();
    }
    public static void masin(String[] args) {
        Reflections reflections = new Reflections("com.huaihkiss",new SubTypesScanner(false));
        reflections.getAllTypes().parallelStream().forEach(className-> {
            try {
                Class<?> clazz = Class.forName(className);
                if(clazz!=null){
                    Method[] methods = clazz.getMethods();
                    if(methods!=null&&methods.length>0){
                        Arrays.asList(methods).parallelStream().forEach(method -> {
                            EnabledPlanJob planJob = method.getAnnotation(EnabledPlanJob.class);
                            if(planJob!=null){
                                System.out.println(planJob.executeTime());
                                System.out.println(planJob.id());
                                System.out.println(planJob.isSync());
//                                System.out.println(method.getName());
                            }
                        });
                    }
                }

            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        });

    }
}

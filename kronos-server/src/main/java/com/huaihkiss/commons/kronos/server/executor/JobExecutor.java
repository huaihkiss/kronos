package com.huaihkiss.commons.kronos.server.executor;

import com.alibaba.fastjson.JSON;
import com.huaihkiss.commons.kronos.server.client.JedisClients;
import com.huaihkiss.commons.kronos.server.listenner.ServiceListenner;
import com.huaihkiss.commons.kronos.server.prop.Constants;
import com.huaihkiss.commons.kronos.server.prop.NodeProperties;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

public class JobExecutor {
    private static JobExecutor jobExecutor = new JobExecutor();
    private String redisHost;
    private String redisPort;
    private String password;
    private ZooKeeper zooKeeper;
    private static JedisClients jedisClients;
    public static JobExecutor getInstance(JedisClients jedisClients){
        synchronized ("job lock"){
            if(jedisClients==null){
                JobExecutor.jedisClients = jedisClients;
            }
            return jobExecutor;
        }

    }

    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    public static void main(String[] args) {
        System.out.println(3%2);
    }
    public void autoExecute(){
        ConcurrentHashMap<String, ConcurrentHashMap<String, String>> idMap = ServiceListenner.idMap;
        while(true){
            if(idMap==null||idMap.isEmpty()){
                try {
                    Thread.sleep(Constants.EXECUTOR_NOTNODE_SLEEP);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else {
                long cur = System.currentTimeMillis();

//                jedisClients.
                Enumeration<String> keys = idMap.keys();
                try {
                    List<String> jobIdList = zooKeeper.getChildren(Constants.ZK_PLAN_PATH, false);
                    for(String jobId:jobIdList){
                        String jobIdPath = Constants.ZK_PLAN_PATH+"/"+jobId;
                        List<String> ipList = zooKeeper.getChildren(jobIdPath, false);

                        if(ipList!=null&&ipList.size()>0){
                            Long incr = jedisClients.incr(""+jobId);
                            String ip = ipList.get(incr.intValue() % ipList.size());
                            byte[] dataArray = zooKeeper.getData(jobIdPath + "/" + ip, false, null);
                            if(dataArray!=null&&dataArray.length>0){
                                String data = new String(dataArray);
                                if(data!=null&&data.length()>0){
                                    NodeProperties nodeProperties = JSON.parseObject(data, NodeProperties.class);
                                    String executeTimeStr = jedisClients.get("" + jobId);
                                    if(executeTimeStr==null||(executeTimeStr!=null&&Long.valueOf(executeTimeStr)<System.currentTimeMillis()-nodeProperties.getExecuteTime())){
                                        try(CloseableHttpAsyncClient client = HttpAsyncClients.createDefault();){
                                            HttpGet get = new HttpGet("http://"+ip+nodeProperties.getPath());
                                            if(nodeProperties.isSync()){
                                                jedisClients.set(""+jobId,System.currentTimeMillis()+"");
                                            }
                                            Future<HttpResponse> future = client.execute(get, new FutureCallback<HttpResponse>() {
                                                @Override
                                                public void completed(HttpResponse httpResponse) {
                                                    if(!nodeProperties.isSync()){
                                                        jedisClients.set(""+jobId,System.currentTimeMillis()+"");
                                                    }
                                                }

                                                @Override
                                                public void failed(Exception e) {
                                                    e.printStackTrace();
                                                }

                                                @Override
                                                public void cancelled() {

                                                }
                                            });
                                        } catch (IOException e) {
                                            e.printStackTrace();
                                        }
                                    }

                                }


                            }

                        }
                    }
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

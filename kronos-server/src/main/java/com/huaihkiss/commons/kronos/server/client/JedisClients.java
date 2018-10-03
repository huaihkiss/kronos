package com.huaihkiss.commons.kronos.server.client;

import com.huaihkiss.commons.kronos.server.prop.Constants;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

import java.io.IOException;
import java.util.Random;

public class JedisClients {
    private JedisPool jedisPool;
    private String host;
    private String password;
    private Integer port;
    private static JedisClients jedisClients;
    private Random random=new Random();
    private JedisClients(String host, Integer port) {
        this(host,null,port);
    }

    private JedisClients(String host, String password, Integer port) {
        this.host = host;
        this.password = password;
        this.port = port;
        if(password!=null){
            this.jedisPool = new JedisPool(new GenericObjectPoolConfig(),host,port,Constants.REDIS_TIMEOUT,password);
        }else{
            this.jedisPool = new JedisPool(host,port);
        }

    }
    public static JedisClients getInstance(String host, String password, Integer port){
        synchronized ("1"){
            if(jedisClients == null){
                jedisClients = new JedisClients(host,password,port);
            }
        }

        return jedisClients;
    }
    public String get(String key){
        try(Jedis jedis = jedisPool.getResource();){
            if(jedis != null){
                return jedis.get(key);
            }else{
                return null;
            }
        }
    }
    public void set(String key,String value){
        try(Jedis jedis = jedisPool.getResource();){
            if(jedis != null){
                jedis.set(key,value);
            }
        }
    }
    public String hget(String key,String field){
        try(Jedis jedis = jedisPool.getResource();){
            if(jedis != null){
                return jedis.hget(key,field);
            }else{
                return null;
            }
        }
    }
    public void hset(String key,String field,String value){
        try(Jedis jedis = jedisPool.getResource();){
            if(jedis != null){
                jedis.hset(key,field,value);
            }
        }
    }
    public void del(String key){
        try(Jedis jedis = jedisPool.getResource();){
            if(jedis != null){
                jedis.del(key);
            }
        }
    }
    public void lpush(String key,String ... value){
        try(Jedis jedis = jedisPool.getResource();){
            if(jedis != null){
                jedis.lpush(key,value);
            }
        }
    }
    public void rpush(String key,String ... value){
        try(Jedis jedis = jedisPool.getResource();){
            if(jedis != null){
                jedis.rpush(key,value);
            }
        }
    }
    public String rpop(String key){
        try(Jedis jedis = jedisPool.getResource();){
            if(jedis != null){
                return jedis.rpop(key);
            }else{
                return null;
            }
        }
    }
    public String lpop(String key){
        try(Jedis jedis = jedisPool.getResource();){
            if(jedis != null){
                return jedis.lpop(key);
            }else{
                return null;
            }
        }
    }
    public Long llen(String key){
        try(Jedis jedis = jedisPool.getResource();){
            if(jedis != null){
                return jedis.llen(key);
            }else{
                return 0L;
            }
        }
    }
    public Long incr(String key){
        try(Jedis jedis = jedisPool.getResource();){
            if(jedis != null){
                return jedis.incr(key);
            }else{
                return 0L;
            }
        }
    }
    public Long expire(String key,Integer expireTime){
        try(Jedis jedis = jedisPool.getResource();){
            if(jedis != null){
                return jedis.expire(key,expireTime);
            }else{
                return 0L;
            }
        }
    }
    public Long decr(String key){
        try(Jedis jedis = jedisPool.getResource();){
            if(jedis != null){
                return jedis.decr(key);
            }else{
                return 0L;
            }
        }
    }
    public void zadd(String key,double score,String value){
        try(Jedis jedis = jedisPool.getResource();){
            if(jedis != null){
                jedis.zadd(key,score,value);
            }
        }
    }

    public void zaddByTimestamp(String key,String value){
        String score = (System.currentTimeMillis()/1000)+""+(random.nextInt(89999)+10000);
        zadd(key,Double.valueOf(score),value);
    }
    public void blpushNotInList(String key,String value){
        try(Jedis jedis = jedisPool.getResource();){
            if(jedis != null){
                jedis.watch(key);
                try(Transaction multi = jedis.multi();){
                    multi.lpush(key,value);
                    multi.exec();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }
    }
    public void zrem(String key,String ... value){
        try(Jedis jedis = jedisPool.getResource();){
            if(jedis != null){
                jedis.zrem(key,value);
            }
        }
    }
    public Long zcard(String key){
        try(Jedis jedis = jedisPool.getResource();){
            if(jedis != null){
                return jedis.zcard(key);
            }else{
                return 0L;
            }
        }
    }
    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

//    public String getHost() {
//        return host;
//    }

    public void setHost(String host) {
        this.host = host;
    }

//    public Integer getPort() {
//        return port;
//    }

    public void setPort(Integer port) {
        this.port = port;
    }
}

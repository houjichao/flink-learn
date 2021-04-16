package com.hjc.learn.dau;

import java.io.Serializable;
import redis.clients.jedis.Jedis;

/**
 * Redis工具类
 *
 * @author houjichao
 */
public class RedisUtils implements Serializable {

    public static transient Jedis jedis;

    public RedisUtils() {
        jedis = new Jedis("localhost", 6379);
        jedis.auth("123456");
    }

}

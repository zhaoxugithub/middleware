package com.serendipity.redisson.demo01;

import org.redisson.Redisson;
import org.redisson.api.RRateLimiter;
import org.redisson.api.RateIntervalUnit;
import org.redisson.api.RateType;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;


public class RateLimiterDemo01 {

    private static RRateLimiter rateLimiter;

    private static String REDIS_URL = "redis://192.168.150.151:6379";

    private static String MyLimiter = "myLimiter";

    private static RedissonClient redissonClient;

    static {
        System.out.println("init....");
        Config config = new Config();
        config.useSingleServer().setAddress(REDIS_URL);
        redissonClient = Redisson.create(config);
        rateLimiter = redissonClient.getRateLimiter(MyLimiter);

        System.out.println("init over....");
    }

    public static void main(String[] args) {
        System.out.println("start......");
        // 设置速率：每秒最多5个请求
        rateLimiter.trySetRate(RateType.OVERALL, 5, 1, RateIntervalUnit.SECONDS);
        for (int i = 0; i < 10; i++) {
            boolean access = rateLimiter.tryAcquire();
            if (access) {
                System.out.println(i + "获取线程");
            } else {
                System.out.println(i + "获取线程失败");
            }
        }
        redissonClient.shutdown();
        System.out.println("over....");
    }
}

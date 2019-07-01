package com.processor.logger;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;


public class StatusLogger {
    private Timer logger;
    private long period;
    private ConcurrentHashMap<String, Integer> statusMap;

    public StatusLogger(long period, ConcurrentHashMap<String, Integer> statusMap){
        logger = new Timer();
        this.period = period;
        this.statusMap = statusMap;
    }

    public void start(){
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                logStatus();
            }
        };
        logger.scheduleAtFixedRate(task, period, period);
    }

    public void logStatus(){
        System.out.println("-------------------------------");
        System.out.println("req.sent->"+statusMap.getOrDefault("req.sent", 0));
        System.out.println("req.processed->"+statusMap.getOrDefault("req.processed", 0));
        System.out.println("req.success->"+statusMap.getOrDefault("req.success", 0));
        System.out.println("req.failed->"+statusMap.getOrDefault("req.failed", 0));
        System.out.println("req.error->"+statusMap.getOrDefault("req.error", 0));
    }

    public void stop(){
        logger.cancel();
        logger.purge();
    }
}

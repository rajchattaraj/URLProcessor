package com.processor.urlconsumer;

import com.processor.util.Helper;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

public class UrlConsumer {

    private BlockingQueue<String> queue;
    private ExecutorService executor;
    private ConcurrentHashMap<String, Integer> statusMap;
    private CloseableHttpClient httpclient;

    public UrlConsumer(BlockingQueue<String> queue,
                       ConcurrentHashMap<String, Integer> statusMap,
                       ExecutorService executor,
                       CloseableHttpClient httpclient){
        this.queue = queue;
        this.executor= executor;
        this.statusMap = statusMap;
        this.httpclient = httpclient;
    }

    private boolean readCompleted;

    public void setReadCompleted() {
        this.readCompleted = true;
    }

    public CompletableFuture<List<Void>> process(){
        String url = null;
        List<CompletableFuture<Void>> httpFutures = new ArrayList<>();
        while(!queue.isEmpty() || !readCompleted){
            url = queue.poll();
            if(url != null) {
                httpFutures.add(callHttp(url));
            }
        }
        return Helper.sequence(httpFutures);
    }


    private CompletableFuture<Void> callHttp(String url){
        return CompletableFuture.runAsync(()->{
            try {
                HttpGet httpget = new HttpGet(url);

                // Create a custom response handler
                ResponseHandler<String> responseHandler = new ResponseHandler<String>() {
                    @Override
                    public String handleResponse(
                            final HttpResponse response) throws ClientProtocolException, IOException {
                        int status = response.getStatusLine().getStatusCode();
                        statusMap.put("req.processed", statusMap.getOrDefault("req.processed", 0)+1);
                        if (status >= 200 && status < 300) {
                            statusMap.put("req.success", statusMap.getOrDefault("req.success", 0)+1);
                        } else {
                            statusMap.put("req.failed", statusMap.getOrDefault("req.failed", 0)+1);
                        }
                        return String.valueOf(status); //just returning the status as we are not caring the about response
                    }

                };
                httpclient.execute(httpget, responseHandler);
                statusMap.put("req.sent", statusMap.getOrDefault("req.sent", 0)+1);
            }catch (IOException e){
                e.printStackTrace();// should be at debug level
                statusMap.put("req.error", statusMap.getOrDefault("req.error", 0)+1);
            }
        }, executor);
    }


}

package com.processor.reader;

import com.processor.util.Helper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

public class UrlFileReader {

    private BlockingQueue<String> queue;
    private ExecutorService executor;

    public UrlFileReader(BlockingQueue<String> queue, ExecutorService executor){
        this.queue = queue;
        this.executor= executor;
    }

    public CompletableFuture<List<Void>> readFiles(String dirPath) {
        File dir = new File(dirPath);
        return Helper.sequence(
                Stream.of(dir.listFiles())
                        .map(this::readFile)
                        .collect(Collectors.toList()));
    }

    private CompletableFuture<Void> readFile(File file) {
        return CompletableFuture.runAsync(()->
            {
                try(GZIPInputStream gzFileStream = new GZIPInputStream(new FileInputStream(file));
                    BufferedReader buffer = new BufferedReader(new InputStreamReader(gzFileStream))) {
                    String line = buffer.readLine();
                    while (line != null) {
                        queue.put(line);
                        line = buffer.readLine();
                    }
                }catch(Exception e){
                    System.err.println("Error in reading input data");
                    e.printStackTrace();
                }
            }
        , executor);
    }

}

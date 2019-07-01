import com.processor.logger.StatusLogger;
import com.processor.reader.UrlFileReader;
import com.processor.urlconsumer.UrlConsumer;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.*;
import static com.processor.util.Configuration.*;

public class URLProcessor {

    private void execute(){
        System.out.println("URLProcessor execution started..");
        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(QUEUE_SIZE);
        ExecutorService readerExecutor = Executors.newFixedThreadPool(READER_THREAD_COUNT);
        ExecutorService consumerExecutor = Executors.newFixedThreadPool(CONSUMER_THREAD_COUNT);
        ConcurrentHashMap<String, Integer> statusMap = new ConcurrentHashMap<>();
        CloseableHttpClient httpclient = HttpClients.custom().setMaxConnTotal(MAX_HTTP_CONNECTION).build();
        UrlFileReader reader = new UrlFileReader(queue, readerExecutor);
        UrlConsumer consumer = new UrlConsumer(queue, statusMap, consumerExecutor, httpclient);
        StatusLogger logger = new StatusLogger(LOGGING_PERIOD, statusMap);
        URL inputDir = getClass().getResource("inputData");
        logger.start();
        Future readerFuture=reader.readFiles(inputDir.getPath()).thenAccept(l->{
            consumer.setReadCompleted();
        });
        Future consumerFuture = consumer.process().thenAccept(l->{
            logger.logStatus();
            System.out.println("All URLs are processed...");
            try {
                httpclient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        while(!readerFuture.isDone() || !consumerFuture.isDone()){
            // Delay the main thread until all URL processed
        };
        readerExecutor.shutdown();
        consumerExecutor.shutdown();
        logger.stop();
        System.out.println("URLProcessor execution completed..");
    }

    public static void main(String... args){
        new URLProcessor().execute();
    }
}

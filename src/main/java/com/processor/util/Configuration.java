package com.processor.util;

/**
 * Ideally should be loaded from Config file
 */
public interface Configuration {
    int QUEUE_SIZE = 1000;
    int READER_THREAD_COUNT=1;
    int CONSUMER_THREAD_COUNT=10;
    long LOGGING_PERIOD= 3000L;
    int MAX_HTTP_CONNECTION=1000;
}

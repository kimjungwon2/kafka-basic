package com.example.multi.basic;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConsumerWorker implements Runnable {

    private String recordValue;

    ConsumerWorker(String recordValue) {
        this.recordValue = recordValue;
    }

    @Override
    public void run() {
        log.info("thread:{}\trecord:{}", Thread.currentThread().getName(), recordValue);
    }
}

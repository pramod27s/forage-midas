package com.jpmc.midascore.kafka;

import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class TransactionListener {
    private static final Logger log = LoggerFactory.getLogger(TransactionListener.class);

    // Optional: keep received transactions for later tasks / inspection
    private final List<Transaction> received = new CopyOnWriteArrayList<>();

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core")
    public void onMessage(Transaction transaction) {
        received.add(transaction);
        log.info("Received transaction: {}", transaction);
        // Breakpoint here
    }

    public List<Transaction> getReceived() {
        return received;
    }
}

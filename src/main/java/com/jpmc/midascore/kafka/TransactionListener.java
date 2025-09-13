package com.jpmc.midascore.kafka;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class TransactionListener {
    private static final Logger log = LoggerFactory.getLogger(TransactionListener.class);

    private final List<Transaction> received = new CopyOnWriteArrayList<>();
    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;

    public TransactionListener(UserRepository userRepository, TransactionRepository transactionRepository) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core")
    @Transactional
    public void onMessage(Transaction transaction) {
        received.add(transaction);
        log.info("Received transaction: {}", transaction);
        // Validation
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());
        if (sender == null || recipient == null) {
            log.debug("Discarding transaction - invalid user(s): {}", transaction);
            return; // invalid ids
        }
        if (sender.getBalance() < transaction.getAmount()) {
            log.debug("Discarding transaction - insufficient funds: {} (sender balance: {})", transaction, sender.getBalance());
            return; // insufficient funds
        }
        // Adjust balances
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount());
        userRepository.save(sender);
        userRepository.save(recipient);
        // Persist transaction record
        TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount());
        transactionRepository.save(record);
        log.debug("Persisted transaction record id={} amount={} senderBalanceAfter={} recipientBalanceAfter={}", record.getId(), record.getAmount(), sender.getBalance(), recipient.getBalance());
    }

    public List<Transaction> getReceived() {
        return received;
    }
}

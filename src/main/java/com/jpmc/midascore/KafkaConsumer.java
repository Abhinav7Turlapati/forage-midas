package com.jpmc.midascore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.foundation.Incentive;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class KafkaConsumer {

    private final DatabaseConduit databaseConduit;
    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${incentive.api-url:http://localhost:8080/incentive}")
    private String incentiveApiUrl;

    public KafkaConsumer(DatabaseConduit databaseConduit) {
        this.databaseConduit = databaseConduit;
    }

    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-group"
    )
    public void listen(String message) {

        try {
            ObjectMapper mapper = new ObjectMapper();
            Transaction transaction =
                    mapper.readValue(message, Transaction.class);

            UserRecord sender =
                    databaseConduit.findById(transaction.getSenderId());

            UserRecord recipient =
                    databaseConduit.findById(transaction.getRecipientId());

            if (sender == null || recipient == null) {
                return;
            }

            if (sender.getBalance() < transaction.getAmount()) {
                return;
            }

            float incentiveAmount = 0.0f;
            try {
                Incentive incentiveResponse = restTemplate.postForObject(incentiveApiUrl, transaction, Incentive.class);
                if (incentiveResponse != null) {
                    incentiveAmount = incentiveResponse.getAmount();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            sender.setBalance(
                    sender.getBalance() - transaction.getAmount()
            );

            recipient.setBalance(
                    recipient.getBalance() + transaction.getAmount() + incentiveAmount
            );

            databaseConduit.save(sender);
            databaseConduit.save(recipient);

            TransactionRecord transactionRecord =
                    new TransactionRecord(
                            sender,
                            recipient,
                            transaction.getAmount(),
                            incentiveAmount
                    );

            databaseConduit.save(transactionRecord);

            System.out.println("Processed: " + transaction + " with incentive: " + incentiveAmount);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
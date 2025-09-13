package com.jpmc.midascore.incentive;

import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class IncentiveClient {
    private static final Logger log = LoggerFactory.getLogger(IncentiveClient.class);
    private final RestTemplate restTemplate;
    private final String incentiveUrl;

    public IncentiveClient(RestTemplateBuilder builder,
                           @Value("${incentive.api.url:http://localhost:8080/incentive}") String incentiveUrl) {
        this.restTemplate = builder.build();
        this.incentiveUrl = incentiveUrl;
    }

    public float fetchIncentive(Transaction tx) {
        try {
            ResponseEntity<Incentive> response = restTemplate.postForEntity(incentiveUrl, tx, Incentive.class);
            if (response.getBody() != null) {
                return Math.max(0f, response.getBody().getAmount());
            }
        } catch (Exception e) {
            log.warn("Incentive API call failed, defaulting incentive=0: {}", e.getMessage());
        }
        return 0f;
    }
}


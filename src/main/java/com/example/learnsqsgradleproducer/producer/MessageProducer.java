package com.example.learnsqsgradleproducer.producer;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.*;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.stereotype.Service;

import javax.swing.text.DateFormatter;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
@Log4j2
public class MessageProducer {

    private static final int MAX_BATCH_SEND_SQS = 10;

    @Value("${aws.sqs.queue}")
    private String queueName;

    @Autowired
    private AmazonSQS amazonSQS;

    public void sentToQueue(String message) {

        log.info("Sending message: {}", message);
        SendMessageRequest sendMessageRequest = new SendMessageRequest()
                .withQueueUrl(queueName)
                .withMessageBody(message);
        amazonSQS.sendMessage(sendMessageRequest);

    }

    public void sentToQueue(List<String> messages){
        List<SendMessageBatchRequestEntry> entries = messages.stream()
                .map(message -> new SendMessageBatchRequestEntry(UUID.randomUUID().toString(), message))
                .toList();

        SendMessageBatchRequest sendMessageBatchRequest = new SendMessageBatchRequest()
                .withQueueUrl(queueName)
                .withEntries(entries);
        amazonSQS.sendMessageBatch(sendMessageBatchRequest);
    }

    public void sentToQueueWithAttrs(String message) {

        log.info("Sending message attrs: {}", message);
        Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
        messageAttributes.put("data", new MessageAttributeValue()
                .withStringValue(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")))
                .withDataType("String"));
        messageAttributes.put("cod", new MessageAttributeValue()
                .withStringValue(UUID.randomUUID().toString())
                .withDataType("String"));
        SendMessageRequest sendMessageRequest = new SendMessageRequest()
                .withMessageAttributes(messageAttributes)
                .withQueueUrl(queueName)
                .withMessageBody(message);

        amazonSQS.sendMessage(sendMessageRequest);

    }


}

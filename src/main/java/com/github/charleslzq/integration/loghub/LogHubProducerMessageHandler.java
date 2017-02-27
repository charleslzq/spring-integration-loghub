package com.github.charleslzq.integration.loghub;

import com.github.charleslzq.loghub.listener.LogGroupHeaders;
import com.github.charleslzq.loghub.producer.LogHubTemplate;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.util.StringUtils;

import java.util.Collections;
import java.util.List;

/**
 * Created by Charles on 2/27/2017.
 */
public class LogHubProducerMessageHandler<T> extends AbstractMessageHandler {
    private final LogHubTemplate<T> logHubTemplate;

    public LogHubProducerMessageHandler(LogHubTemplate<T> logHubTemplate) {
        this.logHubTemplate = logHubTemplate;
    }

    protected void handleMessageInternal(Message<?> message) throws Exception {
        String topic = (String)message.getHeaders().get(LogGroupHeaders.TOPIC.getKey());
        List<T> toSend = Collections.singletonList((T)message.getPayload());
        if (StringUtils.hasText(topic)) {
            logHubTemplate.send(topic, toSend);
        } else {
            logHubTemplate.send(toSend);
        }
    }
}

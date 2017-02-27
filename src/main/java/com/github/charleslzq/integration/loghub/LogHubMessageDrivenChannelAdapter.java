package com.github.charleslzq.integration.loghub;

import com.github.charleslzq.loghub.listener.ClientWorkerContainer;
import com.github.charleslzq.loghub.listener.MessageListener;
import org.springframework.integration.context.OrderlyShutdownCapable;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.messaging.Message;

/**
 * Created by Charles on 2/27/2017.
 */
public class LogHubMessageDrivenChannelAdapter<T> extends MessageProducerSupport  implements OrderlyShutdownCapable {
    private final ClientWorkerContainer<T> clientWorkerContainer;
    private final MessageListener<T> messageListener = new IntegrationMessageListener<T>();

    public LogHubMessageDrivenChannelAdapter(ClientWorkerContainer<T> clientWorkerContainer) {
        this.clientWorkerContainer = clientWorkerContainer;
    }

    @Override
    protected void onInit() {
        this.clientWorkerContainer.setMessageListener(messageListener);
    }

    @Override
    protected void doStart() {
        this.clientWorkerContainer.start();
    }

    @Override
    protected void doStop() {
        this.clientWorkerContainer.stop();
    }

    @Override
    public String getComponentType() {
        return "log-hub:message-driven-channel-adapter";
    }

    public int beforeShutdown() {
        this.clientWorkerContainer.stop();
        return this.getPhase();
    }

    public int afterShutdown() {
        return this.getPhase();
    }

    private class IntegrationMessageListener<T> implements MessageListener<T> {

        public void onMessage(Message<T> message) {
            LogHubMessageDrivenChannelAdapter.this.sendMessage(message);
        }
    }
}

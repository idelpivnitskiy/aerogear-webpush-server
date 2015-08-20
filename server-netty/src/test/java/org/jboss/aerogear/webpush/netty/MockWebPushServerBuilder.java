package org.jboss.aerogear.webpush.netty;

import org.jboss.aerogear.webpush.PushMessage;
import org.jboss.aerogear.webpush.Subscription;
import org.jboss.aerogear.webpush.WebPushServer;
import org.jboss.aerogear.webpush.WebPushServerConfig;

import java.util.Collections;
import java.util.Optional;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockWebPushServerBuilder {

    private final WebPushServer webPushServer = mock(WebPushServer.class);
    private final WebPushServerConfig config = mock(WebPushServerConfig.class);
    private final Subscription subscription;

    private MockWebPushServerBuilder(final Subscription subscription) {
        this.subscription = subscription;
        when(webPushServer.subscribe()).thenReturn(subscription);
        when(webPushServer.subscriptionById(subscription.id())).thenReturn(Optional.of(subscription));
        when(config.messageMaxSize()).thenReturn(4096L);
    }

    public MockWebPushServerBuilder subscriptionMaxAge(final long maxAge) {
        when(config.subscriptionMaxAge()).thenReturn(maxAge);
        return this;
    }

    public MockWebPushServerBuilder waitingPushMessage(final PushMessage message) {
        when(webPushServer.waitingDeliveryMessages(subscription.id())).thenReturn(Collections.singletonList(message));
        return this;
    }

    public MockWebPushServerBuilder messageMaxSize(final long maxSize) {
        when(config.messageMaxSize()).thenReturn(maxSize);
        return this;
    }

    public MockWebPushServerBuilder receiptToken(final String token) {
        when(webPushServer.generateEndpointToken(anyString())).thenReturn(token);
        return this;
    }

    public MockWebPushServerBuilder pushToken(final String token) {
        when(webPushServer.generateEndpointToken(anyString(), anyString())).thenReturn(token);
        when(webPushServer.subscriptionByPushToken(token)).thenReturn(Optional.of(subscription));
        return this;
    }

    public WebPushServer build() throws Exception {
        when(webPushServer.config()).thenReturn(config);
        return webPushServer;
    }

    public static MockWebPushServerBuilder withSubscription(final Subscription subscription) {
        return new MockWebPushServerBuilder(subscription);
    }

}

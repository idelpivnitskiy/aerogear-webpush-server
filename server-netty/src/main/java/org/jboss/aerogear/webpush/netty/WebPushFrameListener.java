/**
 * JBoss, Home of Professional Open Source
 * Copyright Red Hat, Inc., and individual contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jboss.aerogear.webpush.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.EmptyHttp2Headers;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2ConnectionEncoder;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FrameAdapter;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Stream;
import io.netty.util.AsciiString;
import io.netty.util.AttributeKey;
import io.netty.util.ByteString;
import io.netty.util.concurrent.Future;
import org.jboss.aerogear.webpush.NewSubscription;
import org.jboss.aerogear.webpush.Registration;
import org.jboss.aerogear.webpush.Resource;
import org.jboss.aerogear.webpush.Subscription;
import org.jboss.aerogear.webpush.WebLink;
import org.jboss.aerogear.webpush.WebPushServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static io.netty.buffer.Unpooled.copiedBuffer;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCESS_CONTROL_EXPOSE_HEADERS;
import static io.netty.handler.codec.http.HttpHeaderNames.CACHE_CONTROL;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderNames.LOCATION;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE;
import static io.netty.util.CharsetUtil.UTF_8;

public class WebPushFrameListener extends Http2FrameAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(WebPushFrameListener.class);

    private static final String WEBPUSH_URI = "/webpush/";
    private static final AsciiString ANY_ORIGIN = new AsciiString("*");
    private static final AsciiString EXPOSE_HEADERS = new AsciiString("Link, Cache-Control, Location");
    private static final AsciiString PUSH_RECEIPT_HEADER = new AsciiString("Push-Receipt");
    private static final AsciiString TTL_HEADER = new AsciiString("TTL");
    private static final AsciiString AGGREGATION_JSON = new AsciiString("application/push-aggregation+json");
    private static final AttributeKey<String> REG_ID = AttributeKey.valueOf("regId");
    private static final AttributeKey<String> SUBSCRIPTION_ID = AttributeKey.valueOf("subscriptionId");
    private static final AsciiString PREFER = new AsciiString("prefer");

    private static final ConcurrentHashMap<String, Optional<Client>> monitoredStreams = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, String> notificationStreams = new ConcurrentHashMap<>();

    private static final String GET = "GET";
    private static final String POST = "POST";
    private static final String DELETE = "DELETE";

    static final AsciiString LINK = new AsciiString("link");

    private final WebPushServer webpushServer;
    private Http2ConnectionEncoder encoder;

    private Http2Connection.PropertyKey pathPropertyKey;
    private Http2Connection.PropertyKey resourcePropertyKey;
    private Http2Connection.PropertyKey pushReceiptPropertyKey;

    public WebPushFrameListener(final WebPushServer webpushServer) {
        Objects.requireNonNull(webpushServer, "webpushServer must not be null");
        this.webpushServer = webpushServer;
    }

    public void encoder(Http2ConnectionEncoder encoder) {
        this.encoder = encoder;
        Http2Connection connection = encoder.connection();
        pathPropertyKey = connection.newKey();
        resourcePropertyKey = connection.newKey();
        pushReceiptPropertyKey = connection.newKey();
    }

    @Override
    public void onHeadersRead(final ChannelHandlerContext ctx,
                              final int streamId,
                              final Http2Headers headers,
                              final int streamDependency,
                              final short weight,
                              final boolean exclusive,
                              final int padding,
                              final boolean endStream) throws Http2Exception {
        final String path = headers.path().toString();
        final String method = headers.method().toString();
        LOGGER.info("onHeadersRead. streamId={}, method={}, path={}, endstream={}", streamId, method, path, endStream);

        Resource resource = getResource(path);
        Http2Stream stream = encoder.connection().stream(streamId);
        stream.setProperty(pathPropertyKey, path);
        stream.setProperty(resourcePropertyKey, resource);
        switch (method) {
            case GET:   //FIXME
                if (path.contains(Resource.REGISTRATION.resourceName())) {
                    handleMonitor(ctx, path, streamId, padding, headers);
                }
                break;
            case POST:
                switch (resource) {
                    case SUBSCRIBE:
                        handleNewSubscribe(ctx, streamId);
                        return;
                    case RECEIPTS:
                        handleReceipts(ctx, streamId, path);
                        return;
                    case PUSH:
                        Optional<String> pushReceiptToken = getPushReceiptToken(headers);
                        stream.setProperty(pushReceiptPropertyKey, pushReceiptToken);
                        //see onDataRead(...) method
                        return;
                }
                break;
            case DELETE:
                switch (resource) {
                    case PUSH_MESSAGE:
                        handleAcknowledgement(ctx, streamId, path);
                        return;
                    case SUBSCRIPTION:
                        handlePushMessageSubscriptionRemoval(ctx, path, streamId);
                        return;
                    case RECEIPT:
                        handleReceiptSubscriptionRemoval(ctx, path, streamId);
                        return;
                }
                break;
        }
    }

    @Override
    public int onDataRead(final ChannelHandlerContext ctx,
                          final int streamId,
                          final ByteBuf data,
                          final int padding,
                          final boolean endOfStream) throws Http2Exception {
        Http2Stream stream = encoder.connection().stream(streamId);
        String path = stream.getProperty(pathPropertyKey);
        Resource resource = stream.getProperty(resourcePropertyKey);
        LOGGER.info("onDataRead. streamId={}, path={}, resource={}, endstream={}", streamId, path, resource,
                endOfStream);
        switch (resource) {
            case PUSH:
                handlePush(ctx, streamId, path, data, padding); //FIXME rename to handleNotify
                break;
            default:    //FIXME
                handleNotification(ctx, streamId, data, padding, path);
                break;
        }
        return super.onDataRead(ctx, streamId, data, padding, endOfStream);
    }

    private static Resource getResource(String path) {
        String resourceName;
        int idx = path.indexOf('/', WEBPUSH_URI.length());
        if (idx > 0) {
            resourceName = path.substring(WEBPUSH_URI.length(), idx);
        } else {
            resourceName = path.substring(WEBPUSH_URI.length());
        }
        return Resource.byResourceName(resourceName);
    }

    private static Optional<String> getPushReceiptToken(Http2Headers headers) {
        ByteString byteString = headers.get(PUSH_RECEIPT_HEADER);
        if (byteString != null) {
            return extractToken(byteString.toString(), Resource.RECEIPT);
        }
        return Optional.empty();
    }

    private void handlePush(ChannelHandlerContext ctx, int streamId, String path, ByteBuf data, int padding) {
        Optional<String> pushToken = extractToken(path);
        pushToken.ifPresent(pt -> {
            Optional<NewSubscription> subscription = webpushServer.subscriptionByPushToken(pushToken.get());
            subscription.ifPresent(sub -> {
                Http2Stream stream = encoder.connection().stream(streamId);
                Optional<String> receiptToken = stream.getProperty(pushReceiptPropertyKey);
                if (receiptToken.isPresent()) {
                    Optional<NewSubscription> receiptSub = webpushServer.subscriptionByReceiptToken(receiptToken.get());
                    if (!receiptSub.isPresent() || !subscription.equals(receiptSub)) {
                        throw new RuntimeException("Subscriptions mismatched");
                    }
                }
                final int readableBytes = data.readableBytes();
                if (readableBytes > webpushServer.config().messageMaxSize()) {
                    encoder.writeHeaders(ctx, streamId, messageToLarge(), 0, true, ctx.newPromise());
                } else {
                    String pushMessageId = UUID.randomUUID().toString();
                    String pushMessageToken = webpushServer.generateEndpointToken(pushMessageId, sub.id());
                    //TODO save message and push to user agent
                    encoder.writeHeaders(ctx, streamId, pushMessageHeaders(pushMessageToken), 0, true, ctx.newPromise());
                    LOGGER.info("Push Message Resource: {}", pushMessageToken);
                }
            });
        });
    }

    private static Http2Headers pushMessageHeaders(String pushMessageToken) {
        return resourceHeaders(Resource.PUSH_MESSAGE, pushMessageToken);
    }

    public void shutdown() {
        monitoredStreams.entrySet().stream().forEach(kv -> kv.getValue().ifPresent(client -> client.ctx.close()));
        monitoredStreams.clear();
        notificationStreams.clear();
    }

    public void disconnect(final ChannelHandlerContext ctx) {
        final Optional<String> regId = Optional.ofNullable(ctx.attr(REG_ID).get());
        if (regId.isPresent()) {
            final Optional<Client> removed = monitoredStreams.remove(regId.get());
            if (removed != null && removed.isPresent()) {
                final Client client = removed.get();
                LOGGER.info("Removed client regId{}", client);
            }
        }
        LOGGER.info("Disconnected channel {}", ctx.channel().id());
    }

    private void handleNotification(final ChannelHandlerContext ctx,
                                    final int streamId,
                                    final ByteBuf data,
                                    final int padding,
                                    final String path) {
        final int readableBytes = data.readableBytes();
        if (readableBytes > webpushServer.config().messageMaxSize()) {
            encoder.writeHeaders(ctx, streamId, messageToLarge(), 0, true, ctx.newPromise());
        } else {
            final String endpoint = extractEndpointToken(path);
            handleNotify(endpoint, data, padding, en ->
                            en.writeHeaders(ctx, streamId, acceptedHeaders(), 0, true, ctx.newPromise())
            );
//            FIXME
//            Optional.ofNullable(aggregateChannels.get(endpoint)).ifPresent(agg ->
//                            agg.subscriptions().stream().forEach(entry -> handleNotify(entry.endpoint(), data.copy(), padding, e -> {
//                            }))
//            );
        }
    }

    private void handleNotify(final String endpoint,
                              final String data,
                              final int padding,
                              final Consumer<Http2ConnectionEncoder> consumer) {
        handleNotify(endpoint, copiedBuffer(data, UTF_8), padding, consumer);
    }

    private void handleNotify(final String endpoint,
                              final ByteBuf data,
                              final int padding,
                              final Consumer<Http2ConnectionEncoder> consumer) {
        final Optional<String> optionalRegId = subscriptionRegIdForEndpoint(endpoint);
        if (optionalRegId.isPresent()) {
            final Optional<Client> optionalClient = clientForRegId(optionalRegId.get());
            if (optionalClient.isPresent()) {
                final Client client = optionalClient.get();
                LOGGER.info("Handle notification {} payload {}", client, data.toString(UTF_8));
                if (!client.isHeadersSent()) {
                    client.encoder.writeHeaders(client.ctx, client.streamId, EmptyHttp2Headers.INSTANCE, 0, false, client.ctx.newPromise())
                            .addListener(WebPushFrameListener::logFutureError);
                    client.headersSent();
                }
                client.encoder.writeData(client.ctx, client.streamId, data.retain(), padding, false, client.ctx.newPromise())
                        .addListener(WebPushFrameListener::logFutureError);
                webpushServer.setMessage(endpoint, Optional.empty());
            } else {
                webpushServer.setMessage(endpoint, Optional.of(data.toString(UTF_8)));
                consumer.accept(encoder);
            }
        }
    }

    private static Optional<String> subscriptionRegIdForEndpoint(final String endpointToken) {
        return Optional.ofNullable(notificationStreams.get(endpointToken));
    }

    private static Optional<Client> clientForRegId(final String regId) {
        return Optional.ofNullable(monitoredStreams.get(regId)).orElse(Optional.empty());

    }

    private static void logFutureError(final Future future) {
        if (!future.isSuccess()) {
            LOGGER.error("ChannelFuture failed. Cause:", future.cause());
        }
    }

    private void handleNewSubscribe(final ChannelHandlerContext ctx, final int streamId) {
        NewSubscription subscription = webpushServer.newSubscription();
        ctx.attr(SUBSCRIPTION_ID).set(subscription.id());
        encoder.writeHeaders(ctx, streamId, subscriptionHeaders(subscription), 0, true, ctx.newPromise());
        LOGGER.info("Subscription for Push Messages: {}", subscription);
    }

    private Http2Headers subscriptionHeaders(NewSubscription subscription) {
        String pushToken = webpushServer.generateEndpointToken(subscription.pushResourceId(), subscription.id());
        String receiptsToken = webpushServer.generateEndpointToken(subscription.id());
        return resourceHeaders(Resource.SUBSCRIPTION, subscription.id())
                .set(ACCESS_CONTROL_EXPOSE_HEADERS, EXPOSE_HEADERS)
                .set(LINK, asLink(webpushUri(Resource.PUSH, pushToken), WebLink.PUSH),
                        asLink(webpushUri(Resource.RECEIPTS, receiptsToken), WebLink.RECEIPTS))
                .set(CACHE_CONTROL, privateCacheWithMaxAge(webpushServer.config().registrationMaxAge()));
    }

    private void handleReceipts(ChannelHandlerContext ctx, int streamId, String path) {
        Optional<String> receiptsToken = extractToken(path);
        receiptsToken.ifPresent(e -> {
            String subscriptionToken = receiptsToken.get();
            Optional<NewSubscription> subscription = webpushServer.getSubscription(subscriptionToken);
            subscription.ifPresent(sub -> {
                String receiptResourceId = UUID.randomUUID().toString();   //FIXME need save?
                String receiptResourceToken = webpushServer.generateEndpointToken(receiptResourceId, sub.id());
                encoder.writeHeaders(ctx, streamId, receiptsHeaders(receiptResourceToken), 0, true, ctx.newPromise());
                LOGGER.info("Receipt Subscription Resource: {}", receiptResourceToken);
            });
        });
    }

    private static Http2Headers receiptsHeaders(String receiptResourceToken) {
        return resourceHeaders(Resource.RECEIPT, receiptResourceToken);
    }

    private void handleAcknowledgement(ChannelHandlerContext ctx, int streamId, String path) {
        //TODO the push server MUST deliver a response to the application server monitoring the receipt subscription resource
    }

    private void handlePushMessageSubscriptionRemoval(final ChannelHandlerContext ctx, final String path,
            final int streamId) {
        final String endpointToken = extractEndpointToken(path);
        final Optional<Subscription> subscription = webpushServer.subscription(endpointToken);
        if (subscription.isPresent()) {
            webpushServer.removeSubscription(subscription.get());
            notificationStreams.remove(endpointToken);
            encoder.writeHeaders(ctx, streamId, okHeaders(), 0, true, ctx.newPromise());
        } else {
            encoder.writeHeaders(ctx, streamId, notFoundHeaders(), 0, true, ctx.newPromise());
        }
    }

    private void handleReceiptSubscriptionRemoval(final ChannelHandlerContext ctx, final String path,
            final int streamId) {
        //TODO handleReceiptSubscriptionRemoval
    }

    private static Http2Headers resourceHeaders(Resource resource, String resourceToken) {
        return new DefaultHttp2Headers(false)
                .status(CREATED.codeAsText())
                .set(ACCESS_CONTROL_ALLOW_ORIGIN, ANY_ORIGIN)
                .set(LOCATION, webpushUri(resource, resourceToken));
    }

    private static AsciiString webpushUri(Resource resource, String id) {
        return new AsciiString(WEBPUSH_URI + resource.resourceName() + "/" + id);
    }

    private static AsciiString asLink(AsciiString uri, WebLink rel) {
        return new AsciiString("<" + uri + ">;rel=\"" + rel + "\"");
    }

    private static Optional<String> extractToken(String path, Resource resource) {
        String segment = WEBPUSH_URI + resource.resourceName();
        int idx = path.indexOf(segment);
        if (idx < 0) {
            return Optional.empty();
        }
        String subpath = path.substring(idx + segment.length());
        return extractToken(subpath);
    }

    private static Optional<String> extractToken(String path) {
        int idx = path.lastIndexOf('/');
        if (idx < 0) {
            return Optional.empty();
        }
        return Optional.of(path.substring(idx + 1));
    }

    private Http2Headers acceptedHeaders() {
        return new DefaultHttp2Headers(false)
                .status(OK.codeAsText())
                .set(ACCESS_CONTROL_ALLOW_ORIGIN, ANY_ORIGIN)
                .set(CACHE_CONTROL, privateCacheWithMaxAge(webpushServer.config().messageMaxAge()));
    }

    private static Http2Headers messageToLarge() {
        return new DefaultHttp2Headers(false)
                .status(REQUEST_ENTITY_TOO_LARGE.codeAsText())
                .set(ACCESS_CONTROL_ALLOW_ORIGIN, ANY_ORIGIN);
    }

    private Http2Headers createdHeaders(final Subscription subscription) {
        return new DefaultHttp2Headers(false)
                .status(CREATED.codeAsText())
                .set(LOCATION, new AsciiString(WEBPUSH_URI + subscription.endpoint()))
                .set(ACCESS_CONTROL_ALLOW_ORIGIN, ANY_ORIGIN)
                .set(ACCESS_CONTROL_EXPOSE_HEADERS, new AsciiString("Location"))
                .set(CACHE_CONTROL, privateCacheWithMaxAge(webpushServer.config().subscriptionMaxAge()));
    }

    /**
     * Returns a cache-control value with this private and has the specified maxAge.
     *
     * @param maxAge the max age in seconds.
     * @return {@link AsciiString} the value for a cache-control header.
     */
    private static AsciiString privateCacheWithMaxAge(final long maxAge) {
        return new AsciiString("private, max-age=" + maxAge);
    }

    /*
      A monitor request is responded to with a push promise. A push promise is associated with a
      previous client-initiated request (the monitor request)
     */
    private void handleMonitor(final ChannelHandlerContext ctx,
                               final String path,
                               final int streamId,
                               final int padding,
                               final Http2Headers headers) {
        final Optional<Registration> registration = extractRegistrationId(path, Resource.REGISTRATION.resourceName())
                .flatMap(webpushServer::registration);
        registration.ifPresent(reg -> {
            final int pushStreamId = encoder.connection().local().nextStreamId();
            final Client client = new Client(ctx, pushStreamId, encoder);
            monitoredStreams.put(reg.id(), Optional.of(client));
            encoder.writePushPromise(ctx, streamId, pushStreamId, monitorHeaders(reg), 0, ctx.newPromise());
            LOGGER.info("Monitor ctx={}, registrationId={}, pushPromiseStreamId={}, headers={}", ctx, reg.id(), pushStreamId, monitorHeaders(reg));
            final Optional<ByteString> wait = Optional.ofNullable(headers.get(PREFER))
                                                      .filter(val -> "wait=0".equals(val.toString()));
            wait.ifPresent(s ->
                            notificationStreams.entrySet().stream().filter(kv -> kv.getValue().equals(reg.id())).forEach(e -> {
                                final String endpoint = e.getKey();
                                final Optional<Subscription> sub = webpushServer.subscription(endpoint).filter(ch -> ch.message().isPresent());
                                sub.ifPresent(ch -> handleNotify(endpoint, ch.message().get(), padding, q -> {
                                }));
                            })
            );
        });
    }

    private static Http2Headers noContentHeaders() {
        return new DefaultHttp2Headers(false)
                .status(NO_CONTENT.codeAsText())
                .set(ACCESS_CONTROL_ALLOW_ORIGIN, ANY_ORIGIN);
    }

    private static Http2Headers notFoundHeaders() {
        return new DefaultHttp2Headers(false)
                .status(NOT_FOUND.codeAsText())
                .set(ACCESS_CONTROL_ALLOW_ORIGIN, ANY_ORIGIN);
    }

    private Http2Headers monitorHeaders(final Registration registration) {
        return new DefaultHttp2Headers(false)
                .status(OK.codeAsText())
                .set(ACCESS_CONTROL_ALLOW_ORIGIN, ANY_ORIGIN)
                .set(ACCESS_CONTROL_EXPOSE_HEADERS, new AsciiString("Link, Cache-Control"))
//                .set(LINK, asLink(registration.subscribeUri(), WebLink.SUBSCRIBE.toString()),
//                        asLink(registration.aggregateUri(), WebLink.AGGREGATE.toString()))
                .set(CACHE_CONTROL, privateCacheWithMaxAge(webpushServer.config().registrationMaxAge()));
    }

    private static Http2Headers okHeaders() {
        return new DefaultHttp2Headers(false)
                .status(OK.codeAsText())
                .set(ACCESS_CONTROL_ALLOW_ORIGIN, ANY_ORIGIN)
                .set(ACCESS_CONTROL_EXPOSE_HEADERS, CONTENT_TYPE);
    }

    private static Optional<String> extractRegistrationId(final String path, final String segment) {
        try {
            final String subpath = path.substring(path.indexOf(segment) + segment.length() + 1);
            return Optional.of(subpath.subSequence(subpath.lastIndexOf('/') + 1, subpath.length()).toString());
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private static String extractEndpointToken(final String path) {
        return path.substring(path.lastIndexOf('/') + 1);
    }

    private static class Client {

        private final ChannelHandlerContext ctx;
        private final Http2ConnectionEncoder encoder;
        private final int streamId;
        private volatile boolean headersSent;

        Client(final ChannelHandlerContext ctx, final int streamId, final Http2ConnectionEncoder encoder) {
            this.ctx = ctx;
            this.streamId = streamId;
            this.encoder = encoder;
        }

        boolean isHeadersSent() {
            return headersSent;
        }

        void headersSent() {
            headersSent = true;
        }

        @Override
        public String toString() {
            return "Client[streamid=" + streamId + ", ctx=" + ctx + ", headersSent=" + headersSent + "]";
        }
    }
}

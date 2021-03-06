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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ApplicationProtocolNegotiationHandler;
import org.jboss.aerogear.webpush.WebPushServer;


/**
 * Negotiates with the browser if HTTP2 or HTTP is going to be used. Once decided, the Netty
 * pipeline is setup with the correct handlers for the selected protocol.
 */
public class Http2OrHttpHandler extends ApplicationProtocolNegotiationHandler {

    private final WebPushServer webPushServer;

    public Http2OrHttpHandler(final WebPushServer webPushServer) {
        super(ApplicationProtocolNames.HTTP_1_1);
        this.webPushServer = webPushServer;
    }

    @Override
    protected void configurePipeline(final ChannelHandlerContext ctx, final String protocol) throws Exception {
        if (ApplicationProtocolNames.HTTP_2.equals(protocol)) {
            ctx.pipeline().addLast(new WebPushHttp2HandlerBuilder()
                    .webPushServer(webPushServer)
                    .build());
            return;
        }
        if (ApplicationProtocolNames.HTTP_1_1.equals(protocol)) {
            ctx.pipeline().addLast(new WebPushHttp11Handler(webPushServer));
            return;
        }
        throw new IllegalStateException("unknown protocol: " + protocol);
    }

}

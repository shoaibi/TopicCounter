/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.company.product.test.http;

import com.company.product.test.queue.MessageProducer;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.ReferenceCountUtil;
import kafka.common.InvalidTopicException;

import java.util.List;
import java.util.Map;

import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.*;

public class HttpServerHandler extends SimpleChannelInboundHandler<Object> {

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
        // added for forward compatibility with 5.0 when channelRead0 would be renamed to messageReceived
        messageReceived(ctx, msg);
    }

    protected void messageReceived(ChannelHandlerContext ctx, Object msg) {
        FullHttpResponse response = null;
        try {
            if (msg instanceof HttpRequest) {
                HttpRequest request = (HttpRequest) msg;

                QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.getUri());
                Map<String, List<String>> params = queryStringDecoder.parameters();
                validateQueryString(params);
                compileAndSendMessage(params);

                // no need to provide a response, we did what was expected of us.
                response = new DefaultFullHttpResponse(HTTP_1_1, NO_CONTENT);
            }
        } catch (IllegalArgumentException iae) {
            response = new DefaultFullHttpResponse(HTTP_1_1, INTERNAL_SERVER_ERROR);
            throw iae;
        } catch (InvalidTopicException ite) {
            response = new DefaultFullHttpResponse(HTTP_1_1, BAD_REQUEST);
            throw ite;
        } finally {
            ctx.write(response);
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }

    /**
     * Given a queryString Map, validate that it has both topic and message keys set
     * @param qs
     * @throws IllegalArgumentException
     */
    private void validateQueryString(Map<String, List<String>> qs) throws IllegalArgumentException {
        if (!qs.isEmpty()) {

            if (qs.size() > 2) {
                throw new IllegalArgumentException("Please provide only 2 query parameters");
            }

            if (!qs.containsKey("topic") || !qs.containsKey("message")) {
                throw new IllegalArgumentException("Please provide both parameters, topic and message");
            }

        } else {
            throw new IllegalArgumentException("Missing topic and message query parameters");
        }
    }

    /**
     * Provided a queryString map, extract topic and message and invoke Producer
     * @param qs
     * @throws InvalidTopicException
     */
    private void compileAndSendMessage(Map<String, List<String>> qs) throws InvalidTopicException {
        String topic = qs.get("topic").get(0);
        String message = qs.get("message").get(0);
        MessageProducer.sendMessage(topic, message);
    }
}

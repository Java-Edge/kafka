/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;

import static org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS;

public class RequestContext implements AuthorizableRequestContext {
    // 请求头数据：一些用户不可见元数据信息，etc. Request类型、Request API版本、clientId
    public final RequestHeader header;
    // 请求发送方的TCP连接串标识，由Kafka根据一定规则定义，主要用于表示TCP连接
    public final String connectionId;
    // 请求发送方IP地址
    public final InetAddress clientAddress;
    // Kafka用户认证类，用于认证授权
    public final KafkaPrincipal principal;
    // 监听器名称，可以是预定义的监听器（如PLAINTEXT），也可自行定义
    public final ListenerName listenerName;
    // 安全协议类型，目前支持4种：PLAINTEXT、SSL、SASL_PLAINTEXT、SASL_SSL
    public final SecurityProtocol securityProtocol;
    // 用户自定义的一些连接方信息
    public final ClientInformation clientInformation;

    public RequestContext(RequestHeader header,
                          String connectionId,
                          InetAddress clientAddress,
                          KafkaPrincipal principal,
                          ListenerName listenerName,
                          SecurityProtocol securityProtocol,
                          ClientInformation clientInformation) {
        this.header = header;
        this.connectionId = connectionId;
        this.clientAddress = clientAddress;
        this.principal = principal;
        this.listenerName = listenerName;
        this.securityProtocol = securityProtocol;
        this.clientInformation = clientInformation;
    }

    // 从给定的ByteBuffer中提取出Request和对应的Size值
    public RequestAndSize parseRequest(ByteBuffer buffer) {
        if (isUnsupportedApiVersionsRequest()) {
            // 不支持的ApiVersions请求类型被视为是V0版本的请求，并且不做解析操作，直接返回
            ApiVersionsRequest apiVersionsRequest = new ApiVersionsRequest(new ApiVersionsRequestData(), (short) 0, header.apiVersion());
            return new RequestAndSize(apiVersionsRequest, 0);
        } else {
            // 从请求头数据获取ApiKeys
            ApiKeys apiKey = header.apiKey();
            try {
                // 从请求头数据获取版本信息
                short apiVersion = header.apiVersion();
                // 解析请求
                Struct struct = apiKey.parseRequest(apiVersion, buffer);
                AbstractRequest body = AbstractRequest.parseRequest(apiKey, apiVersion, struct);
                // 封装解析后的请求对象以及请求大小返回
                return new RequestAndSize(body, struct.sizeOf());
            } catch (Throwable ex) {
                // 解析过程中出现任何问题都视为无效请求，直接抛异常
                throw new InvalidRequestException("Error getting request for apiKey: " + apiKey +
                        ", apiVersion: " + header.apiVersion() +
                        ", connectionId: " + connectionId +
                        ", listenerName: " + listenerName +
                        ", principal: " + principal, ex);
            }
        }
    }

    public Send buildResponse(AbstractResponse body) {
        ResponseHeader responseHeader = header.toResponseHeader();
        return body.toSend(connectionId, responseHeader, apiVersion());
    }

    private boolean isUnsupportedApiVersionsRequest() {
        return header.apiKey() == API_VERSIONS && !API_VERSIONS.isVersionSupported(header.apiVersion());
    }

    public short apiVersion() {
        // Use v0 when serializing an unhandled ApiVersion response
        if (isUnsupportedApiVersionsRequest())
            return 0;
        return header.apiVersion();
    }

    @Override
    public String listenerName() {
        return listenerName.value();
    }

    @Override
    public SecurityProtocol securityProtocol() {
        return securityProtocol;
    }

    @Override
    public KafkaPrincipal principal() {
        return principal;
    }

    @Override
    public InetAddress clientAddress() {
        return clientAddress;
    }

    @Override
    public int requestType() {
        return header.apiKey().id;
    }

    @Override
    public int requestVersion() {
        return header.apiVersion();
    }

    @Override
    public String clientId() {
        return header.clientId();
    }

    @Override
    public int correlationId() {
        return header.correlationId();
    }
}

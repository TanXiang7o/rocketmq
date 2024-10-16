/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.broker.processor;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.PollingInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
@RunWith(MockitoJUnitRunner.class)
public class PollingInfoProcessorTest {
    private PollingInfoProcessor pollingInfoProcessor;

    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(), new MessageStoreConfig());

    @Mock
    private TopicConfigManager topicConfigManager;

    @Mock
    private SubscriptionGroupManager subscriptionGroupManager;

    @Mock
    private ChannelHandlerContext handlerContext;

    @Mock
    private Channel channel;

    @Before
    public void init() {
        pollingInfoProcessor = new PollingInfoProcessor(brokerController);
        when(brokerController.getTopicConfigManager()).thenReturn(topicConfigManager);
        TopicConfig topicConfig = new TopicConfig("Topic");
        when(topicConfigManager.selectTopicConfig(anyString())).thenReturn(topicConfig);
        when(brokerController.getSubscriptionGroupManager()).thenReturn(subscriptionGroupManager);
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName("TopicGroup");
        when(subscriptionGroupManager.findSubscriptionGroupConfig(anyString())).thenReturn(subscriptionGroupConfig);
        when(handlerContext.channel()).thenReturn(channel);
    }

    @Test
    public void testProcessRequest() throws RemotingCommandException {
        RemotingCommand request = createPollingInfoRequest("TopicGroup", "topic", 0);
        RemotingCommand response = pollingInfoProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testNoPermission() throws RemotingCommandException {
        RemotingCommand request = createPollingInfoRequest("TopicGroup", "topic", 0);
        brokerController.getBrokerConfig().setBrokerPermission(PermName.PERM_INHERIT);
        RemotingCommand response = pollingInfoProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.NO_PERMISSION);
    }

    @Test
    public void testQueueIdIllegal() throws RemotingCommandException {
        RemotingCommand request = createPollingInfoRequest("TopicGroup", "topic", 17);
        RemotingCommand response = pollingInfoProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
    }

    @Test
    public void testSubscriptionGroupNotExist() throws RemotingCommandException {
        when(subscriptionGroupManager.findSubscriptionGroupConfig(anyString())).thenReturn(null);
        RemotingCommand request = createPollingInfoRequest("TopicGroup", "topic", 0);
        RemotingCommand response = pollingInfoProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
    }

    @Test
    public void testTopicNotExist() throws RemotingCommandException {
        when(topicConfigManager.selectTopicConfig(anyString())).thenReturn(null);
        RemotingCommand request = createPollingInfoRequest("TopicGroup", "topic", 0);
        RemotingCommand response = pollingInfoProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.TOPIC_NOT_EXIST);
    }

    public RemotingCommand createPollingInfoRequest(String group, String topic, int queueId) {
        PollingInfoRequestHeader pollingInfoRequestHeader = new PollingInfoRequestHeader();
        pollingInfoRequestHeader.setConsumerGroup(group);
        pollingInfoRequestHeader.setTopic(topic);
        pollingInfoRequestHeader.setQueueId(queueId);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.POLLING_INFO, pollingInfoRequestHeader);
        request.makeCustomHeaderToNet();
        return request;
    }
}

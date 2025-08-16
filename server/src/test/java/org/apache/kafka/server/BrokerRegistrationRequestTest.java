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

package org.apache.kafka.server;

import kafka.server.ControllerInformation;
import kafka.server.ControllerNodeProvider;
import kafka.server.ControllerServer;
import kafka.server.NodeToControllerChannelManagerImpl;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.BrokerRegistrationRequest;
import org.apache.kafka.common.requests.BrokerRegistrationResponse;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.ControllerRequestCompletionHandler;
import org.apache.kafka.server.common.Feature;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.MetadataVersionTestUtils;
import org.apache.kafka.server.common.NodeToControllerChannelManager;

import org.junit.jupiter.api.Assertions;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import scala.Option;

/**
 * This test simulates a broker registering with the KRaft quorum under different configurations.
 */
public class BrokerRegistrationRequestTest {

    public NodeToControllerChannelManager brokerToControllerChannelManager(ClusterInstance clusterInstance, Metrics metrics) {
        var controllerSocketServer = clusterInstance.controllers().values().stream()
            .map(ControllerServer::socketServer)
            .findFirst()
            .orElseThrow();

        return new NodeToControllerChannelManagerImpl(
                new TestControllerNodeProvider(clusterInstance),
                Time.SYSTEM,
                metrics,
                controllerSocketServer.config(),
                "heartbeat",
                "test-heartbeat-",
                10000L
        );
    }

    @SuppressWarnings("unchecked")
    public <T extends AbstractRequest, R extends AbstractResponse> R sendAndReceive(
        NodeToControllerChannelManager channelManager,
        AbstractRequest.Builder<T> reqBuilder,
        int timeoutMs
    ) throws Exception {
        var responseFuture = new CompletableFuture<R>();
        channelManager.sendRequest(reqBuilder, new ControllerRequestCompletionHandler() {
            @Override
            public void onTimeout() {
                responseFuture.completeExceptionally(new TimeoutException());
            }

            @Override
            public void onComplete(ClientResponse response) {
                responseFuture.complete((R) response.responseBody());
            }
        });
        return responseFuture.get(timeoutMs, TimeUnit.MILLISECONDS);
    }

    public Errors registerBroker(
        NodeToControllerChannelManager channelManager,
        String clusterId,
        int brokerId,
        Long zkEpoch,
        FeatureLevel featureLevelToSend
    ) throws Exception {
        var features = new BrokerRegistrationRequestData.FeatureCollection();

        if (featureLevelToSend != null) {
            features.add(new BrokerRegistrationRequestData.Feature()
                    .setName(MetadataVersion.FEATURE_NAME)
                    .setMinSupportedVersion(featureLevelToSend.min())
                    .setMaxSupportedVersion(featureLevelToSend.max())
            );
        }
        
        Feature.PRODUCTION_FEATURES.stream()
            .filter(feature -> !feature.featureName().equals(MetadataVersion.FEATURE_NAME))
            .forEach(feature -> features.add(new BrokerRegistrationRequestData.Feature()
                .setName(feature.featureName())
                .setMinSupportedVersion(feature.minimumProduction())
                .setMaxSupportedVersion(feature.latestTesting())));

        var listener = new BrokerRegistrationRequestData.Listener()
                .setName("EXTERNAL")
                .setHost("example.com")
                .setPort(8082)
                .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id);
        
        var req = new BrokerRegistrationRequestData()
            .setBrokerId(brokerId)
            .setLogDirs(List.of(Uuid.randomUuid()))
            .setClusterId(clusterId)
            .setIncarnationId(Uuid.randomUuid())
            .setIsMigratingZkBroker(zkEpoch != null)
            .setFeatures(features)
            .setListeners(new BrokerRegistrationRequestData.ListenerCollection(List.of(listener).iterator()));

        BrokerRegistrationResponse resp = this.sendAndReceive(
            channelManager, 
            new BrokerRegistrationRequest.Builder(req), 
            30000
        );
        return Errors.forCode(resp.data().errorCode());
    }

    @ClusterTest(types = {Type.KRAFT}, controllers = 1, metadataVersion = MetadataVersion.IBP_3_3_IV3)
    public void shouldRejectZkMigratingBrokerWhenFeatureLevelDoesNotSupportMigration(ClusterInstance clusterInstance) throws Exception {
        var clusterId = clusterInstance.clusterId();
        var metrics = new Metrics();
        var channelManager = brokerToControllerChannelManager(clusterInstance, metrics);
        try {
            channelManager.start();
            Assertions.assertEquals(
                Errors.BROKER_ID_NOT_REGISTERED,
                registerBroker(channelManager, clusterId, 100, 1L, 
                    new FeatureLevel(MetadataVersionTestUtils.IBP_3_3_IV0_FEATURE_LEVEL, MetadataVersion.IBP_3_3_IV3.featureLevel()))
            );
        } finally {
            channelManager.shutdown();
            metrics.close();
        }
    }

    @ClusterTest(types = {Type.KRAFT}, controllers = 1, metadataVersion = MetadataVersion.IBP_3_3_IV3)
    public void shouldRejectRegistrationWithoutFeatureLevels(ClusterInstance clusterInstance) throws Exception {
        var clusterId = clusterInstance.clusterId();
        var metrics = new Metrics();
        var channelManager = brokerToControllerChannelManager(clusterInstance, metrics);
        try {
            channelManager.start();
            Assertions.assertEquals(
                Errors.INVALID_REGISTRATION,
                registerBroker(channelManager, clusterId, 100, null, null)
            );
        } finally {
            channelManager.shutdown();
            metrics.close();
        }
    }

    @ClusterTest(types = {Type.KRAFT}, controllers = 1, metadataVersion = MetadataVersion.IBP_3_3_IV3)
    public void shouldRejectRegistrationWhenFeatureLevelTooHigh(ClusterInstance clusterInstance) throws Exception {
        var clusterId = clusterInstance.clusterId();
        var metrics = new Metrics();
        var channelManager = brokerToControllerChannelManager(clusterInstance, metrics);
        try {
            channelManager.start();
            Assertions.assertEquals(
                Errors.UNSUPPORTED_VERSION,
                registerBroker(channelManager, clusterId, 100, null,
                    new FeatureLevel(MetadataVersion.IBP_3_4_IV0.featureLevel(), MetadataVersion.IBP_3_4_IV0.featureLevel()))
            );
        } finally {
            channelManager.shutdown();
            metrics.close();
        }
    }

    @ClusterTest(types = {Type.KRAFT}, controllers = 1, metadataVersion = MetadataVersion.IBP_3_3_IV3)
    public void shouldRegisterWhenSupportedRangeAndNotMigrating(ClusterInstance clusterInstance) throws Exception {
        var clusterId = clusterInstance.clusterId();
        var metrics = new Metrics();
        var channelManager = brokerToControllerChannelManager(clusterInstance, metrics);
        try {
            channelManager.start();
            Assertions.assertEquals(
                Errors.NONE,
                registerBroker(channelManager, clusterId, 100, null, 
                    new FeatureLevel(MetadataVersion.IBP_3_3_IV3.featureLevel(), MetadataVersion.IBP_3_4_IV0.featureLevel()))
            );
        } finally {
            channelManager.shutdown();
            metrics.close();
        }
    }

    public record FeatureLevel(short min, short max) { }

    private record TestControllerNodeProvider(ClusterInstance clusterInstance)
            implements ControllerNodeProvider {

        public Optional<Node> node() {
            return Optional.of(new Node(
                    clusterInstance.controllers().keySet().iterator().next(),
                    "127.0.0.1",
                    clusterInstance.controllerBoundPorts().get(0)
            ));
        }
        
        public ListenerName listenerName() {
            return clusterInstance.controllerListenerName();
        }

        public SecurityProtocol securityProtocol() {
            return SecurityProtocol.PLAINTEXT;
        }

        public String saslMechanism() {
            return "";
        }

        @Override
        public ControllerInformation getControllerInfo() {
            return ControllerInformation.apply(
                    Option.apply(node().orElse(null)),
                    listenerName(),
                    securityProtocol(),
                    saslMechanism()
            );
        }
    }
}

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

package kafka.server;

import kafka.network.SocketServer;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.BrokerRegistrationRequest;
import org.apache.kafka.common.requests.BrokerRegistrationResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
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

    public NodeToControllerChannelManager brokerToControllerChannelManager(ClusterInstance clusterInstance) {
        var controllerSocketServer = clusterInstance.controllers().values().stream()
            .map(ControllerServer::socketServer)
            .findFirst()
            .orElseThrow();

        return new NodeToControllerChannelManagerImpl(
                new TestControllerNodeProvider(controllerSocketServer, clusterInstance),
                Time.SYSTEM,
                new Metrics(),
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

        var req = new BrokerRegistrationRequestData()
            .setBrokerId(brokerId)
            .setLogDirs(List.of(Uuid.randomUuid()))
            .setClusterId(clusterId)
            .setIncarnationId(Uuid.randomUuid())
            .setIsMigratingZkBroker(zkEpoch != null)
            .setFeatures(features)
            .setListeners(new BrokerRegistrationRequestData.ListenerCollection(
                    List.of(
                            new BrokerRegistrationRequestData.Listener()
                                    .setName("EXTERNAL")
                                    .setHost("example.com")
                                    .setPort(8082)
                                    .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
                    ).iterator()
            ));

        var resp = sendAndReceive(
            channelManager, 
            new BrokerRegistrationRequest.Builder(req), 
            30000
        );
        return Errors.forCode(((BrokerRegistrationResponse) resp).data().errorCode());
    }

    public Errors createTopics(NodeToControllerChannelManager channelManager, String topicName) throws Exception {
        var createTopics = new CreateTopicsRequestData();
        createTopics.setTopics(new CreateTopicsRequestData.CreatableTopicCollection());
        createTopics.topics().add(
            new CreatableTopic()
                .setName(topicName)
                .setNumPartitions(10)
                .setReplicationFactor((short) 1)
        );
        createTopics.setTimeoutMs(500);

        var req = new CreateTopicsRequest.Builder(createTopics);
        var resp = sendAndReceive(channelManager, req, 3000);
        var responseData = ((CreateTopicsResponse) resp).data();
        return Errors.forCode(responseData.topics().find(topicName).errorCode());
    }

    @ClusterTest(types = {Type.KRAFT}, controllers = 1, metadataVersion = MetadataVersion.IBP_3_3_IV3)
    public void testRegisterZkWith33Controller(ClusterInstance clusterInstance) throws Exception {
        // Verify that a controller running an old metadata.version cannot register a ZK broker
        var clusterId = clusterInstance.clusterId();
        var channelManager = brokerToControllerChannelManager(clusterInstance);
        try {
            channelManager.start();
            // Invalid registration (isMigratingZkBroker, but MV does not support migrations)
            Assertions.assertEquals(
                Errors.BROKER_ID_NOT_REGISTERED,
                registerBroker(channelManager, clusterId, 100, 1L, 
                    new FeatureLevel(MetadataVersionTestUtils.IBP_3_3_IV0_FEATURE_LEVEL, MetadataVersionTestUtils.IBP_3_3_IV3_FEATURE_LEVEL))
            );

            // No features (MV) sent with registration, controller can't verify
            Assertions.assertEquals(
                Errors.INVALID_REGISTRATION,
                registerBroker(channelManager, clusterId, 100, null, null)
            );

            // Given MV is too high for controller to support
            Assertions.assertEquals(
                Errors.UNSUPPORTED_VERSION,
                registerBroker(channelManager, clusterId, 100, null, 
                    new FeatureLevel(MetadataVersionTestUtils.IBP_3_4_IV0_FEATURE_LEVEL, MetadataVersionTestUtils.IBP_3_4_IV0_FEATURE_LEVEL))
            );

            // Controller supports this MV and isMigratingZkBroker is false, so this one works
            Assertions.assertEquals(
                Errors.NONE,
                registerBroker(channelManager, clusterId, 100, null, 
                    new FeatureLevel(MetadataVersionTestUtils.IBP_3_3_IV3_FEATURE_LEVEL, MetadataVersionTestUtils.IBP_3_4_IV0_FEATURE_LEVEL))
            );
        } finally {
            channelManager.shutdown();
        }
    }

    public record FeatureLevel(short min, short max) { }

    private record TestControllerNodeProvider(SocketServer controllerSocketServer,
                                       ClusterInstance clusterInstance) implements ControllerNodeProvider {

        public Optional<Node> node() {
            return Optional.of(new Node(
                    controllerSocketServer.config().nodeId(),
                    "127.0.0.1",
                    controllerSocketServer.boundPort(clusterInstance.controllerListenerName())
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

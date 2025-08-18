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
package org.apache.kafka.clients.admin;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.test.TestUtils;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.server.config.ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;


@ClusterTestDefaults(
    brokers = 4,
    serverProperties = {
        @ClusterConfigProperty(key = AUTO_CREATE_TOPICS_ENABLE_CONFIG, value = "false"),
        @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, value = "0")
    }
)
class DescribeProducersWithBrokerIdTest {
    private static final String TOPIC_NAME = "test-topic";
    private static final int NUM_PARTITIONS = 3;
    private static final short REPLICATION_FACTOR = 3;

    private final ClusterInstance clusterInstance;
    public DescribeProducersWithBrokerIdTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    private static void sendTestRecords(Producer<Object, Object> producer) {
        for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
            producer.send(new ProducerRecord<>(TOPIC_NAME, partition, "key-" + partition, "value-" + partition));
        }
        producer.flush();
    }

    @ClusterTest
    void testDescribeProducersDefaultRoutesToLeader() throws Exception {
        clusterInstance.createTopic(TOPIC_NAME, NUM_PARTITIONS, REPLICATION_FACTOR);

        try (var producer = clusterInstance.producer(Map.of(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
             var admin = clusterInstance.admin()) {

            sendTestRecords(producer);

            var topicPartition = new TopicPartition(TOPIC_NAME, 0);
            var leaderBrokerId = clusterInstance.getLeaderBrokerId(topicPartition);

            var stateWithExplicitLeader = admin.describeProducers(List.of(topicPartition), 
                    new DescribeProducersOptions().brokerId(leaderBrokerId))
                    .partitionResult(topicPartition).get();
            
            var stateWithDefaultRouting = admin.describeProducers(List.of(topicPartition))
                    .partitionResult(topicPartition).get();
            
            assertNotNull(stateWithDefaultRouting);
            assertFalse(stateWithDefaultRouting.activeProducers().isEmpty());
            assertEquals(stateWithExplicitLeader.activeProducers(), stateWithDefaultRouting.activeProducers());
        }
    }

    @ClusterTest
    void testDescribeProducersFromFollower() throws Exception {
        clusterInstance.createTopic(TOPIC_NAME, NUM_PARTITIONS, REPLICATION_FACTOR);

        try (var producer = clusterInstance.producer(Map.of(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
             var admin = clusterInstance.admin()) {

            sendTestRecords(producer);

            var topicPartition = new TopicPartition(TOPIC_NAME, 0);
            var topicDescription = admin.describeTopics(List.of(TOPIC_NAME)).allTopicNames().get().get(TOPIC_NAME);
            var replicaBrokerIds = topicDescription.partitions().get(0).replicas().stream()
                    .map(Node::id)
                    .toList();

            var leaderBrokerId = clusterInstance.getLeaderBrokerId(topicPartition);
            var followerBrokerId = replicaBrokerIds.stream()
                    .filter(id -> !id.equals(leaderBrokerId))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("No follower found"));
            
            var followerState = admin.describeProducers(List.of(topicPartition), 
                    new DescribeProducersOptions().brokerId(followerBrokerId))
                    .partitionResult(topicPartition).get();
            var leaderState = admin.describeProducers(List.of(topicPartition))
                    .partitionResult(topicPartition).get();

            assertNotNull(followerState);
            assertFalse(followerState.activeProducers().isEmpty());
            
            var followerProducerId = followerState.activeProducers().iterator().next().producerId();
            var leaderProducerId = leaderState.activeProducers().iterator().next().producerId();
            assertEquals(leaderProducerId, followerProducerId);
        }
    }

    @ClusterTest
    void testDescribeProducersWithInvalidBrokerId() throws Exception {
        clusterInstance.createTopic(TOPIC_NAME, NUM_PARTITIONS, REPLICATION_FACTOR);

        try (var producer = clusterInstance.producer(Map.of(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
             var admin = clusterInstance.admin()) {

            sendTestRecords(producer);
            
            var topicPartition = new TopicPartition(TOPIC_NAME, 0);
            var topicDescription = admin.describeTopics(List.of(TOPIC_NAME)).allTopicNames().get().get(TOPIC_NAME);
            var replicaBrokerIds = topicDescription.partitions().get(0).replicas().stream()
                    .map(Node::id)
                    .toList();

            var nonReplicaBrokerId = clusterInstance.brokerIds().stream()
                    .filter(id -> !replicaBrokerIds.contains(id))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("No non-replica broker found"));
            
            TestUtils.assertFutureThrows(NotLeaderOrFollowerException.class, 
                    admin.describeProducers(List.of(topicPartition), 
                            new DescribeProducersOptions().brokerId(nonReplicaBrokerId))
                            .partitionResult(topicPartition)); 
        }
    }
}
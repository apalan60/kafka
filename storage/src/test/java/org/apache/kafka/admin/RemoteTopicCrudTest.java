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

package org.apache.kafka.admin;

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.TestUtils;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.ClusterTests;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.server.log.remote.storage.NoOpRemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.NoOpRemoteStorageManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.function.Executable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import scala.jdk.javaapi.OptionConverters;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ClusterTestDefaults(
    types = Type.KRAFT,
    brokers = 2,
    serverProperties = {
        @ClusterConfigProperty(key = RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, value = "true"),
        @ClusterConfigProperty(key = RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, value = "org.apache.kafka.server.log.remote.storage.NoOpRemoteStorageManager"),
        @ClusterConfigProperty(key = RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, value = "org.apache.kafka.server.log.remote.storage.NoOpRemoteLogMetadataManager"),
        @ClusterConfigProperty(key = "log.retention.ms", value = "2000"),
        @ClusterConfigProperty(key = RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, value = "1000"),
        @ClusterConfigProperty(key = "retention.bytes", value = "2048"),
        @ClusterConfigProperty(key = RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, value = "1024")
    }
)
class RemoteTopicCrudTest {

    private final ClusterInstance cluster;
    private final int numPartitions = 2;
    private final short numReplicationFactor = 2;

    private String testTopicName;

    public RemoteTopicCrudTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @BeforeEach
    void setUp(TestInfo info) {
        var methodName = info.getTestMethod().orElseThrow().getName();
        testTopicName = methodName + "-" + UUID.randomUUID().toString().replace("-", "").substring(0, 3);
    }

    @ClusterTest
    void testCreateRemoteTopicWithValidRetentionTime() {
        try (var admin = cluster.admin()) {
            var topicConfig = Map.of(
                TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true",
                TopicConfig.RETENTION_MS_CONFIG, "60000",
                TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, "30000"
            );
            admin.createTopics(List.of(new NewTopic(testTopicName, numPartitions, numReplicationFactor).configs(topicConfig)));
        }
    }

    @ClusterTest
    void testCreateRemoteTopicWithValidRetentionSize() throws Exception {
        var topicConfig = Map.of(
            TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true",
            TopicConfig.RETENTION_BYTES_CONFIG, "512",
            TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, "256"
        );
        try (var admin = cluster.admin()) {
            admin.createTopics(List.of(new NewTopic(testTopicName, numPartitions, numReplicationFactor).configs(topicConfig)));
        }
        verifyRemoteLogTopicConfigs(topicConfig);
    }


    @ClusterTest
    void testCreateRemoteTopicWithInheritedLocalRetentionTime() throws Exception {
        // inherited local retention ms is 1000
        var topicConfig = Map.of(
            TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true",
            TopicConfig.RETENTION_MS_CONFIG, "1001"
        );
        try (var admin = cluster.admin()) {
            admin.createTopics(List.of(new NewTopic(testTopicName, numPartitions, numReplicationFactor).configs(topicConfig)));
        }
        verifyRemoteLogTopicConfigs(topicConfig);
    }

    @ClusterTest
    void testCreateRemoteTopicWithInheritedLocalRetentionSize() throws Exception {
        // inherited local retention bytes is 1024
        var topicConfig = Map.of(
            TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true",
            TopicConfig.RETENTION_BYTES_CONFIG, "1025"
        );
        try (var admin = cluster.admin()) {
            admin.createTopics(List.of(new NewTopic(testTopicName, numPartitions, numReplicationFactor).configs(topicConfig)));
        }
        verifyRemoteLogTopicConfigs(topicConfig);
    }

    @ClusterTest
    void testCreateRemoteTopicWithInvalidRetentionTime() {
        // inherited local retention ms is 1000
        var topicConfig = Map.of(
            TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true",
            TopicConfig.RETENTION_MS_CONFIG, "200"
        );

        try (var admin = cluster.admin()) {
            assertThrowsExceptionIgnoringResult(() -> admin.createTopics(List.of(new NewTopic(testTopicName, numPartitions, numReplicationFactor).configs(topicConfig))).all().get());
        }
    }

    @ClusterTest
    void testCreateRemoteTopicWithInvalidRetentionSize() {
        // inherited local retention bytes is 1024
        var topicConfig = Map.of(
            TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true",
            TopicConfig.RETENTION_BYTES_CONFIG, "512"
        );

        try (var admin = cluster.admin()) {
            assertThrowsExceptionIgnoringResult(() -> admin.createTopics(List.of(new NewTopic(testTopicName, numPartitions, numReplicationFactor).configs(topicConfig))).all().get());
        }
    }
   
    @ClusterTest
    void testCreateCompactedRemoteStorage() {
        var topicConfig = Map.of(
            TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true",
            TopicConfig.CLEANUP_POLICY_CONFIG, "compact"
        );

        try (var admin = cluster.admin()) {
            assertThrowsExceptionIgnoringResult(() -> admin.createTopics(
                List.of(new NewTopic(testTopicName, numPartitions, numReplicationFactor).configs(topicConfig))).all().get());
        }
    }

    // `remote.log.delete.on.disable` and `remote.log.copy.disable` only works in KRaft mode.
    @ClusterTests({
        @ClusterTest(serverProperties = {
            @ClusterConfigProperty(key = "remote.log.copy.disable", value = "true"),
            @ClusterConfigProperty(key = "remote.log.delete.on.disable", value = "true")
        }),
        @ClusterTest(serverProperties = {
            @ClusterConfigProperty(key = "remote.log.copy.disable", value = "true"),
            @ClusterConfigProperty(key = "remote.log.delete.on.disable", value = "false")
        }),
        @ClusterTest(serverProperties = {
            @ClusterConfigProperty(key = "remote.log.copy.disable", value = "false"),
            @ClusterConfigProperty(key = "remote.log.delete.on.disable", value = "true")
        }),
        @ClusterTest(serverProperties = {
            @ClusterConfigProperty(key = "remote.log.copy.disable", value = "false"),
            @ClusterConfigProperty(key = "remote.log.delete.on.disable", value = "false")
        })
    })
    void testCreateRemoteTopicWithCopyDisabledAndDeleteOnDisable() throws Exception {
        var topicConfig = Map.of(
            TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG, cluster.config().serverProperties().get("remote.log.copy.disable"),
            TopicConfig.REMOTE_LOG_DELETE_ON_DISABLE_CONFIG, cluster.config().serverProperties().get("remote.log.delete.on.disable")
        );

        try (var admin = cluster.admin()) {
            var result = admin.createTopics(List.of(new NewTopic(testTopicName, numPartitions, numReplicationFactor).configs(topicConfig)));
            assertDoesNotThrow(() -> result.all().get(30, TimeUnit.SECONDS));
        }

        verifyRemoteLogTopicConfigs(topicConfig);
    }

    // `remote.log.delete.on.disable` only works in KRaft mode.
    @ClusterTest
    void testCreateTopicRetentionMsValidationWithRemoteCopyDisabled() throws Exception {
        var testTopicName2 = testTopicName + "2";
        var testTopicName3 = testTopicName + "3";
        var errorMsgMs = "When `remote.log.copy.disable` is set to true, the `local.retention.ms` and `retention.ms` " +
            "must be set to the identical value because there will be no more logs copied to the remote storage.";

        // 1. create a topic with `remote.log.copy.disable=true` and have different local.retention.ms and retention.ms value,
        //    it should fail to create the topic
        var topicConfig = new HashMap<>(Map.of(
            TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true",
            TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG, "true",
            TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, "100",
            TopicConfig.RETENTION_MS_CONFIG, "1000",
            TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, "-2"
        ));

        try (var admin = cluster.admin()) {
            // Test that creating topic with invalid config fails with appropriate error message
            var err = assertThrowsException(() -> admin.createTopics(List.of(new NewTopic(testTopicName, numPartitions, numReplicationFactor).configs(topicConfig))).all().get());
            assertEquals(errorMsgMs, err.getMessage());

            // 2. change the local.retention.ms value to the same value as retention.ms should successfully create the topic
            topicConfig.put(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, "1000");
            admin.createTopics(List.of(new NewTopic(testTopicName, numPartitions, numReplicationFactor).configs(topicConfig))).all().get();

            // 3. change the local.retention.ms value to "-2" should also successfully create the topic
            topicConfig.put(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, "-2");
            admin.createTopics(List.of(new NewTopic(testTopicName2, numPartitions, numReplicationFactor).configs(topicConfig))).values().get(testTopicName2).get();

            // 4. create a topic with `remote.log.copy.disable=false` and have different local.retention.ms and retention.ms value,
            //    it should successfully creates the topic.
            topicConfig.clear();
            topicConfig.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true");
            topicConfig.put(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, "100");
            topicConfig.put(TopicConfig.RETENTION_MS_CONFIG, "1000");
            topicConfig.put(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, "-2");
            admin.createTopics(List.of(new NewTopic(testTopicName3, numPartitions, numReplicationFactor).configs(topicConfig))).values().get(testTopicName3).get();

            // 5. alter the config to `remote.log.copy.disable=true`, it should fail the config change
            var configs = new java.util.HashMap<ConfigResource, java.util.Collection<AlterConfigOp>>();
            configs.put(new ConfigResource(ConfigResource.Type.TOPIC, testTopicName3),
                List.of(
                    new AlterConfigOp(new ConfigEntry(TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG, "true"),
                        AlterConfigOp.OpType.SET)
                ));

            var err2 = assertThrowsException(() -> admin.incrementalAlterConfigs(configs).all().get());
            assertEquals(errorMsgMs, err2.getMessage());

            // 6. alter the config to `remote.log.copy.disable=true` and local.retention.ms == retention.ms, it should work without error
            configs.put(new ConfigResource(ConfigResource.Type.TOPIC, testTopicName3),
                List.of(
                    new AlterConfigOp(new ConfigEntry(TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG, "true"),
                        AlterConfigOp.OpType.SET),
                    new AlterConfigOp(new ConfigEntry(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, "1000"),
                        AlterConfigOp.OpType.SET)
                ));

            admin.incrementalAlterConfigs(configs).all().get();
        }
    }


    @ClusterTest
    void testCreateTopicRetentionBytesValidationWithRemoteCopyDisabled() throws Exception {
        var testTopicName2 = testTopicName + "2";
        var testTopicName3 = testTopicName + "3";
        var errorMsgBytes = "When `remote.log.copy.disable` is set to true, the `local.retention.bytes` and `retention.bytes` " +
            "must be set to the identical value because there will be no more logs copied to the remote storage.";

        // 1. create a topic with `remote.log.copy.disable=true` and have different local.retention.bytes and retention.bytes value,
        //    it should fail to create the topic
        var topicConfig = new HashMap<>(Map.of(
            TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true",
            TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG, "true",
            TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, "100",
            TopicConfig.RETENTION_BYTES_CONFIG, "1000",
            TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, "-2"
        ));

        try (var admin = cluster.admin()) {
            var err = assertThrowsException(() -> admin.createTopics(List.of(new NewTopic(testTopicName, numPartitions, numReplicationFactor).configs(topicConfig))).all().get());
            assertEquals(errorMsgBytes, err.getMessage());

            // 2. change the local.retention.bytes value to the same value as retention.bytes should successfully create the topic
            topicConfig.put(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, "1000");
            admin.createTopics(List.of(new NewTopic(testTopicName, numPartitions, numReplicationFactor).configs(topicConfig))).all().get();

            // 3. change the local.retention.bytes value to "-2" should also successfully create the topic
            topicConfig.put(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, "-2");
            admin.createTopics(List.of(new NewTopic(testTopicName2, numPartitions, numReplicationFactor).configs(topicConfig))).values().get(testTopicName2).get();

            // 4. create a topic with `remote.log.copy.disable=false` and have different local.retention.bytes and retention.bytes value,
            //    it should successfully creates the topic.
            topicConfig.clear();
            topicConfig.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true");
            topicConfig.put(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, "100");
            topicConfig.put(TopicConfig.RETENTION_BYTES_CONFIG, "1000");
            topicConfig.put(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, "-2");
            admin.createTopics(List.of(new NewTopic(testTopicName3, numPartitions, numReplicationFactor).configs(topicConfig))).values().get(testTopicName3).get();

            // 5. alter the config to `remote.log.copy.disable=true`, it should fail the config change
            var configs = new java.util.HashMap<ConfigResource, java.util.Collection<AlterConfigOp>>();
            configs.put(new ConfigResource(ConfigResource.Type.TOPIC, testTopicName3),
                List.of(
                    new AlterConfigOp(new ConfigEntry(TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG, "true"),
                        AlterConfigOp.OpType.SET)
                ));
            var err2 = assertThrowsException(() -> admin.incrementalAlterConfigs(configs).all().get());
            assertEquals(errorMsgBytes, err2.getMessage());

            // 6. alter the config to `remote.log.copy.disable=true` and local.retention.bytes == retention.bytes, it should work without error
            configs.put(new ConfigResource(ConfigResource.Type.TOPIC, testTopicName3),
                java.util.Arrays.asList(
                    new AlterConfigOp(new ConfigEntry(TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG, "true"),
                        AlterConfigOp.OpType.SET),
                    new AlterConfigOp(new ConfigEntry(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, "1000"),
                        AlterConfigOp.OpType.SET)
                ));
            admin.incrementalAlterConfigs(configs).all().get();
        }
    }

    @ClusterTest
    void testEnableRemoteLogOnExistingTopic() throws Exception {
        try (var admin = cluster.admin()) {
            var topicConfig = new HashMap<String, String>();
            admin.createTopics(List.of(new NewTopic(testTopicName, numPartitions, numReplicationFactor).configs(new HashMap<>()))).all().get();

            var configs = new java.util.HashMap<ConfigResource, java.util.Collection<AlterConfigOp>>();
            configs.put(new ConfigResource(ConfigResource.Type.TOPIC, testTopicName),
                Collections.singleton(new AlterConfigOp(new ConfigEntry(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"), AlterConfigOp.OpType.SET))
            );
            admin.incrementalAlterConfigs(configs).all().get();
            verifyRemoteLogTopicConfigs(topicConfig);
        }
    }

    @ClusterTest(serverProperties = {
        @ClusterConfigProperty(key = RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, value = "false")
    })
    void testEnableRemoteLogWhenSystemRemoteStorageIsDisabled() throws ExecutionException, InterruptedException {
        try (var admin = cluster.admin()) {
            var topicConfig = Map.of(
                TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"
            );
            var message = assertThrowsException(
                () -> admin.createTopics(
                    List.of(new NewTopic(testTopicName, numPartitions, numReplicationFactor).configs(topicConfig))).all().get());
            assertTrue(message.getMessage().contains("Tiered Storage functionality is disabled in the broker"));

            admin.createTopics(List.of(new NewTopic(testTopicName, numPartitions, numReplicationFactor))).all().get();

            var configs = new java.util.HashMap<ConfigResource, java.util.Collection<AlterConfigOp>>();
            configs.put(new ConfigResource(ConfigResource.Type.TOPIC, testTopicName),
                Collections.singleton(
                    new AlterConfigOp(new ConfigEntry(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"),
                        AlterConfigOp.OpType.SET))
            );
            var errorMessage = assertThrowsException(
                () -> admin.incrementalAlterConfigs(configs).all().get());
            assertTrue(errorMessage.getMessage().contains("Tiered Storage functionality is disabled in the broker"));
        }
    }

    @ClusterTest
    void testUpdateTopicConfigWithValidRetentionTime() throws Exception {
        try (var admin = cluster.admin()) {
            var topicConfig = Map.of(
                TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"
            );
            admin.createTopics(List.of(new NewTopic(testTopicName, numPartitions, numReplicationFactor)
                .configs(topicConfig))).all().get();

            var configs = new java.util.HashMap<ConfigResource, java.util.Collection<AlterConfigOp>>();
            configs.put(new ConfigResource(ConfigResource.Type.TOPIC, testTopicName),
                List.of(
                    new AlterConfigOp(new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "200"),
                        AlterConfigOp.OpType.SET),
                    new AlterConfigOp(new ConfigEntry(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, "100"),
                        AlterConfigOp.OpType.SET)
                ));
            admin.incrementalAlterConfigs(configs).all().get();
            verifyRemoteLogTopicConfigs(topicConfig);
        }
    }

    @ClusterTest
    void testUpdateTopicConfigWithValidRetentionSize() throws Exception {
        try (var admin = cluster.admin()) {
            var topicConfig = Map.of(
                TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true" 
            );

            admin.createTopics(List.of(new NewTopic(testTopicName, numPartitions, numReplicationFactor)
                .configs(topicConfig))).all().get();
                
            var configs = new java.util.HashMap<ConfigResource, java.util.Collection<AlterConfigOp>>();
            configs.put(new ConfigResource(ConfigResource.Type.TOPIC, testTopicName),
                java.util.Arrays.asList(
                    new AlterConfigOp(new ConfigEntry(TopicConfig.RETENTION_BYTES_CONFIG, "200"),
                        AlterConfigOp.OpType.SET),
                    new AlterConfigOp(new ConfigEntry(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, "100"),
                        AlterConfigOp.OpType.SET)
                ));
            admin.incrementalAlterConfigs(configs).all().get();
            verifyRemoteLogTopicConfigs(topicConfig);
        }
    }

    @ClusterTest
    void testUpdateTopicConfigWithInheritedLocalRetentionTime() throws Exception {
        try (var admin = cluster.admin()) {
            var topicConfig = Map.of(
                TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"
            );
            
            admin.createTopics(List.of(new NewTopic(testTopicName, numPartitions, numReplicationFactor)
                .configs(topicConfig))).all().get(); 
            
            // inherited local retention ms is 1000
            var configs = new java.util.HashMap<ConfigResource, java.util.Collection<AlterConfigOp>>();
            configs.put(new ConfigResource(ConfigResource.Type.TOPIC, testTopicName),
                List.of(
                    new AlterConfigOp(new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "200"),
                        AlterConfigOp.OpType.SET)
                ));

            assertThrowsExceptionIgnoringResult(
                () -> admin.incrementalAlterConfigs(configs).all().get());
        }
    }

    @ClusterTest
    void testUpdateTopicConfigWithInheritedLocalRetentionSize() throws Exception {
        try (var admin = cluster.admin()) {
            var topicConfig = Map.of(
                TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"
            );
            
            admin.createTopics(List.of(new NewTopic(testTopicName, numPartitions, numReplicationFactor)
                .configs(topicConfig))).all().get();

            // inherited local retention bytes is 1024
            var configs = new java.util.HashMap<ConfigResource, java.util.Collection<AlterConfigOp>>();
            configs.put(new ConfigResource(ConfigResource.Type.TOPIC, testTopicName),
                List.of(
                    new AlterConfigOp(new ConfigEntry(TopicConfig.RETENTION_BYTES_CONFIG, "512"),
                        AlterConfigOp.OpType.SET)
                ));

            assertThrowsException(InvalidConfigurationException.class,
                () -> admin.incrementalAlterConfigs(configs).all().get(),
                "Invalid local retention size");
        }
    }

    // The remote storage config validation on controller level only works in KRaft
    @ClusterTest
    void testUpdateTopicConfigWithDisablingRemoteStorage() throws Exception {
        try (var admin = cluster.admin()) {
            var topicConfig = Map.of(
                TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"
            );

            admin.createTopics(List.of(new NewTopic(testTopicName, numPartitions, numReplicationFactor)
                .configs(topicConfig))).all().get();
            
            var configs = new java.util.HashMap<ConfigResource, java.util.Collection<AlterConfigOp>>();
            configs.put(new ConfigResource(ConfigResource.Type.TOPIC, testTopicName),
                List.of(
                    new AlterConfigOp(new ConfigEntry(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false"),
                        AlterConfigOp.OpType.SET)
                ));

            assertThrowsException(InvalidConfigurationException.class,
                () -> admin.incrementalAlterConfigs(configs).all().get(),
                "It is invalid to disable remote storage without deleting remote data. " +
                    "If you want to keep the remote data and turn to read only, please set `remote.storage.enable=true,remote.log.copy.disable=true`. " +
                    "If you want to disable remote storage and delete all remote data, please set `remote.storage.enable=false,remote.log.delete.on.disable=true`.");
        }
    }

    @ClusterTest
    void testUpdateTopicConfigWithDisablingRemoteStorageWithDeleteOnDisable() throws Exception {
        try (var admin = cluster.admin()) {
            var topicConfig = Map.of(
                TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"
            );

            admin.createTopics(List.of(new NewTopic(testTopicName, numPartitions, numReplicationFactor)
                .configs(topicConfig))).all().get();

            var configs = new java.util.HashMap<ConfigResource, java.util.Collection<AlterConfigOp>>();
            configs.put(new ConfigResource(ConfigResource.Type.TOPIC, testTopicName),
                java.util.Arrays.asList(
                    new AlterConfigOp(new ConfigEntry(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false"),
                        AlterConfigOp.OpType.SET),
                    new AlterConfigOp(new ConfigEntry(TopicConfig.REMOTE_LOG_DELETE_ON_DISABLE_CONFIG, "true"),
                        AlterConfigOp.OpType.SET)
                ));
            admin.incrementalAlterConfigs(configs).all().get();

            var newProps = new HashMap<String, String>();
            for (AlterConfigOp op : configs.get(new ConfigResource(ConfigResource.Type.TOPIC, testTopicName))) {
                newProps.put(op.configEntry().name(), op.configEntry().value());
            }

            verifyRemoteLogTopicConfigs(newProps);
        }
    }

    @ClusterTest(
        serverProperties = {
            @ClusterConfigProperty(key = RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, value = "org.apache.kafka.admin.RemoteTopicCrudTest$MyRemoteStorageManager"),
            @ClusterConfigProperty(key = RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, value = "org.apache.kafka.admin.RemoteTopicCrudTest$MyRemoteLogMetadataManager")
        }
    )
    void testTopicDeletion() throws Exception {
        try (var admin = cluster.admin()) {
            MyRemoteStorageManager.DELETE_SEGMENT_EVENT_COUNTER.set(0);
            var topicConfig = Map.of(
                TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true",
                TopicConfig.RETENTION_MS_CONFIG, "200",
                TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, "100"
            );
            admin.createTopics(List.of(new NewTopic(testTopicName, numPartitions, numReplicationFactor)
                .configs(topicConfig))).all().get();

            admin.deleteTopics(List.of(testTopicName)).all().get();

            assertThrowsException(UnknownTopicOrPartitionException.class,
                () -> admin.describeTopics(List.of(testTopicName)).allTopicNames().get(),
                "Topic should be deleted");

            TestUtils.waitForCondition(() ->
                    numPartitions * MyRemoteLogMetadataManager.SEGMENT_COUNT_PER_PARTITION == MyRemoteStorageManager.DELETE_SEGMENT_EVENT_COUNTER.get(), 
                "Remote log segments should be deleted only once by the leader");
        }
    }

    private <T extends Throwable> T assertThrowsException(Class<T> expectedType,
                                                          Executable executable,
                                                          String message) {
        return assertThrows(expectedType, () -> {
            try {
                executable.execute();
            } catch (ExecutionException e) {
                throw e.getCause();
            }
        }, message);
    }

    private Throwable assertThrowsException(Executable executable) {
        return assertThrowsException((Class<? extends Throwable>) InvalidConfigurationException.class, executable, null);
    }

    private void assertThrowsExceptionIgnoringResult(Executable executable) {
        assertThrowsException((Class<? extends Throwable>) InvalidConfigurationException.class, executable, null);
    }

    private void verifyRemoteLogTopicConfigs(Map<String, String> topicConfig) throws Exception {
        TestUtils.waitForCondition(() -> {
            var logBuffer = cluster.brokers().values()
                .stream()
                .map(broker -> broker.logManager().getLog(new TopicPartition(testTopicName, 0), false))
                .map(OptionConverters::toJava)
                .flatMap(Optional::stream)
                .toList();
    
            var result = !logBuffer.isEmpty();
    
            if (result) {
                if (topicConfig.containsKey(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG)) {
                    result = Boolean.parseBoolean(
                        topicConfig.get(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG)) == logBuffer.get(0).config().remoteStorageEnable();
                }
    
                if (topicConfig.containsKey(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG)) {
                    result = result
                        && Long.parseLong(
                        topicConfig.get(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG))
                        == logBuffer.get(0).config().localRetentionBytes();
                }
    
                if (topicConfig.containsKey(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG)) {
                    result = result
                        && Long.parseLong(
                        topicConfig.get(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG))
                        == logBuffer.get(0).config().localRetentionMs();
                }
    
                if (topicConfig.containsKey(TopicConfig.RETENTION_MS_CONFIG)) {
                    result = result
                        && Long.parseLong(
                        topicConfig.get(TopicConfig.RETENTION_MS_CONFIG))
                        == logBuffer.get(0).config().retentionMs;
                }
    
                if (topicConfig.containsKey(TopicConfig.RETENTION_BYTES_CONFIG)) {
                    result = result
                        && Long.parseLong(
                        topicConfig.get(TopicConfig.RETENTION_BYTES_CONFIG))
                        == logBuffer.get(0).config().retentionSize;
                }
    
                if (topicConfig.containsKey(TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG)) {
                    result = result
                        && Boolean.parseBoolean(
                        topicConfig.get(TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG))
                        == logBuffer.get(0).config().remoteLogCopyDisable();
                }
    
                if (topicConfig.containsKey(TopicConfig.REMOTE_LOG_DELETE_ON_DISABLE_CONFIG)) {
                    result = result
                        && Boolean.parseBoolean(
                        topicConfig.get(TopicConfig.REMOTE_LOG_DELETE_ON_DISABLE_CONFIG))
                        == logBuffer.get(0).config().remoteLogDeleteOnDisable();
                }
            }
            return result;
        }, "Failed to update topic config $topicConfig" + topicConfig);
    }


    public static class MyRemoteStorageManager extends NoOpRemoteStorageManager {
        public static final AtomicInteger DELETE_SEGMENT_EVENT_COUNTER = new AtomicInteger(0);

        @Override
        public void deleteLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
            DELETE_SEGMENT_EVENT_COUNTER.incrementAndGet();
        }
    }

    public static class MyRemoteLogMetadataManager extends NoOpRemoteLogMetadataManager {
        public static final int SEGMENT_COUNT_PER_PARTITION = 10;
        public static final int RECORDS_PER_SEGMENT = 100;
        public static final int SEGMENT_SIZE = 1024;

        private final MockTime time = new MockTime();

        @Override
        public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicIdPartition topicIdPartition) {
            List<RemoteLogSegmentMetadata> segmentMetadataList = new ArrayList<>();
            for (var idx = 0; idx < SEGMENT_COUNT_PER_PARTITION; idx++) {
                var timestamp = time.milliseconds();
                long startOffset = idx * RECORDS_PER_SEGMENT;
                var endOffset = startOffset + RECORDS_PER_SEGMENT - 1;
                var segmentLeaderEpochs = Collections.singletonMap(0, 0L);
                var segmentId = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
                var metadata = new RemoteLogSegmentMetadata(
                    segmentId,
                    startOffset,
                    endOffset,
                    timestamp,
                    0,
                    timestamp,
                    SEGMENT_SIZE,
                    Optional.empty(),
                    RemoteLogSegmentState.COPY_SEGMENT_FINISHED,
                    segmentLeaderEpochs
                );
                segmentMetadataList.add(metadata);
            }
            return segmentMetadataList.iterator();
        }
    }
}

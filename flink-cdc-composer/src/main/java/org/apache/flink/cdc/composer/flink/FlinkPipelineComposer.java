/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.composer.flink;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.composer.PipelineComposer;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.flink.coordination.OperatorIDGenerator;
import org.apache.flink.cdc.composer.flink.translator.DataSinkTranslator;
import org.apache.flink.cdc.composer.flink.translator.DataSourceTranslator;
import org.apache.flink.cdc.composer.flink.translator.PartitioningTranslator;
import org.apache.flink.cdc.composer.flink.translator.SchemaOperatorTranslator;
import org.apache.flink.cdc.composer.flink.translator.TransformTranslator;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.cdc.runtime.serializer.event.EventSerializer;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Composer for translating data pipeline to a Flink DataStream job. */
@Internal
public class FlinkPipelineComposer implements PipelineComposer {

    private final StreamExecutionEnvironment env;
    private final boolean isBlocking;

    public static FlinkPipelineComposer ofRemoteCluster(
            org.apache.flink.configuration.Configuration flinkConfig, List<Path> additionalJars) {
        org.apache.flink.configuration.Configuration effectiveConfiguration =
                new org.apache.flink.configuration.Configuration();
        // Use "remote" as the default target
        effectiveConfiguration.set(DeploymentOptions.TARGET, "remote");
        effectiveConfiguration.addAll(flinkConfig);
        StreamExecutionEnvironment env = new StreamExecutionEnvironment(effectiveConfiguration);
        additionalJars.forEach(
                jarPath -> {
                    try {
                        FlinkEnvironmentUtils.addJar(env, jarPath.toUri().toURL());
                    } catch (Exception e) {
                        throw new RuntimeException(
                                String.format(
                                        "Unable to convert JAR path \"%s\" to URL when adding JAR to Flink environment",
                                        jarPath),
                                e);
                    }
                });
        return new FlinkPipelineComposer(env, false);
    }

    public static FlinkPipelineComposer ofMiniCluster() {
        return new FlinkPipelineComposer(
                StreamExecutionEnvironment.getExecutionEnvironment(), true);
    }

    private FlinkPipelineComposer(StreamExecutionEnvironment env, boolean isBlocking) {
        this.env = env;
        this.isBlocking = isBlocking;
    }

    @Override
    /**
     * 组合管道定义为一个执行流程
     *
     * @param pipelineDef 管道定义对象，包含管道的所有配置和组件
     * @return 返回组装后的管道执行对象
     */
    public PipelineExecution compose(PipelineDef pipelineDef) {
        // 设置并行度
        int parallelism = pipelineDef.getConfig().get(PipelineOptions.PIPELINE_PARALLELISM);
        env.getConfig().setParallelism(parallelism);

        // 构建源操作符
        DataSourceTranslator sourceTranslator = new DataSourceTranslator();
        DataStream<Event> stream =
                sourceTranslator.translate(pipelineDef.getSource(), env, pipelineDef.getConfig());

        // 构建TransformSchemaOperator用于处理Schema事件
        TransformTranslator transformTranslator = new TransformTranslator();
        stream = transformTranslator.translateSchema(stream, pipelineDef.getTransforms());

        // 构建Schema操作符
        SchemaOperatorTranslator schemaOperatorTranslator =
                new SchemaOperatorTranslator(
                        pipelineDef
                                .getConfig()
                                .get(PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR),
                        pipelineDef.getConfig().get(PipelineOptions.PIPELINE_SCHEMA_OPERATOR_UID),
                        pipelineDef
                                .getConfig()
                                .get(PipelineOptions.PIPELINE_SCHEMA_OPERATOR_RPC_TIMEOUT));
        OperatorIDGenerator schemaOperatorIDGenerator =
                new OperatorIDGenerator(schemaOperatorTranslator.getSchemaOperatorUid());

        // 构建TransformDataOperator用于处理数据事件
        stream =
                transformTranslator.translateData(
                        stream,
                        pipelineDef.getTransforms(),
                        schemaOperatorIDGenerator.generate(),
                        pipelineDef.getConfig().get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE));

        // 提前构建DataSink，因为Schema操作符需要MetadataApplier
        DataSink dataSink = createDataSink(pipelineDef.getSink(), pipelineDef.getConfig());

        // 应用Schema操作
        stream =
                schemaOperatorTranslator.translate(
                        stream, parallelism, dataSink.getMetadataApplier(), pipelineDef.getRoute());

        // 构建Partitioner用于事件的洗牌
        PartitioningTranslator partitioningTranslator = new PartitioningTranslator();
        stream =
                partitioningTranslator.translate(
                        stream,
                        parallelism,
                        parallelism,
                        schemaOperatorIDGenerator.generate(),
                        dataSink.getDataChangeEventHashFunctionProvider());

        // 构建Sink操作符
        DataSinkTranslator sinkTranslator = new DataSinkTranslator();
        sinkTranslator.translate(
                pipelineDef.getSink(), stream, dataSink, schemaOperatorIDGenerator.generate());

        // 添加框架的JAR文件
        addFrameworkJars();

        // 返回组装后的管道执行对象
        return new FlinkPipelineExecution(
                env, pipelineDef.getConfig().get(PipelineOptions.PIPELINE_NAME), isBlocking);
    }

    private DataSink createDataSink(SinkDef sinkDef, Configuration pipelineConfig) {
        // Search the data sink factory
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier(
                        sinkDef.getType(), DataSinkFactory.class);

        // Include sink connector JAR
        FactoryDiscoveryUtils.getJarPathByIdentifier(sinkDef.getType(), DataSinkFactory.class)
                .ifPresent(jar -> FlinkEnvironmentUtils.addJar(env, jar));

        // Create data sink
        return sinkFactory.createDataSink(
                new FactoryHelper.DefaultContext(
                        sinkDef.getConfig(),
                        pipelineConfig,
                        Thread.currentThread().getContextClassLoader()));
    }

    private void addFrameworkJars() {
        try {
            Set<URI> frameworkJars = new HashSet<>();
            // Common JAR
            // We use the core interface (Event) to search the JAR
            Optional<URL> commonJar = getContainingJar(Event.class);
            if (commonJar.isPresent()) {
                frameworkJars.add(commonJar.get().toURI());
            }
            // Runtime JAR
            // We use the serializer of the core interface (EventSerializer) to search the JAR
            Optional<URL> runtimeJar = getContainingJar(EventSerializer.class);
            if (runtimeJar.isPresent()) {
                frameworkJars.add(runtimeJar.get().toURI());
            }
            for (URI jar : frameworkJars) {
                FlinkEnvironmentUtils.addJar(env, jar.toURL());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to search and add Flink CDC framework JARs", e);
        }
    }

    private Optional<URL> getContainingJar(Class<?> clazz) throws Exception {
        URL container = clazz.getProtectionDomain().getCodeSource().getLocation();
        if (Files.isDirectory(Paths.get(container.toURI()))) {
            return Optional.empty();
        }
        return Optional.of(container);
    }
}

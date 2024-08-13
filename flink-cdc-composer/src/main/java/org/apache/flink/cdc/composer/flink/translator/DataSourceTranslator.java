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

package org.apache.flink.cdc.composer.flink.translator;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.FlinkSourceFunctionProvider;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.flink.FlinkEnvironmentUtils;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Translator used to build {@link DataSource} which will generate a {@link DataStream}. */
@Internal
public class DataSourceTranslator {

    /**
     * 将定义的数据源转换为 Flink 的 Event 类型 DataStreamSource。
     *
     * @param sourceDef 数据源定义，包含数据源类型和配置等信息。
     * @param env 流执行环境，用于创建和管理 Flink 数据流。
     * @param pipelineConfig 管道配置，包含全局管道设置如并行度等。
     * @return 返回 Event 类型的 DataStreamSource，表示流数据源。
     */
    public DataStreamSource<Event> translate(
            SourceDef sourceDef, StreamExecutionEnvironment env, Configuration pipelineConfig) {
        // 查找数据源工厂
        DataSourceFactory sourceFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier(
                        sourceDef.getType(), DataSourceFactory.class);

        // 创建数据源
        DataSource dataSource =
                sourceFactory.createDataSource(
                        new FactoryHelper.DefaultContext(
                                sourceDef.getConfig(),
                                pipelineConfig,
                                Thread.currentThread().getContextClassLoader()));

        // 向环境添加数据源 JAR
        FactoryDiscoveryUtils.getJarPathByIdentifier(sourceDef.getType(), DataSourceFactory.class)
                .ifPresent(jar -> FlinkEnvironmentUtils.addJar(env, jar));

        // 获取数据源提供者
        final int sourceParallelism = pipelineConfig.get(PipelineOptions.PIPELINE_PARALLELISM);
        EventSourceProvider eventSourceProvider = dataSource.getEventSourceProvider();
        //source 可能是指 flink 里已存在的数据源，直接就能用
        if (eventSourceProvider instanceof FlinkSourceProvider) {
            // FlinkSourceProvider 类型的数据源
            FlinkSourceProvider sourceProvider = (FlinkSourceProvider) eventSourceProvider;
            return env.fromSource(
                            sourceProvider.getSource(),
                            WatermarkStrategy.noWatermarks(),
                            sourceDef.getName().orElse(generateDefaultSourceName(sourceDef)),
                            new EventTypeInfo())
                    .setParallelism(sourceParallelism);
        //sourceFunction，可能是指通过实现这个接口，自己定义了产生流的逻辑
        } else if (eventSourceProvider instanceof FlinkSourceFunctionProvider) {
            // FlinkSourceFunctionProvider 类型的数据源
            FlinkSourceFunctionProvider sourceFunctionProvider =
                    (FlinkSourceFunctionProvider) eventSourceProvider;
            DataStreamSource<Event> stream =
                    env.addSource(sourceFunctionProvider.getSourceFunction(), new EventTypeInfo())
                            .setParallelism(sourceParallelism);
            if (sourceDef.getName().isPresent()) {
                stream.name(sourceDef.getName().get());
            }
            return stream;
        } else {
            // 不支持的提供者类型
            throw new IllegalStateException(
                    String.format(
                            "不支持的 EventSourceProvider 类型 \"%s\"",
                            eventSourceProvider.getClass().getCanonicalName()));
        }
    }

    private String generateDefaultSourceName(SourceDef sourceDef) {
        return String.format("Flink CDC Event Source: %s", sourceDef.getType());
    }
}

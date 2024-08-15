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

package org.apache.flink.cdc.connectors.mysql.source;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.connectors.mysql.CustomProxy;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.source.reader.MySqlPipelineRecordEmitter;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;

/** A {@link DataSource} for mysql cdc connector. */
@Internal
public class MySqlDataSource implements DataSource {

    private final MySqlSourceConfigFactory configFactory;
    private final MySqlSourceConfig sourceConfig;

    public MySqlDataSource(MySqlSourceConfigFactory configFactory) {
        this.configFactory = configFactory;
        this.sourceConfig = configFactory.createConfig(0);
    }

    @Override
    /**
     * 获取事件源提供者
     * 该方法负责创建并返回一个MySql事件源提供者，该提供者能够根据配置从MySql数据库中捕获事件
     * 主要用于将数据库的变更事件流式读取并转换为Flink能够处理的事件流
     * //1.收到请求时，创建一个定时器，60s后返回
     * //2.等待定时器返回或者等待收到事件
     * @return EventSourceProvider 事件源提供者实例，专门用于从MySql数据库捕获事件
     */
    public EventSourceProvider getEventSourceProvider() {
        // 创建一个MySql事件反序列化器，用于将数据库的日志事件转换为应用程序可以处理的事件格式
        // 参数包括变更日志模式和是否包含模式更改
        MySqlEventDeserializer deserializer =
                new MySqlEventDeserializer(
                        DebeziumChangelogMode.ALL, sourceConfig.isIncludeSchemaChanges());

        // 创建一个MySql源实例，用于从MySql数据库中读取事件
        // 该实例使用上面创建的反序列化器，并定制化创建PipelineRecordEmitter，以适配特定的反序列化器和配置
        MySqlSource<Event> source =
                new MySqlSource<>(
                        configFactory,
                        deserializer,
                        (sourceReaderMetrics, sourceConfig) ->
                                new MySqlPipelineRecordEmitter(
                                        deserializer, sourceReaderMetrics, sourceConfig));

        MySqlSource<Event> proxy = new CustomProxy<>(source).getProxy(t -> {
            t.setConfigFactory(configFactory);
            t.setDeserializationSchema(deserializer);
            t.setRecordEmitterSupplier((sourceReaderMetrics, sourceConfig) ->
                    new MySqlPipelineRecordEmitter(
                            deserializer, sourceReaderMetrics, sourceConfig));
            return t;
        });
        // 返回一个事件源提供者，它包装了上述创建的MySql源实例
        // 该提供者负责将MySql源集成到Flink中，使Flink能够从MySql数据库中消费事件
        return FlinkSourceProvider.of(proxy);
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        return new MySqlMetadataAccessor(sourceConfig);
    }

    @VisibleForTesting
    public MySqlSourceConfig getSourceConfig() {
        return sourceConfig;
    }
}

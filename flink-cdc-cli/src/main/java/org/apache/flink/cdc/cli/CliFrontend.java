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

package org.apache.flink.cdc.cli;

import org.apache.commons.cli.*;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.cdc.cli.utils.ConfigurationUtils;
import org.apache.flink.cdc.cli.utils.FlinkEnvironmentUtils;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.cli.CliFrontendOptions.SAVEPOINT_ALLOW_NON_RESTORED_OPTION;
import static org.apache.flink.cdc.cli.CliFrontendOptions.SAVEPOINT_CLAIM_MODE;
import static org.apache.flink.cdc.cli.CliFrontendOptions.SAVEPOINT_PATH_OPTION;

/** The frontend entrypoint for the command-line interface of Flink CDC. */
public class CliFrontend {
    private static final Logger LOG = LoggerFactory.getLogger(CliFrontend.class);
    private static final String FLINK_HOME_ENV_VAR = "FLINK_HOME";
    private static final String FLINK_CDC_HOME_ENV_VAR = "FLINK_CDC_HOME";

    private static final String url = "jdbc:mysql://localhost:3306/app_db?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    private static final String url_source = "jdbc:mysql://localhost:3306/source_db?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";

    private static final String user = "root";
    private static final String password = "123456";
    private static final Integer LIMIT_COUNT = 25;
    private static Connection conn1;
    private static Connection conn_source;
    private static ScheduledExecutorService pool = Executors.newSingleThreadScheduledExecutor();
    static {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        //从 source 查出结果
        //写入 sink
        //删除 source
        // 建立连接
        try {
            conn1 = DriverManager.getConnection(url, user, password);
            conn_source = DriverManager.getConnection(url_source, user, password);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    /*
    为什么好像没启动 MysqlBinlogSplitAssigner

    SplitFetcher runOnce()
        如果 !taskQueue.isEmpty() 运行 addTask，并把 task 放入 assignedSplits。
        如果 !assignedSplits.isEmpty() 运行 fetchTask

        AddSplitsTask   把 splits 放到 MySqlSplitReader 的 snapshotSplits 和 binlogSplits 里
        FetchTask       让 MySqlSplitReader 去 上面的两个 list 里获取并处理 split，后面调用 SnapshotSplitReader::

    1. 13:57:19.624 [flink-pekko.actor.default-dispatcher-5] INFO  org.apache.flink.runtime.source.coordinator.SourceCoordinator  - Starting split enumerator for source Source: Flink CDC Event Source: mysql
    2. MySqlSnapshotSplitAssigner 获取数据库所有表，按配置的 chunkSize， 按主键均匀或者不均匀 地 切分成 splits，保存在 remainingSplits 里
    3. MySqlSourceEnumerator  splitReader 向 enumerator 请求 split， enumerator 收到请求后，拿出 split 分配给 enumerator
    4. MysqlSourceReader 收到 splits 后，调用 super.addSplits(unfinishedSplits); 交给 SourceReaderBase 去处理
        4.0 SourceReaderBase 交给 SingleThreadFetcherManager 去处理，manager先创建 fetcher，再把 splits 放到 fetcher的队列里，再把 fetcher 扔到线程池里去执行
                fetcher 会去循环执行 runOnce()，从队列里拿出任务执行
        4.1 SourceReaderBase 会创建一个单独的线程(如果不存在则创建)，然后把 splits 放到线程的队列里，异步地让线程去执行，为的是不阻塞主流程
        4.2 最后其实调用的是 MySqlSource 里创建的 SplitReader。 splitReader.handleSplitsChanges(new SplitsAddition<>(splitsToAdd));
        4.3 SplitReader 按 binlog 的类型，放到 snapshotSplits 和 binlogSplits 里
        4.4 然后 SplitFetcher 会执行 FetchTask，最终调用的是 SnapshotSplitReader::submitSplit(MySqlSplit mySqlSplit);

        4.5 FetchTask 会往 elementsQueue 里放元素，OutputOperator 会调 SourceReaderBase::pollNext，取出元素后，由 recordEmitter.emitRecord(record, currentSplitOutput, currentSplitContext.state);
        4.6 recordEmitter::emitRecord 会执行 debeziumDeserializationSchema.deserialize(element, outputCollector); 转成 event，然后再发送

    5.1 提交 snapshotsplit 时，snapshotsplitreader 会创建 splitSnapshotReadTask 并扔到线程池里执行
    5.2 splitSnapshotReadTask 从数据源读取数据后，封装成 SourceRecord，再封装成 DataChangeEvent， 放到queue 里
    5.3 MySqlSplitReader 的 pollSplitRecords 会从 queue 里取出数据，封装成 SourceRecords，然后返回

    1. 先读取 snapshot，读取前获取当前binlog的位置，记为 low，读取完数据后，放入list里，获取现在binlog的位置，记为 high
    2. 如果 low == high, 证明当前数据库没有产生binlog，这些数据直接就能用
    3. 如果 low < high, 表示在此期间数据库有binlog产生，需要获取此期间的binlog，如果这些binlog数据里有涉及到 list 里的，以binlog里的数据为准

    带来一个问题，什么时候消费binlog?
    多并行度的情况下，怎么并行消费 binlog?


        MySqlSourceReader::addSplits(unfinishedSplits);
            MySqlSplitReader::handleSplitsChanges(new SplitsAddition<>(splitsToAdd));
            snapshotSplits.add(mySqlSplit.asSnapshotSplit());


    SnapshotSplitReader 和 BinlogSplitReader 共同合作，才能保证数据不缺不漏
        1. SnapshotSplitReader 接收 snapshotSplit[比如:只处理id在(1,1000) 之内的数据]，记录 low offset，读取数据后，记录 high offset
            1.1 如果 low != high，则新建 binlogSplitTask，从数据库读取出 [low,high] 之间的 binlog，组成 low|snapshot|high|binlog|end 的数据格局
            1.2 然后把 binlog 和 snapshot 合并，此动作意味着， 在binlog里，id位于(1,1000) 之内的数据， high offset 以前的，已经处理完了
            1.3 最后把 snapshotSplit 和 high offset 保存到本地，同时也上传到 enumerator 上

        2. 所有 snapshotSplits 全都处理完成后，enumerator 有了所有的 split 的处理结果即 high offset，
            2.1 选出 最小的 offset，此 offset 表示以前的所有 id，都不需要处理了
            2.2 接下来要创建一个一直运行的 binlogSplit，start offset 为此 offset，没有 end offset，把此 binlogSplit 发送给所有 subtask

        3. BinlogSplitReader 接收 binlogSplit，从 start offset 按顺序读取数据，放到 queue 里
            3.1 当要发送到输出流的时候，即调 pollSplitRecords 的时候，会从 queue 里获取数据，判断该数据 record 是否需要发送，不需要发送就丢弃
            3.2 因为每个 snapshotSplit 处理范围不一样，high offset 也可能不一样， 这个 high offset 代表小于此 offset 的 特定id 范围的数据，不需要处理
            3.3 binlogSplitReader 会获取 当前 slot(subtask) 上处理过的 snapshotSplits，根据 record 找到 snapshotSplit，然后判断 record 的 offset 是否大于 snapshotSplit 的 high offset
            3.4 如果没找到 snapshotSplit，说明这条数据不需要这个 subtask 处理，如果不大于 high offset，说明不需要处理

     */

    public static void main(String[] args) throws Exception {
        //开启数据同步
        pool.scheduleWithFixedDelay(() -> {
            sinkData(fetchData());
            delData();
        }, 0, 1, java.util.concurrent.TimeUnit.MILLISECONDS);

        Options cliOptions = CliFrontendOptions.initializeOptions();
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(cliOptions, args);

        // Help message
        if (args.length == 0 || commandLine.hasOption(CliFrontendOptions.HELP)) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setLeftPadding(4);
            formatter.setWidth(80);
            formatter.printHelp(" ", cliOptions);
            return;
        }

        // Create executor and execute the pipeline
        PipelineExecution.ExecutionInfo result = createExecutor(commandLine).run();

        // Print execution result
        printExecutionInfo(result);
    }

    @VisibleForTesting
    static CliExecutor createExecutor(CommandLine commandLine) throws Exception {
        // The pipeline definition file would remain unparsed
        List<String> unparsedArgs = commandLine.getArgList();
        if (unparsedArgs.isEmpty()) {
            throw new IllegalArgumentException(
                    "Missing pipeline definition file path in arguments. ");
        }

        // Take the first unparsed argument as the pipeline definition file
        Path pipelineDefPath = Paths.get(unparsedArgs.get(0));
        if (!Files.exists(pipelineDefPath)) {
            throw new FileNotFoundException(
                    String.format("Cannot find pipeline definition file \"%s\"", pipelineDefPath));
        }

        // Global pipeline configuration
        Configuration globalPipelineConfig = getGlobalConfig(commandLine);

        // Load Flink environment
        Path flinkHome = getFlinkHome(commandLine);
        Configuration flinkConfig = FlinkEnvironmentUtils.loadFlinkConfiguration(flinkHome);

        // Savepoint
        SavepointRestoreSettings savepointSettings = createSavepointRestoreSettings(commandLine);

        // Additional JARs
        List<Path> additionalJars =
                Arrays.stream(
                                Optional.ofNullable(
                                                commandLine.getOptionValues(CliFrontendOptions.JAR))
                                        .orElse(new String[0]))
                        .map(Paths::get)
                        .collect(Collectors.toList());

        // Build executor
        return new CliExecutor(
                pipelineDefPath,
                flinkConfig,
                globalPipelineConfig,
                commandLine.hasOption(CliFrontendOptions.USE_MINI_CLUSTER),
                additionalJars,
                savepointSettings);
    }

    private static SavepointRestoreSettings createSavepointRestoreSettings(
            CommandLine commandLine) {
        if (commandLine.hasOption(SAVEPOINT_PATH_OPTION.getOpt())) {
            String savepointPath = commandLine.getOptionValue(SAVEPOINT_PATH_OPTION.getOpt());
            boolean allowNonRestoredState =
                    commandLine.hasOption(SAVEPOINT_ALLOW_NON_RESTORED_OPTION.getOpt());
            final RestoreMode restoreMode;
            if (commandLine.hasOption(SAVEPOINT_CLAIM_MODE)) {
                restoreMode =
                        org.apache.flink.configuration.ConfigurationUtils.convertValue(
                                commandLine.getOptionValue(SAVEPOINT_CLAIM_MODE),
                                RestoreMode.class);
            } else {
                restoreMode = SavepointConfigOptions.RESTORE_MODE.defaultValue();
            }
            // allowNonRestoredState is always false because all operators are predefined.
            return SavepointRestoreSettings.forPath(
                    savepointPath, allowNonRestoredState, restoreMode);
        } else {
            return SavepointRestoreSettings.none();
        }
    }

    private static Path getFlinkHome(CommandLine commandLine) {
        // Check command line arguments first
        String flinkHomeFromArgs = commandLine.getOptionValue(CliFrontendOptions.FLINK_HOME);
        if (flinkHomeFromArgs != null) {
            LOG.debug("Flink home is loaded by command-line argument: {}", flinkHomeFromArgs);
            return Paths.get(flinkHomeFromArgs);
        }

        // Fallback to environment variable
        String flinkHomeFromEnvVar = System.getenv(FLINK_HOME_ENV_VAR);
        if (flinkHomeFromEnvVar != null) {
            LOG.debug("Flink home is loaded by environment variable: {}", flinkHomeFromEnvVar);
            return Paths.get(flinkHomeFromEnvVar);
        }

        throw new IllegalArgumentException(
                "Cannot find Flink home from either command line arguments \"--flink-home\" "
                        + "or the environment variable \"FLINK_HOME\". "
                        + "Please make sure Flink home is properly set. ");
    }

    private static Configuration getGlobalConfig(CommandLine commandLine) throws Exception {
        // Try to get global config path from command line
        String globalConfig = commandLine.getOptionValue(CliFrontendOptions.GLOBAL_CONFIG);
        if (globalConfig != null) {
            Path globalConfigPath = Paths.get(globalConfig);
            LOG.info("Using global config in command line: {}", globalConfigPath);
            return ConfigurationUtils.loadConfigFile(globalConfigPath);
        }

        // Fallback to Flink CDC home
        String flinkCdcHome = System.getenv(FLINK_CDC_HOME_ENV_VAR);
        if (flinkCdcHome != null) {
            Path globalConfigPath =
                    Paths.get(flinkCdcHome).resolve("conf").resolve("flink-cdc.yaml");
            LOG.info("Using global config in FLINK_CDC_HOME: {}", globalConfigPath);
            return ConfigurationUtils.loadConfigFile(globalConfigPath);
        }

        // Fallback to empty configuration
        LOG.warn(
                "Cannot find global configuration in command-line or FLINK_CDC_HOME. Will use empty global configuration.");
        return new Configuration();
    }

    private static void printExecutionInfo(PipelineExecution.ExecutionInfo info) {
        System.out.println("Pipeline has been submitted to cluster.");
        System.out.printf("Job ID: %s\n", info.getId());
        System.out.printf("Job Description: %s\n", info.getDescription());
    }

    private static List<Map<String, Object>> fetchData() {

        List<Map<String, Object>> list = new LinkedList<>();

        String sql = "select code, name, level, pcode, category from area_code_2024 order by code asc limit " + LIMIT_COUNT;
        try (Statement stmt = conn_source.createStatement();
             ResultSet rs = stmt.executeQuery(sql);
        ) {
            while (rs.next()) {
                HashMap<String, Object> hashMap = new HashMap<String, Object>() {
                    {
                        put("code", rs.getLong("code"));
                        put("name", rs.getString("name"));
                        put("level", rs.getLong("level"));
                        put("pcode", rs.getLong("pcode"));
                        put("category", rs.getLong("category"));
                    }
                };
                list.add(hashMap);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    private static void sinkData(List<Map<String, Object>> data) {
        String sql = "INSERT INTO area_code_2024 (code, name, level, pcode, category) VALUES (?, ?, ?, ?, ?)";
        try (
                // 创建PreparedStatement
                PreparedStatement pstmt = conn1.prepareStatement(sql);
        ) {
            // 遍历数据列表，为每个数据项设置参数并添加到批处理中
            for (Map<String, Object> map : data) {
                pstmt.setLong(1, Long.parseLong(map.get("code").toString()));
                pstmt.setString(2, map.get("name").toString());
                pstmt.setLong(3, Long.parseLong(map.get("level").toString()));
                pstmt.setLong(4, Long.parseLong(map.get("pcode").toString()));
                pstmt.setLong(5, Long.parseLong(map.get("category").toString()));
                pstmt.addBatch(); // 将此SQL语句添加到批处理中
            }

            // 执行批处理
            int[] affectedRows = pstmt.executeBatch();

            System.out.println("成功插入了 " + affectedRows.length + " 行数据。");

            // 注意：这里不需要显式关闭pstmt和conn，因为使用了try-with-resources语句
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void delData() {

        String sql = "delete from area_code_2024 order by code asc limit " + LIMIT_COUNT;
        try (
                // 创建PreparedStatement
                PreparedStatement pstmt = conn_source.prepareStatement(sql);
        ) {
            // 遍历数据列表，为每个数据项设置参数并添加到批处理中


            // 执行批处理
            int affectedRows = pstmt.executeUpdate();

            System.out.println("成功删除了 " + affectedRows + " 行数据。");

            // 注意：这里不需要显式关闭pstmt和conn，因为使用了try-with-resources语句
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

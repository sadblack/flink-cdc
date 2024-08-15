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
    private static final Integer LIMIT_COUNT = 10;
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

    public static void main(String[] args) throws Exception {
        //开启数据同步
        pool.scheduleWithFixedDelay(() -> {
            sinkData(fetchData());
            delData();
        }, 0, 1000, java.util.concurrent.TimeUnit.MILLISECONDS);


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

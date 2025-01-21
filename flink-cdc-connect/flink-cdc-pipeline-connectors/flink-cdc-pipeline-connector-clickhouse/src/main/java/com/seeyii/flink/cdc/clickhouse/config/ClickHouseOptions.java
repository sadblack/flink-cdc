package com.seeyii.flink.cdc.clickhouse.config;


import com.seeyii.flink.cdc.clickhouse.internal.options.ClickHouseConnectionOptions;

public class ClickHouseOptions {

    private String username;
    private String password;
    private String url;
    private String database;
    private String tableName;

    public ClickHouseOptions(String username, String password, String url, String database, String tableName) {
        this.username = username;
        this.password = password;
        this.url = url;
        this.database = database;
        this.tableName = tableName;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getUrl() {
        return url;
    }

    public String getDatabase() {
        return database;
    }

    public String getTableName() {
        return tableName;
    }

    public ClickHouseConnectionOptions toConnectionOptions() {
        return new ClickHouseConnectionOptions(url, username, password, database, tableName);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String username;
        private String password;
        private String url;
        private String database;
        private String tableName;

        public Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setUrl(String url) {
            this.url = url;
            return this;
        }

        public Builder setDatabase(String database) {
            this.database = database;
            return this;
        }

        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public ClickHouseOptions build() {

            return new ClickHouseOptions(username, password, url, database, tableName);
        }
    }
}

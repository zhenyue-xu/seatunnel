package org.apache.seatunnel.connectors.greenplum.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;

import java.io.Serializable;

public class GreenplumConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String tableName;
    private final String delimiter;
    private final String nullString;
    private final int batchSize;
    private final int queueSize;
    private final int parallelism;
    
    // Optional fields
    private final String query;
    private final String partitionColumn;
    private final Long partitionLowerBound;
    private final Long partitionUpperBound;
    private final String columnDefinitions;
    
    private GreenplumConfig(Builder builder) {
        this.jdbcUrl = builder.jdbcUrl;
        this.username = builder.username;
        this.password = builder.password;
        this.tableName = builder.tableName;
        this.delimiter = builder.delimiter;
        this.nullString = builder.nullString;
        this.batchSize = builder.batchSize;
        this.queueSize = builder.queueSize;
        this.parallelism = builder.parallelism;
        this.query = builder.query;
        this.partitionColumn = builder.partitionColumn;
        this.partitionLowerBound = builder.partitionLowerBound;
        this.partitionUpperBound = builder.partitionUpperBound;
        this.columnDefinitions = builder.columnDefinitions;
    }
    
    public static GreenplumConfig buildFromConfig(ReadonlyConfig config) {
        Builder builder = new Builder()
            .jdbcUrl(config.get(GreenplumOptions.URL))
            .username(config.get(GreenplumOptions.USERNAME))
            .password(config.get(GreenplumOptions.PASSWORD))
            .tableName(config.get(GreenplumOptions.TABLE))
            .delimiter(config.get(GreenplumOptions.DELIMITER))
            .nullString(config.get(GreenplumOptions.NULL_STRING))
            .batchSize(config.get(GreenplumOptions.BATCH_SIZE))
            .queueSize(config.get(GreenplumOptions.QUEUE_SIZE))
            .parallelism(config.get(GreenplumOptions.PARALLELISM));
        
        // Optional fields
        if (config.getOptional(GreenplumOptions.QUERY).isPresent()) {
            builder.query(config.get(GreenplumOptions.QUERY));
        }
        if (config.getOptional(GreenplumOptions.PARTITION_COLUMN).isPresent()) {
            builder.partitionColumn(config.get(GreenplumOptions.PARTITION_COLUMN));
        }
        if (config.getOptional(GreenplumOptions.PARTITION_LOWER_BOUND).isPresent()) {
            builder.partitionLowerBound(config.get(GreenplumOptions.PARTITION_LOWER_BOUND));
        }
        if (config.getOptional(GreenplumOptions.PARTITION_UPPER_BOUND).isPresent()) {
            builder.partitionUpperBound(config.get(GreenplumOptions.PARTITION_UPPER_BOUND));
        }
        if (config.getOptional(GreenplumOptions.COLUMN_DEFINITIONS).isPresent()) {
            builder.columnDefinitions(config.get(GreenplumOptions.COLUMN_DEFINITIONS));
        }
        
        return builder.build();
    }
    
    // Getters
    public String getJdbcUrl() {
        return jdbcUrl;
    }
    
    public String getUsername() {
        return username;
    }
    
    public String getPassword() {
        return password;
    }
    
    public String getTableName() {
        return tableName;
    }
    
    public String getDelimiter() {
        return delimiter;
    }
    
    public String getNullString() {
        return nullString;
    }
    
    public int getBatchSize() {
        return batchSize;
    }
    
    public int getQueueSize() {
        return queueSize;
    }
    
    public int getParallelism() {
        return parallelism;
    }
    
    public String getQuery() {
        return query;
    }
    
    public String getPartitionColumn() {
        return partitionColumn;
    }
    
    public Long getPartitionLowerBound() {
        return partitionLowerBound;
    }
    
    public Long getPartitionUpperBound() {
        return partitionUpperBound;
    }
    
    public String getColumnDefinitions() {
        return columnDefinitions;
    }
    
    // Builder
    public static class Builder {
        private String jdbcUrl;
        private String username;
        private String password;
        private String tableName;
        private String delimiter = "|";
        private String nullString = "\\N";
        private int batchSize = 10000;
        private int queueSize = 100;
        private int parallelism = 4;
        private String query;
        private String partitionColumn;
        private Long partitionLowerBound;
        private Long partitionUpperBound;
        private String columnDefinitions;
        
        public Builder jdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
            return this;
        }
        
        public Builder username(String username) {
            this.username = username;
            return this;
        }
        
        public Builder password(String password) {
            this.password = password;
            return this;
        }
        
        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }
        
        public Builder delimiter(String delimiter) {
            this.delimiter = delimiter;
            return this;
        }
        
        public Builder nullString(String nullString) {
            this.nullString = nullString;
            return this;
        }
        
        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }
        
        public Builder queueSize(int queueSize) {
            this.queueSize = queueSize;
            return this;
        }
        
        public Builder parallelism(int parallelism) {
            this.parallelism = parallelism;
            return this;
        }
        
        public Builder query(String query) {
            this.query = query;
            return this;
        }
        
        public Builder partitionColumn(String partitionColumn) {
            this.partitionColumn = partitionColumn;
            return this;
        }
        
        public Builder partitionLowerBound(Long partitionLowerBound) {
            this.partitionLowerBound = partitionLowerBound;
            return this;
        }
        
        public Builder partitionUpperBound(Long partitionUpperBound) {
            this.partitionUpperBound = partitionUpperBound;
            return this;
        }
        
        public Builder columnDefinitions(String columnDefinitions) {
            this.columnDefinitions = columnDefinitions;
            return this;
        }
        
        public GreenplumConfig build() {
            return new GreenplumConfig(this);
        }
    }
}
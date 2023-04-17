package model;

public class TableMetadata {
    public TableMetadata(String tableName, String columnName, String columnType, boolean clusteringKey, String indexName, String indexType, String min, String max) {
        this.tableName = tableName;
        ColumnName = columnName;
        ColumnType = columnType;
        ClusteringKey = clusteringKey;
        IndexName = indexName;
        IndexType = indexType;
        this.min = min;
        this.max = max;
    }

    public TableMetadata() {
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getColumnName() {
        return ColumnName;
    }

    public void setColumnName(String columnName) {
        ColumnName = columnName;
    }

    public String getColumnType() {
        return ColumnType;
    }

    public void setColumnType(String columnType) {
        ColumnType = columnType;
    }

    public boolean getClusteringKey() {
        return ClusteringKey;
    }

    public void setClusteringKey(boolean clusteringKey) {
        ClusteringKey = clusteringKey;
    }

    public String getIndexName() {
        return IndexName;
    }

    public void setIndexName(String indexName) {
        IndexName = indexName;
    }

    public String getIndexType() {
        return IndexType;
    }

    public void setIndexType(String indexType) {
        IndexType = indexType;
    }

    public String getMin() {
        return min;
    }

    public void setMin(String min) {
        this.min = min;
    }

    public String getMax() {
        return max;
    }

    public void setMax(String max) {
        this.max = max;
    }

    String tableName;
    String ColumnName;
    String ColumnType;
    boolean ClusteringKey = false;
    String IndexName;
    String IndexType;
    String min;
    String max;
}

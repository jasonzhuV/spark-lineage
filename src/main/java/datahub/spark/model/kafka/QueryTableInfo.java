package datahub.spark.model.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author : zhupeiwen
 * @date : 2023/7/17
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class QueryTableInfo implements Message {
    /**
     * source: 查询
     * target: 插入
     */
    private String recordType;
    private String database;
    private Long duration;
    private String engine;
    private String queryText;
    private String operationName;
    private Long timestamp;
    private String user;
    private String[] userGroupNames;
    private QueryTable queryTable;
}

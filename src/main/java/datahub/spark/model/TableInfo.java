package datahub.spark.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author : zhupeiwen
 * @date : 2023/7/12
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class TableInfo {
    private String dbName;
    private String tableName;
}
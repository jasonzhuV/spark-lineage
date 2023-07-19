package datahub.spark.model.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * @author : zhupeiwen
 * @date : 2023/7/17
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class QueryTable {
    private String database;
    private String table;
}

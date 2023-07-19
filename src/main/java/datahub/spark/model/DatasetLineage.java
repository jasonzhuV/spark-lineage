package datahub.spark.model;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import datahub.spark.model.dataset.SparkDataset;

import lombok.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class DatasetLineage {

  private Set<SparkDataset> sources = new HashSet<>();
  private String callSiteShort;
  private String plan;
  private SparkDataset sink;

  public void addSource(SparkDataset source) {
    sources.add(source);
  }

  public Set<SparkDataset> getSources() {
    return Collections.unmodifiableSet(sources);
  }
}

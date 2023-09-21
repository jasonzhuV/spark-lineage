package datahub.test;

import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author : zhupeiwen
 * @date : 2023/7/18
 */
public class TestHashCode {
    public static void main(String[] args) {
        Set<String> sourceNames;
        sourceNames = new TreeSet<>(new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        });

//        sourceNames.add("test_zhupeiwen.test_table_count_and_table_name_influence_lineage_0");
//        sourceNames.add("test_zhupeiwen.test_table_count_and_table_name_influence_lineage_1");
//        sourceNames.add("test_zhupeiwen.test_table_count_and_table_name_influence_lineage_2");
//        sourceNames.add("test_zhupeiwen.ingest_2");
//        sourceNames.add("test_zhupeiwen.test_table_count_and_table_name_influence_lineage_3");
//        sourceNames.add("test_zhupeiwen.test_table_count_and_table_name_influence_lineage_4");
//        sourceNames.add("test_zhupeiwen.test_table_count_and_table_name_influence_lineage_5");
//        sourceNames.add("test_zhupeiwen.test_table_count_and_table_name_influence_lineage_6");
//        sourceNames.add("test_zhupeiwen.test_table_count_and_table_name_influence_lineage_7");
//        sourceNames.add("test_zhupeiwen.test_table_count_and_table_name_influence_lineage_8");
//        sourceNames.add("test_zhupeiwen.test_table_count_and_table_name_influence_lineage_9");
//        sourceNames.add("test_zhupeiwen.test_table_count_and_table_name_influence_lineage_10");
//        sourceNames.add("test_zhupeiwen.test_table_count_and_table_name_influence_lineage_11");
        sourceNames.add("hdp_loanbank_dw.h_dmd_xw_query_info_merge_d");
        sourceNames.add("hdp_loanbank_ods.prd_bceapv_ap_apply");
        sourceNames.add("hdp_loanbank_ods.prd_ccrs_vzoomtax_nsrjcxx_mongo");
//        sourceNames.add("hdp_ods.hd_db_klcs_lcs_contract_t_rt");
        //1281084323

        sourceNames.add("hdp_ods.db_klcs_lcs_contract_t");
        //722307539


        System.out.println(String.join("^", sourceNames));
        System.out.println(String.valueOf(sourceNames.hashCode() & Integer.MAX_VALUE));
        //812407900
        //1352562592


    }
}

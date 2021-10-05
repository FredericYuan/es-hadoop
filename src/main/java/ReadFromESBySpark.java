import com.google.gson.Gson;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.Map;

public class ReadFromESBySpark {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("my-app").clone()
                .set("es.nodes", "116.198.53.17")
                .set("es.port", "9200")
                .set("es.nodes.wan.only", "true")
                .set("es.input.use.sliced.partitions", "false")
                .setMaster("local[*]") //本地调试
                .set("es.input.max.docs.per.partition", "100000000");

        JavaSparkContext sc = new JavaSparkContext(conf);
        //浏览器直接查询：http://116.198.53.17:9200/spark2021/docs/_search?q=name:tom
        JavaPairRDD<String, Map<String, Object>> rdd = JavaEsSpark.esRDD(sc, "spark2021/docs", "?q=name:john");
        for (Map<String, Object> item : rdd.values().collect()) {
            System.out.println(new Gson().toJson(item));
        }
        sc.stop();
    }
}
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WriteToESUseRDD {

    public static void main(String[] args) {

        /**
         * https://www.elastic.co/cn/what-is/elasticsearch-hadoop
         * https://blog.csdn.net/qq_24365213/article/details/76850746
         * https://cloud.tencent.com/developer/article/1380432?from=10680
         *
         * zip -d es-hadoop.jar META-INF/*.RSA META-INF/*.DSA META-INF/*.SF
         * spark-submit --jars elasticsearch-spark-20_2.11-5.6.2.jar --class "WriteToESUseRDD" es-hadoop.jar
         * spark-submit --class "WriteToESUseRDD" es-hadoop.jar
         *
         * http://116.198.53.17:9200/spark2021/docs/_search/
         */
        SparkConf conf = new SparkConf().setAppName("my-app").clone()
                .set("es.nodes", "116.198.53.17")
                .set("es.port", "9200")
                .set("es.nodes.wan.only", "true")
                .set("es.batch.size.bytes", "30M")
                .set("es.batch.size.entries", "20000")
                .set("es.batch.write.refresh", "false")
                .set("es.batch.write.retry.count", "50")
                .set("es.batch.write.retry.wait", "500s")
                .set("es.http.timeout", "5m")
                .set("es.http.retries", "50")
//                .setMaster("116.198.53.17")
//                .setMaster("local[*]") //本地调试用
                .set("es.action.heart.beat.lead", "50s");

        JavaSparkContext sc = new JavaSparkContext(conf);

//        Map<String, ?> logs = ImmutableMap.of("clientip", "255.255.255.254",
//                "request", "POST /write/using_spark_rdd HTTP/1.1",
//                "status", 200,"size", 802,
//                "@timestamp", 895435190);
//        List<Map<String, ?>> list = ImmutableList.of(logs);
//        JavaRDD<Map<String, ?>> javaRDD = sc.parallelize(list);
//        JavaEsSpark.saveToEs(javaRDD, "logs-201998/type");
        List<Map<String, Object>> list = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();
        map.put("id", 1);
        map.put("name", "tom");
        map.put("age", 19);
        list.add(map);
        map = new HashMap<>();
        map.put("id", 2);
        map.put("name", "john");
        map.put("age", 25);
        list.add(map);
        for(int i=2; i <= 50; i++) {
            map = new HashMap<>();
            map.put("id", i);
            map.put("name", "john");
            map.put("age", 25);
            list.add(map);
        }
        JavaRDD<Map<String, Object>> javaRDD = sc.<Map<String, Object>>parallelize(list);
        JavaEsSpark.saveToEs(javaRDD, "spark2021/docs");
        System.out.println("存储成功！");
        sc.stop();
    }
}
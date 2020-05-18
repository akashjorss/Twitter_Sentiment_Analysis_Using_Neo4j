import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {
	
	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf().setAppName("SparkTraining").setMaster("local[*]");
        JavaSparkContext ctx = new JavaSparkContext(conf);

        System.out.println(Show_Tweets.basicAnalysis(ctx));
	}
}


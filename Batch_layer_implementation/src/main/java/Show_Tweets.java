import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.util.parsing.json.JSONObject;

public class Show_Tweets {
	
	public static String basicAnalysis(JavaSparkContext ctx) {
		String out = "";
		
		JavaRDD<String> tweetsRDD = ctx.textFile("src/main/resources/sample_tweets.json");

		out += "The file has "+tweetsRDD.count()+" tweets\n";
		out += "#################################\n";

		out += "The first five tweets have the following text:\n";
		for (String line : tweetsRDD.take(5)) {
			JsonObject tweet = new Gson().fromJson(line, JsonObject.class);
			out += "	"+tweet.get("text");
			out += "\n";
		}
		out += "#################################\n";
		
		return out;
	}
}


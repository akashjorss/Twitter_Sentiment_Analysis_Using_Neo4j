import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;


import com.google.common.io.Files;

import BasicSentimentAnalysis.BasicSentimentAnalysis;
import StreamProcessingFunctions.DecayingExponential;
import twitter4j.Status;

public class Main {

	static String TWITTER_CONFIG_PATH = "SET PATH TO TWITTER CONFIGURATION FILE HERE";
	static String HADOOP_COMMON_PATH = "SET YOUR HADOOP COMMON FILE PATH HERE";


	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", HADOOP_COMMON_PATH);
		SparkConf conf = new SparkConf().setAppName("SparkStreamingTraining").setMaster("local[*]");
		conf.set("spark.driver.bindAddress", "127.0.0.1");
		JavaSparkContext ctx = new JavaSparkContext(conf);
		JavaStreamingContext jsc = new JavaStreamingContext(ctx, new Duration(1000));
		LogManager.getRootLogger().setLevel(Level.ERROR);
		LogManager.shutdown();
		jsc.checkpoint(Files.createTempDir().getAbsolutePath());
		Utils.setupTwitter(TWITTER_CONFIG_PATH);
		JavaDStream<Status> tweets = TwitterUtils.createStream(jsc);

		if (args[0].equals("BasicSentimentAnalysis")) {
			BasicSentimentAnalysis.sentimentAnalysis(tweets);
		}
		else if (args[0].equals("StreamProcessingFunctions")) {
			DecayingExponential.decayingWindow(tweets);
		}

		jsc.start();
		jsc.awaitTermination();
	}

}

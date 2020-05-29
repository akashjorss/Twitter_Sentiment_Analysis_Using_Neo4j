package BasicSentimentAnalysis;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;
import scala.Tuple5;
import twitter4j.Status;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class BasicSentimentAnalysis {

	public static void sentimentAnalysis(JavaDStream<Status> statuses) {
		JavaPairDStream<Tuple2<Long, String>, String> coronaTweetsInEnglish = statuses.filter(status -> {
			String tweetText = status.getText();
			boolean englishTweet = Objects.nonNull(tweetText) && LanguageDetector.isEnglish(tweetText);
//          ideally we should have converted tweet to lowercase here and did a contains check with COVID and coronavirus
//         as lowercase. Not doing it as question explicitly states only filter on these words as is.
			return englishTweet && (tweetText.contains("COVID") || tweetText.contains("coronavirus"));
		}).mapToPair(status -> {
			Tuple2<Long, String> tweetKey = new Tuple2<>(status.getId(), status.getText());
			String standardizedTweet = status.getText().replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase();
			return new Tuple2<>(tweetKey, standardizedTweet);
		});
		JavaPairDStream<Tuple2<Long, String>, List<String>> stemmedTweetsCached = coronaTweetsInEnglish.mapValues(tweetText -> {
			String[] words = tweetText.split(" ");
			return Arrays.stream(words).filter(word -> !StopWords.getWords().contains(word)).collect(Collectors.toList());
		}).cache();
		JavaPairDStream<Tuple2<Long, String>, Float> positiveScoreStream = stemmedTweetsCached.mapValues(words -> {
			long positiveWordCount = words.stream().filter(word -> PositiveWords.getWords().contains(word)).count();
			return ((float) positiveWordCount) / words.size();
		});
		JavaPairDStream<Tuple2<Long, String>, Float> negativeScoreStream = stemmedTweetsCached.mapValues(words -> {
			long positiveWordCount = words.stream().filter(word -> NegativeWords.getWords().contains(word)).count();
			return ((float) positiveWordCount) / words.size();
		});
		JavaPairDStream<Tuple2<Long, String>, Tuple2<Float, Float>> combinedScores = positiveScoreStream.join(negativeScoreStream);
		combinedScores.map(pair -> {
			Long tweetId = pair._1._1;
			String tweetText = pair._1._2;
			Float positiveScore = pair._2._1;
			Float negativeScore = pair._2._2;
			String sentiment = "Neutral";
			if (positiveScore > negativeScore) {
				sentiment = "Positive";
			}
			if (negativeScore > positiveScore) {
				sentiment = "Negative";
			}
			return new Tuple5<>(tweetId, tweetText, positiveScore, negativeScore, sentiment);
		}).print();
	}
}
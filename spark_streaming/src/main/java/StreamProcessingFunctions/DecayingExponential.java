package StreamProcessingFunctions;

import com.google.common.collect.Iterators;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;
import twitter4j.Status;

import java.util.List;

public class DecayingExponential {

    public static final double DECAY_CONSTANT = (1 - Math.pow(10, -9));
    public static final double THRESHOLD = (1.0 / 4.0);

    public static void decayingWindow(JavaDStream<Status> statuses) {
        JavaDStream<String> words = statuses
                .flatMap(status -> Iterators.forArray(status.getText().split(" ")));

        JavaDStream<String> hashTags = words
                .filter(word -> word.startsWith("#"));

        JavaPairDStream<String, Integer> tuples = hashTags.
                mapToPair(hashtag -> new Tuple2<>(hashtag, 1));

        Function2<List<Integer>, Optional<Double>, Optional<Double>> updateFunction =
                (values, state) -> {
                    Double currentValue = state.or(0.0);
                    Double newValue = currentValue * DECAY_CONSTANT;
                    for (int i : values) {
                        newValue += i;
                    }
                    if (newValue < THRESHOLD) {
                        return Optional.empty();
                    }
                    return Optional.of(newValue);
                };

        JavaPairDStream<String, Double> counts = tuples.updateStateByKey(updateFunction);
        JavaPairDStream<Double, String> swappedCounts = counts.mapToPair(Tuple2::swap);
        JavaPairDStream<Double, String> sortedCounts = swappedCounts.transformToPair(count -> count.sortByKey(false));

        /* Explanation

          1. If we are supposed to calculate top 10 in all history then exponential decay does not work, as we drop off elements
          which do not meet the threshold criteria and since old hash tags will keep decaying until they dropped out even if once they
          had a high count.

          2. For this same reason, computing median (50% percentile) does not make sense for decaying window, tweets smaller than
          threshold will not be seen, given us a wrong impression of what the median is.
         */
        sortedCounts.foreachRDD(rdd -> System.out.println("Top 20 Most Recent: " + rdd.take(20)));
    }
}
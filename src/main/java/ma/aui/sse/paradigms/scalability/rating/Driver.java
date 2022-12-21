package ma.aui.sse.paradigms.scalability.rating;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;

public class Driver {

        // Master URL and application name for Spark
        private static final String MASTER_URL = "spark://10.10.10.10:7070";
        private static final String APP_NAME = "Rating";
        // Number of top rated products to retrieve
        private static final int N = 10;

        public static void main(String[] args) {
                // Create a SparkConf object with the specified app name and master URL
                SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster(MASTER_URL);
                // Create a JavaSparkContext object using the SparkConf object
                JavaSparkContext sc = new JavaSparkContext(conf);

                // Create a list of Rating objects
                List<Rating> ratings = null; // To do

                // Parallelize the list of Rating objects over an RDD
                JavaRDD<Rating> ratingRDD = sc.parallelize(ratings);

                // Convert the ratingRDD into a pair RDD, where each element is a tuple
                // containing the productId and stars fields of the Rating object
                JavaPairRDD<String, Integer> indexedRatingRDD = ratingRDD
                                .mapToPair(rating -> new Tuple2<>(rating.getProductId(), rating.getStars()));

                // Use reduceByKey() to sum the ratings for each product, resulting in a pair
                // RDD containing the product IDs and the sum of the ratings for each product
                JavaPairRDD<String, Integer> ratingSumByProductRDD = indexedRatingRDD.reduceByKey((a, b) -> a + b);

                // Use mapToPair() to convert the indexedRatingRDD into a pair RDD where each
                // element is a tuple containing the productId and a count of 1
                // Use reduceByKey() to sum the counts for each product, resulting in a pair RDD
                // containing the product IDs and the count of ratings for each product
                JavaPairRDD<String, Integer> ratingCountByProductRDD = indexedRatingRDD
                                .mapToPair(indexedRating -> new Tuple2<>(indexedRating._1, 1))
                                .reduceByKey((a, b) -> a + b);

                // Use join() to join the two pair RDDs containing the sum of ratings and the
                // count of ratings for each product
                // This results in a pair RDD containing the product IDs, the sum of ratings,
                // and the count of ratings for each product
                JavaPairRDD<String, Tuple2<Integer, Integer>> ratingSumAndCountByProductRDD = ratingSumByProductRDD
                                .join(ratingCountByProductRDD);

                // Use mapToPair() to convert the resulting pair RDD into a pair RDD containing
                // the product IDs and the average rating for each product
                JavaPairRDD<String, Double> averageRatingByProductRDD = ratingSumAndCountByProductRDD
                                .mapToPair(sumAndCountByProduct -> new Tuple2<>(sumAndCountByProduct._1,
                                                (double) sumAndCountByProduct._2._1 / sumAndCountByProduct._2._2));

                // Retrieve the list of average ratings
                List<Tuple2<String, Double>> averageRatingsByProduct = averageRatingByProductRDD.collect();

                // Retrieve an iterator for the list of average ratings
                Iterator<Tuple2<String, Double>> it = averageRatingsByProduct.iterator();

                // Iterate through the list of average ratings
                while (it.hasNext()) {
                        // Retrieve the next rating
                        Tuple2<String, Double> rating = it.next();
                        // Print the product ID and average rating
                        System.out.println(rating._1 + " : " + rating._2);
                }

                // Determine weakly-rated products
                // Use filter() to filter the averageRatingByProductRDD to only include products
                // with average rating strictly less than 3
                JavaPairRDD<String, Double> weaklyRatedProductsRDD = averageRatingByProductRDD
                                .filter(productRating -> productRating._2 < 3);
                // Retrieve the list of weakly-rated products
                List<Tuple2<String, Double>> weaklyRatedProducts = weaklyRatedProductsRDD.collect();
                // Print the list of weakly-rated products
                System.out.println("Weakly-rated products:");
                weaklyRatedProducts.forEach(
                                productRating -> System.out.println(productRating._1 + " : " + productRating._2));

                // Determine top N rated products
                // Use mapToPair() to convert the averageRatingByProductRDD into a pair RDD
                // where the keys are the average ratings and the values are the product IDs
                // Use sortByKey() to sort the pair RDD in descending order based on the average
                // ratings
                JavaPairRDD<Double, String> invertedAverageRatingByProductRDD = averageRatingByProductRDD
                                .mapToPair(productRating -> new Tuple2<>(productRating._2, productRating._1))
                                .sortByKey(false); // false for descending order
                // Retrieve the top N rated products
                List<Tuple2<Double, String>> topNRatedProducts = invertedAverageRatingByProductRDD.take(N);
                // Print the top N rated products
                System.out.println("Top " + N + " rated products:");
                topNRatedProducts.forEach(
                                productRating -> System.out.println(productRating._2 + " : " + productRating._1));
                // Determine mean and standard deviation of product ratings
                // Convert the averageRatingByProductRDD into a JavaDoubleRDD containing just
                // the values (average ratings)
                JavaDoubleRDD averageRatingsRDD = averageRatingByProductRDD.values().mapToDouble(Double::valueOf);
                // Use mean() to calculate the mean of the average ratings
                double mean = averageRatingsRDD.mean();
                // Use stdev() to calculate the standard deviation of the average ratings
                double stddev = averageRatingsRDD.stdev();
                // Print the mean and standard deviation of the average ratings
                System.out.println("Mean of product ratings: " + mean);
                System.out.println("Standard deviation of product ratings: " + stddev);
                sc.close();
        }
}

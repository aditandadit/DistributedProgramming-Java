package edu.coursera.distributed;

import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;
import java.util.LinkedList;
import java.util.List;

/**
 * A wrapper class for the implementation of a single iteration of the iterative
 * PageRank algorithm.
 */
public final class PageRank {
    /**
     * Default constructor.
     */
    private PageRank() {
    }

    /**
     * Given an RDD of websites and their ranks, compute new ranks for all
     * websites and return a new RDD containing the updated ranks.
     *
     * Recall from lectures that given a website B with many other websites
     * linking to it, the updated rank for B is the sum over all source websites
     * of the rank of the source website divided by the number of outbound links
     * from the source website. This new rank is damped by multiplying it bymvn mvsdfssdfsdfsdfsdfsdfsdfsdfsdfsdf
     * 0.85 and adding that to 0.15. Put more simply:
     *
     *   new_rank(B) = 0.15 + 0.85 * sum(rank(A) / out_count(A)) for all A linking to B
     *
     * For this assignment, you are responsible for implementing this PageRank
     * algorithm using the Spark Java APIs.
     *
     * The reference solution of sparkPageRank uses the following Spark RDD
     * APIs. However, you are free to develop whatever solution makes the most
     * sense to you which also demonstrates speedup on multiple threads.
     *
     *   1) JavaPairRDD.join
     *   2) JavaRDD.flatMapToPair
     *   3) JavaPairRDD.reduceByKey
     *   4) JavaRDD.mapValues
     *
     * @param sites The connectivity of the website graph, keyed on unique
     *              website IDs.
     * @param ranks The current ranks of each website, keyed on unique website
     *              IDs.
     * @return The new ranks of the websites graph, using the PageRank
     *         algorithm to update site ranks.
     */
    public static JavaPairRDD<Integer, Double> sparkPageRank(
            final JavaPairRDD<Integer, Website> sites,
            final JavaPairRDD<Integer, Double> ranks) {

        JavaPairRDD<Integer, Double> newRanks =
                sites.join(ranks) //Join <int, Website> and <int, double>
                        .flatMapToPair(kv -> { //Iterate over join of sites and Ranks
                            Integer websiteId = kv._1(); // <Integer, WebSite, Double> Triple
                            Tuple2<Website, Double> value = kv._2();
                            Website website = kv._2()._1();         // Current Website
                            Double currentRank = kv._2()._2();      // Rank for current Website

                            List<Tuple2<Integer, Double>> contributions = new LinkedList<>();
                            // Add a Contribution for each Edge Going in/out of this Current Website
                            website.edgeIterator().forEachRemaining(
                                    target -> {
                                        contributions.add(new Tuple2(target, currentRank/ (double) website.getNEdges()));
                                        // For each Adjascent Website
                                        // Add a contribution from current Website
                                    }
                            );
                            return contributions;
                        });

        return newRanks
                .reduceByKey((d1,d2) -> d1 + d2)
        // new Ranks has contributions from other website to website with current Key
        // Add all to get total Contributions
                .mapValues(v -> 0.15 + 0.85 * v);
        // use PageRank weighted formula to get actual score


    }
}

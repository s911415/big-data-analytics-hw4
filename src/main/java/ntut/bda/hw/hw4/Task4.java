package ntut.bda.hw.hw4;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

public class Task4 extends Task {
    private static final Logger logger = LogManager.getLogger(Task4.class);
    private static final int IDX_USER_1 = 0;
    private static final int IDX_USER_2 = 1;

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: Task4 <totalCheckins> <edges> [<output dir>]");
            System.exit(1);
        }

        final SparkSession spark = SparkSession
                .builder()
                .appName("BDA_HW4_TASK4")
                .getOrCreate();

        final JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        logger.info("Task4 Running");
        JavaRDD<String> checkIns = spark.read().textFile(args[0]).javaRDD();
        JavaRDD<String> edges = spark.read().textFile(args[1]).javaRDD();

        final Function<Iterable<Integer>, HashSet<Integer>> mapToHashSet = (l) -> {
            final HashSet<Integer> set = new HashSet<>();
            l.forEach(set::add);

            return set;
        };

        final JavaPairRDD<Integer, HashSet<Integer>> locationAndUsers =
                checkIns
                        .mapToPair(s -> {
                                    final String[] row = TAB.split(s);
                                    return new Tuple2<>(
                                            Integer.valueOf(row[IDX_LOCATION]),
                                            Integer.valueOf(row[IDX_USER_ID])
                                    );
                                }
                        )
                        .groupByKey()
                        .mapValues(mapToHashSet);

        final JavaPairRDD<Integer, HashSet<Integer>> friends =
                edges
                        .mapToPair(s -> {
                                    final String[] row = TAB.split(s);
                                    return new Tuple2<>(
                                            Integer.valueOf(row[IDX_USER_1]),
                                            Integer.valueOf(row[IDX_USER_2])
                                    );
                                }
                        )
                        .groupByKey()
                        .mapValues(mapToHashSet);

        final Map<Integer, HashSet<Integer>> friendList = friends.collectAsMap();

        final List<Tuple2<Integer, Integer>> output = locationAndUsers.mapToPair((v) -> {
            final Integer LOC_ID = v._1;
            final HashSet<Integer> checks = v._2;
            final HashSet<Integer> hasTraversal = new HashSet<>();
            final int maxLen = checks.parallelStream()
                    .mapToInt(people -> {
                        if(hasTraversal.contains(people)) return 0;
                        final HashSet<Integer> triedSet = new HashSet<>();
                        final Queue<Integer> queue = new LinkedList<>();
                        triedSet.add(people);
                        queue.add(people);
                        Integer f = null;
                        while ((f = queue.poll()) != null) {
                            if (friendList.containsKey((f))) {
                                friendList.get(f).stream() // f's friends
                                        .filter(i -> checks.contains(i) && !triedSet.contains(i))
                                        .forEach(i -> {
                                            triedSet.add(i);
                                            queue.add(i);
                                        });
                            }
                        }

                        hasTraversal.addAll(triedSet);
                        return triedSet.size();
                    }).max().orElse(0);

            return new Tuple2<>(LOC_ID, maxLen);
        }).mapToPair(Tuple2::swap).sortByKey(false).mapToPair(Tuple2::swap).collect();


        logger.info("Task4 Finished");
        logger.info("Writing result");

        try (PrintWriter printWriter = getLogWriter("Task4", args.length > 2 ? args[2] : null)) {
            printWriter.println("Lists the locations with the largest \"check-in community\"");
            printWriter.println("location_id, largest_community_size");
            for (Tuple2<?, ?> tuple : output) {
                printWriter.println(tuple._1() + ", " + tuple._2());
            }
            printWriter.flush();
        } catch (IOException e) {
            logger.error("Failed to write result", e);
        }
        logger.info("Result written.");

        spark.stop();

    }

    private static Integer getIntersectionSize(
            final Set<Integer> set1, final Set<Integer> set2
    ) {
        if (set1.size() < set2.size()) {
            return set1.parallelStream().mapToInt(x -> set2.contains(x) ? 1 : 0).sum();
        } else {
            return set2.parallelStream().mapToInt(x -> set1.contains(x) ? 1 : 0).sum();
        }

    }
}

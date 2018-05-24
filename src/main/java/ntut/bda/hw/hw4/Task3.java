package ntut.bda.hw.hw4;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.function.Function;

public class Task3 extends Task {
    private static final Logger logger = LogManager.getLogger(Task3.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: Task3 <totalCheckins> [<output dir>]");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("BDA_HW4_TASK3")
                .getOrCreate();

        logger.info("Task3 Running");
        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        List<Tuple2<Integer, Integer>> output =
                lines
                        .map(s ->
                                Integer.valueOf(
                                        TAB.split(s)[IDX_CHK_TIME].substring(11, 13)
                                )
                        )
                        .mapToPair(s -> new Tuple2<>(s, 1))
                        .reduceByKey((i1, i2) -> i1 + i2)
                        .mapToPair(Tuple2::swap)
                        .sortByKey(false)
                        .mapToPair(Tuple2::swap)
                        .collect();


        logger.info("Task3 Finished");
        logger.info("Writing result");

        final Function<Integer, String> mapHour = (h) -> String.format("[%02d:00, %02d:00)", h, h + 1);

        try (PrintWriter printWriter = getLogWriter("Task3", args.length > 1 ? args[1] : null)) {
            printWriter.println("Lists the most popular time for check-ins");
            printWriter.println("hour, freq");
            for (Tuple2<Integer, Integer> tuple : output) {
                printWriter.println(mapHour.apply(tuple._1()) + ", " + tuple._2());
            }

            printWriter.flush();
        } catch (IOException e) {
            logger.error("Failed to write result", e);
        }
        logger.info("Result written.");

        spark.stop();

    }
}

package ntut.bda.hw.hw4;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

public class Task1 extends Task {
    private static final Logger logger = LogManager.getLogger(Task1.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: Task1 <totalCheckins> [<output dir>]");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("BDA_HW4_TASK1")
                .getOrCreate();

        logger.info("Task1 Running");
        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        List<Tuple2<Integer, Integer>> output =
                lines
                        .map(s -> Integer.valueOf(TAB.split(s)[IDX_LOCATION]))
                        .mapToPair(s -> new Tuple2<>(s, 1))
                        .reduceByKey((i1, i2) -> i1 + i2)
                        .mapToPair(Tuple2::swap)
                        .sortByKey(false)
                        .mapToPair(Tuple2::swap)
                        .collect();


        logger.info("Task1 Finished");
        logger.info("Writing result");

        try (PrintWriter printWriter = getLogWriter("Task1", args.length > 1 ? args[1] : null)) {
            printWriter.println("Lists the top checked-in locations");
            printWriter.println("location_id, freq");
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
}

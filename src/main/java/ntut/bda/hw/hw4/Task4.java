package ntut.bda.hw.hw4;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;

public class Task4 extends Task {
    private static final Logger logger = LogManager.getLogger(Task4.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: Task4 <totalCheckins> <edges>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("BDA_HW4_TASK4")
                .getOrCreate();

        logger.info("Task4 Running");
        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();


        logger.info("Task4 Finished");
        logger.info("Writing result");

        try (PrintWriter printWriter = getLogWriter("Task4")) {
            printWriter.println("Lists the locations with the largest “check-in community”");
            printWriter.println("location_id, largest_community_size");
            /*for (Tuple2<?, ?> tuple : output) {
                printWriter.println(tuple._1() + ", " + tuple._2());
            }
            */
            printWriter.flush();
        } catch (IOException e) {
            logger.error("Failed to write result", e);
        }
        logger.info("Result written.");

        spark.stop();

    }
}

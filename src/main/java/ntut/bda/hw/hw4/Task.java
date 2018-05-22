package ntut.bda.hw.hw4;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

public abstract class Task {
    private static final Logger logger = LogManager.getLogger(Task1.class);

    protected static final Pattern TAB = Pattern.compile("\t");
    protected static final int IDX_USER_ID = 0;
    protected static final int IDX_CHK_TIME = 1;
    protected static final int IDX_LAT = 2;
    protected static final int IDX_LON = 3;
    protected static final int IDX_LOCATION = 4;

    protected static PrintWriter getLogWriter(String name) throws IOException {
        File tmpFile = new File(System.getProperty("java.io.tmpdir"), name + ".log");
        logger.info("Log write to: " + tmpFile.getAbsolutePath());

        return new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tmpFile), StandardCharsets.UTF_8)));
    }
}

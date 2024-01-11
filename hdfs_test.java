import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.commons.cli.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;




public class hdfs_test {
    private static final String HADOOP_VERSION = "3.2.2";
    private static final String CORE_SITE_PATH_STR = "/users/jason92/local/hadoop-" + HADOOP_VERSION + "/etc/hadoop/core-site.xml";
    private static final String HDFS_SITE_PATH_STR = "/users/jason92/local/hadoop-" + HADOOP_VERSION + "/etc/hadoop/hdfs-site.xml";
    private static final String YARN_SITE_PATH_STR = "/users/jason92/local/hadoop-" + HADOOP_VERSION + "/etc/hadoop/yarn-site.xml";
    private static final Configuration CONF = new Configuration();
    private static final String HDFS_DIR = "/user/jason92/";

    private static class ThreadTask implements Runnable {
        private FileSystem fs;
        private Path dir;
        private int threadID;
        private int totalThread;
        private int numOfActions;


        public ThreadTask (FileSystem fs, String dir, int threadID, int totalThread, int numOfActions) {
            this.fs = fs;
            this.dir = new Path(dir);
            this.threadID = threadID;
            this.totalThread = totalThread;
            this.numOfActions = numOfActions;
        }

        @Override
        public void run() {
            try {
                // ThreadLocalRandom tlr = ThreadLocalRandom.current();
                for (int i = 0; i < this.numOfActions; i++) {
                    int fileID = i * this.totalThread + this.threadID;
                    Path path = new Path(this.dir, "file-" + fileID);
                    String content = String.valueOf(fileID);
                    OutputStream os = this.fs.create(path, true);
                    os.write(content.getBytes());
                    System.out.format("write %s to file %d%n", content, fileID);
                    os.close();
                    for (int j = i; j >=0 && j > i - 5; j--) {
                        fileID = j * this.totalThread + this.threadID;
                        path = new Path(this.dir, "file-" + fileID);
                        InputStream is = fs.open(path);
                        BufferedReader br = new BufferedReader(new InputStreamReader(is));
                        content = br.readLine();
                        System.out.format("read %s from file %d%n", content, fileID);
                        br.close();
                        is.close();
                    }
                }

            } catch (Exception e) {
                System.err.format("Exception in run(): %s", e.getMessage());
            }
        }
    };

    public static void main(String[] args) {
        CONF.addResource(new Path(CORE_SITE_PATH_STR));
        CONF.addResource(new Path(HDFS_SITE_PATH_STR));
        CONF.addResource(new Path(YARN_SITE_PATH_STR));

        int threadPoolSize = Integer.parseInt(args[0]);
        int threadSize = Integer.parseInt(args[1]);
        int numOfActions = Integer.parseInt(args[2]);

        try {
            FileSystem fs = FileSystem.get(CONF);

            ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);

            for (int i = 0; i < threadSize; i++) {
                ThreadTask task = new ThreadTask(fs, HDFS_DIR, i, threadSize, numOfActions);
                executor.execute(task);
            }

            executor.shutdown();

        } catch (Exception e) {
            System.err.format("Exception in main(): %s", e.getMessage());
        }

        
    }
}

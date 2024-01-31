import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.commons.cli.*;
import org.apache.hadoop.security.UserGroupInformation;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.Scanner;
import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;




public class hdfs_test {
    private static final String HADOOP_VERSION = "3.2.2";
    private static final String CORE_SITE_PATH_STR = "/users/jason92/hadoop-" + HADOOP_VERSION + "/etc/hadoop/core-site.xml";
    private static final String HDFS_SITE_PATH_STR = "/users/jason92/hadoop-" + HADOOP_VERSION + "/etc/hadoop/hdfs-site.xml";
    private static final String YARN_SITE_PATH_STR = "/users/jason92/hadoop-" + HADOOP_VERSION + "/etc/hadoop/yarn-site.xml";
    private static final Configuration CONF = new Configuration();
    // private static final String HDFS_DIR = "/user/jason92/";

    private static class ThreadTask implements Runnable {
        private FileSystem fs;
        private Path dir;
        private int threadID;
        private int totalThread;
        private int numOfActions;
        private UserGroupInformation ugi;



        public ThreadTask (UserGroupInformation ugi, FileSystem fs, String dir, int threadID, int totalThread, int numOfActions) {
            this.fs = fs;
            this.dir = new Path(dir);
            this.threadID = threadID;
            this.totalThread = totalThread;
            this.numOfActions = numOfActions;
            this.ugi = ugi;
        }

        @Override
        public void run() {

            try {
                this.ugi.doAs((java.security.PrivilegedExceptionAction<Void>) () -> {
                    Scanner scanner = new Scanner(System.in);
                    for (int i = 0; i < this.numOfActions; i++) {
                        int fileID = i * this.totalThread + this.threadID;
                        Path path = new Path(this.dir, "file-" + fileID);
                        String content = String.valueOf(fileID);
                        OutputStream os = this.fs.create(path, true);
                        os.write(content.getBytes());
                        System.out.format("write %s to file %d%n", content, fileID);
                        // System.out.println("wait user input");
                        // String str = scanner.nextLine();
                        // System.out.println("user input is: " + str);
                        Thread.sleep(20000);
                        System.out.format("write %s to file %d done after 20 Seconds%n", content, fileID);
                    }
                    return null;
                });
            } catch (Exception e) {
                System.err.format("Exception in run(): %s", e.getMessage());
            }
        }
    };

    public static void main(String[] args) {
        CONF.addResource(new Path(CORE_SITE_PATH_STR));
        CONF.addResource(new Path(HDFS_SITE_PATH_STR));
        CONF.addResource(new Path(YARN_SITE_PATH_STR));

        String username = args[0];
        int threadPoolSize = Integer.parseInt(args[1]);
        int threadSize = Integer.parseInt(args[2]);
        int numOfActions = Integer.parseInt(args[3]);
        String hdfs_dir = "/user/"+username;
        
        try {
            // UserGroupInformation ugi = UserGroupInformation.createProxyUser(username, UserGroupInformation.getCurrentUser());

            FileSystem fs = FileSystem.get(CONF);

            ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);

            for (int i = 0; i < threadSize; i++) {
                // ThreadTask task = new ThreadTask(ugi, fs, hdfs_dir, i, threadSize, numOfActions);
                // executor.execute(task);
                int finalI = i;
                executor.submit(() -> {
                    try {
                        Path path = new Path(hdfs_dir, "file-" + finalI);
                        OutputStream os = fs.create(path, true);
                        os.write("test".getBytes());
                        System.out.format("file %d start%n", finalI);
                        Thread.sleep(15000);
                        System.out.format("file %d done%n", finalI);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }

            executor.shutdown();

        } catch (Exception e) {
            System.err.format("Exception in main(): %s", e.getMessage());
        }

        
    }
}

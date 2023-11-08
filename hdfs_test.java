import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.commons.cli.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Callable;
import java.io.*;

public class HdfsTest {

    private static final int NUMBER_OF_CORES = 30;
    private static final String CORE_SITE_PATH_STR = "/users/jason92/local/hadoop-3.3.6/etc/hadoop/core-site.xml";
    private static final String HDFS_SITE_PATH_STR = "/users/jason92/local/hadoop-3.3.6/etc/hadoop/hdfs-site.xml";
    private static final String YARN_SITE_PATH_STR = "/users/jason92/local/hadoop-3.3.6/etc/hadoop/yarn-site.xml";
    private static final Configuration CONF = new Configuration();
    private static final String LOCAL_DIR = "/users/jason92/projects/hdfs/xs-files/";
    private static final String HDFS_DIR = "/user/jason92/";
    private static ExecutorService executorService = Executors.newFixedThreadPool(NUMBER_OF_CORES);

    static {
        CONF.addResource(new Path(CORE_SITE_PATH_STR));
        CONF.addResource(new Path(HDFS_SITE_PATH_STR));
        CONF.addResource(new Path(YARN_SITE_PATH_STR));
    }

    public static void main(String[] args) {
        if (args.length != 5) {
            System.err.println("Please provide 5 arguements");
            System.exit(1);
        }

        int localFileStartID = Integer.ParseInt(args[0]);
        int localFileEndID = Integer.ParseInt(args[1]);
        int uploadBatchSize = Integer.ParseInt(args[2]);
        int hdfsDirStartID = Integer.ParseInt(args[3]);
        int hdfsDirEndID = Integer.ParseInt(args[4]);


        System.out.println("Starting HDFS upload process...");
        uploadFilesToHdfs(localFileStartID, localFileEndID, uploadBatchSize, hdfsDirStartID, hdfsDirEndID);
        System.out.println("HDFS upload process completed.");
    }

    private static void uploadFilesToHdfs(int localFileStartID, int localFileEndID, int uploadBatchSize, int hdfsDirStartID, int hdfsDirEndID) {
        List<String> localFilePaths = generateLocalFilePaths(localFileStartID, localFileEndID);
        List<Path> hdfsDirPaths = generateHdfsDirPaths(hdfsDirStartID, hdfsDirEndID);
        List<Future<String>> futures = new ArrayList<>();

        FileSystem fs = FileSystem.get(CONF);

        for (Path hdfsDirPath : hdfsDirPaths) {

            for (int i = 0; i < localFilePaths.size(); i += uploadBatchSize) {
                int realBatchSize = uploadBatchSize;
                if (i + realBatchSize > localFilePaths.size()) {
                    realBatchSize = localFilePaths.size() - i;
                }
                Path[] srcPaths = localFilePaths.subList(i, i + realBatchSize).toArray();
                Future<String> future = executorService.submit(new Callable<String>() {
                    public String call() throws Exception {
                        fs.copyFromLocalFile(false, true, srcPaths, hdfsDirPath);
                        return "upload files [" + i + " - " + (i + realBatchSize) + "] to " + hdfsDirPath.getName() + " finished";
                    }
                });
                futures.add(future);
            }
        }

        // Collect the results of the futures
        for (Future<String> future : futures) {
            try {
                // Block and wait for the future to complete
                System.out.println(future.get());
            } catch (Exception e) {
                System.err.println("Exception occurred during file upload:");
                e.printStackTrace();
            }
        }

        fs.close();

        // Properly shut down the executor service
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static List<String> generateLocalFilePaths(int start, int end) {
        List<String> localFilePaths = new ArrayList<>();
        for (int i = start; i < end; i++) {
            String localPath = LOCAL_DIR + "file-" + i;
            localFilePaths.add(localPath);
        }
        return localFilePaths;
    }

    private static List<Path> generateHdfsDirPaths(int start, int end) {
        List<Path> hdfsDirPaths = new ArrayList<>();
        for (int i = start; i < end; i++) {
            String hdfsDirPath = HDFS_DIR + "xs-files-dir-" + i + "/";
            hdfsDirPaths.add(new Path(hdfsDirPath));
        }
        return hdfsDirPaths;
    }
}
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

public class hdfs_test {
    private static final String CORE_SITE_PATH_STR = "/users/jason92/local/hadoop-3.3.6/etc/hadoop/core-site.xml";
    private static final String HDFS_SITE_PATH_STR = "/users/jason92/local/hadoop-3.3.6/etc/hadoop/hdfs-site.xml";
    private static final String YARN_SITE_PATH_STR = "/users/jason92/local/hadoop-3.3.6/etc/hadoop/yarn-site.xml";
    private static final Configuration CONF = new Configuration();
    private static final String LOCAL_DIR = "/users/jason92/projects/hdfs/xs-files/";
    private static final String HDFS_DIR = "/user/jason92/";

    public static void main(String[] args) {
        if (args.length != 5) {
            System.err.println("Please provide 5 arguments");
            System.exit(1);
        }

        int hdfsFileStartID = Integer.parseInt(args[0]);
        int hdfsFileEndID = Integer.parseInt(args[1]);
        int hdfsDirStartID = Integer.parseInt(args[2]);
        int hdfsDirEndID = Integer.parseInt(args[3]);
        int numberOfCores = Integer.parseInt(args[4]);

        CONF.addResource(new Path(CORE_SITE_PATH_STR));
        CONF.addResource(new Path(HDFS_SITE_PATH_STR));
        CONF.addResource(new Path(YARN_SITE_PATH_STR));

        System.out.println("Starting HDFS upload process...");
        try {
            uploadFilesToHdfs(hdfsFileStartID, hdfsFileEndID, hdfsDirStartID, hdfsDirEndID, numberOfCores);
            System.out.println("HDFS upload process completed.");
        } catch (Exception e) {
            System.err.println("Error during file upload: " + e.getMessage());
        }
    }

    private static void uploadFilesToHdfs(int hdfsFileStartID, int hdfsFileEndID, int hdfsDirStartID, int hdfsDirEndID, int numberOfCores) {
        List<Path> hdfsDirPaths = generateHdfsDirPaths(hdfsDirStartID, hdfsDirEndID);
            
        try {
            FileSystem fs = FileSystem.get(CONF);
            ExecutorService executorService = Executors.newFixedThreadPool(numberOfCores);

            for (Path hdfsDirPath : hdfsDirPaths) {
                if (!fs.exists(hdfsDirPath)) {
                    fs.mkdirs(hdfsDirPath);
                }
                List<Future<String>> futures = new ArrayList<>();
                for (int i = hdfsFileStartID; i < hdfsFileEndID; i++) {
                    Path hdfsFilePath = new Path(hdfsDirPath, "file-" + i);
                    Future<String> future = executorService.submit(new Callable<String>() {
                        public String call() throws Exception {
                            boolean res = fs.createNewFile(hdfsFilePath);
                            if (res) {
                                return "create new file: " + hdfsFilePath + " succeed";
                            }
                            return "file: " + hdfsFilePath + " already exist";

                        }
                    });
                    futures.add(future);
                }
                // Collect the results of the futures
                for (int i = 0; i < futures.size(); i++) {
                    try {
                        Future<String> future = futures.get(i);
                        String ret = future.get();
                        if (i % 1000 == 0) {
                            System.out.println(ret);
                        }
                    } catch (Exception e) {
                        System.err.println("Exception occurred during file upload:");
                        e.printStackTrace();
                    }
                }
            }

            fs.close();
            executorService.shutdown();

        } catch (Exception e) {
            System.err.println("Error during file upload: " + e.getMessage());
        }
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

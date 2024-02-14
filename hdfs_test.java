import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.commons.cli.*;
import org.apache.hadoop.security.UserGroupInformation;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;




public class hdfs_test {
    private static final Configuration CONF = new Configuration();

    public static void main(String[] args) {
        String hadoop_version = args[0];
        String core_site_path = "/users/jason92/hadoop-" + hadoop_version + "/etc/hadoop/core-site.xml";
        String hdfs_site_path = "/users/jason92/hadoop-" + hadoop_version + "/etc/hadoop/hdfs-site.xml";
        String yarn_site_path = "/users/jason92/hadoop-" + hadoop_version + "/etc/hadoop/yarn-site.xml";
        String username = args[1];
	    int dataSize = Integer.parseInt(args[2]);
        int threadSize = Integer.parseInt(args[3]);
        String hdfs_dir = "/user/"+username;

        CONF.addResource(new Path(core_site_path));
        CONF.addResource(new Path(hdfs_site_path));
        CONF.addResource(new Path(yarn_site_path));
        
        
        try {
            ScheduledExecutorService executor = Executors.newScheduledThreadPool(threadSize);
            FileSystem fs = FileSystem.get(CONF);
            byte[] data = new byte[dataSize];

            for (int i = 0; i < threadSize; i++) {
                Path path = new Path(hdfs_dir, "file-" + i);
                ScheduledTask task = new ScheduledTask(fs, data, path);
                executor.schedule(task, 2, TimeUnit.SECONDS);
            }

            executor.shutdown();
            executor.awaitTermination(200, TimeUnit.SECONDS);
            executor.shutdownNow();
            System.out.println("executor shutdown");
            fs.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

        
    }

    public static class ScheduledTask implements Runnable {
        static AtomicInteger GlobalID = new AtomicInteger(0);
        static AtomicInteger counter = new AtomicInteger(0);
        int id;
        int waitMS = 0;
        FileSystem fs;
        byte[] data;
        Path path;
        OutputStream os;
        DateTimeFormatter formatter;
        public ScheduledTask(FileSystem fs, byte[] data, Path path) {
            try{
                id = GlobalID.getAndIncrement();
                this.fs = fs;
                this.data = data;
                this.path = path;
                os = fs.create(path, true);
                formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            try {
                long start = System.nanoTime();
                System.out.format("%d: started%n", id);
                if (counter.incrementAndGet() == GlobalID.get()) System.out.println("exit");
                os.write(data);
                long end = System.nanoTime();

                LocalTime startTime = LocalTime.ofNanoOfDay(TimeUnit.NANOSECONDS.toNanos(start));
                String formattedStartTime = startTime.format(formatter);
                double elapsedTime = (end - start) / 1000000.0;
                System.out.format("%d: start at [%s], write %d bytes, took %f ms %n", id, formattedStartTime, data.length, elapsedTime);
                os.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}



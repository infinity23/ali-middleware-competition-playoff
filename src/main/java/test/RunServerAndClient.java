package test;

import com.alibaba.middleware.race.sync.Client;
import com.alibaba.middleware.race.sync.Server;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RunServerAndClient {

    private static String schema = "middleware5";
    private static String table = "student";
    private static String start = "100";
    private static String end = "200";

    private static ExecutorService executorService = Executors.newCachedThreadPool();

    public static void main(String[] args) throws Exception {
        Server.main(new String[]{schema,table,start,end});
        Client.main(new String[]{"127.0.0.1"});
    }
}

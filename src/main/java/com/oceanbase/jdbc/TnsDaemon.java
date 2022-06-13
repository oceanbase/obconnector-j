package com.oceanbase.jdbc;

import java.io.*;
import java.nio.file.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.oceanbase.jdbc.internal.failover.impl.LoadBalanceInfo;
import com.oceanbase.jdbc.internal.failover.utils.ConfigParser;

public class TnsDaemon extends Thread {
    private Map<String, LoadBalanceInfo> propertyMap = new ConcurrentHashMap<>();
    private static  String configFileName = "tnsnames.ob";

    private void fullFillMap(String tnsPath) throws IOException {

        String filePath = tnsPath + "/" + configFileName;
        File file = new File(filePath);
        Reader reader = new InputStreamReader(new FileInputStream(file));
        HashMap<String, LoadBalanceInfo> tmp = ConfigParser.getLoadBalanceInfosFromReader(reader);
        propertyMap.clear();
        for (String key : tmp.keySet()) {
            propertyMap.put(key, tmp.get(key));
        }
    }

    @Override
    public void run() {
        try {
            ConfigParser.TnsFileInfo tnsFileInfo = ConfigParser.getTnsFilePath();
            configFileName = tnsFileInfo.name;
            fullFillMap(tnsFileInfo.path);
            WatchService watchService = FileSystems.getDefault().newWatchService();
            File file1 = new File(tnsFileInfo.path);
            file1.toPath().register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
            while (true) {
                WatchKey key = watchService.take();
                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent<Path> ev = (WatchEvent<Path>) event;
                    Path filename = ev.context();
                    if (filename.toString().equals(configFileName)) {
                        fullFillMap(tnsFileInfo.path);
                    }
                }
                boolean vaild = key.reset();
                if (!vaild) {
                    break;
                }
            }
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    public LoadBalanceInfo getTnsProperties(String key) {
        return propertyMap.get(key);
    }

}

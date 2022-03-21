package com.oceanbase.jdbc;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.oceanbase.jdbc.internal.util.Utils;

public class TnsDaemon extends Thread {
    private Map<String, Utils.LoadBlanceInfo> propertyMap = new ConcurrentHashMap<>();
    private static final String configFileName = "tnsnames.ob";

    private void fullFillMap(String tnsPath) throws IOException {
        HashMap<String, Utils.LoadBlanceInfo> tmp = Utils.getPropertiesFromTNSNames(tnsPath + "/" + configFileName);
        for (String key : tmp.keySet()) {
            propertyMap.put(key, tmp.get(key));
        }
    }

    @Override
    public void run() {
        try {
            String tnsPath = System.getProperty("oceanbase.tns_admin");
            String tnsEnv = System.getenv("OCEANBASE_TNS_ADMIN");
            if (tnsEnv == null && tnsPath == null) {
                return;
            }
            if (tnsPath == null) {
                tnsPath = tnsEnv;
            }
            fullFillMap(tnsPath);
            WatchService watchService = FileSystems.getDefault().newWatchService();
            File file1 = new File(tnsPath);
            file1.toPath().register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
            while (true) {
                WatchKey key = watchService.take();
                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent<Path> ev = (WatchEvent<Path>) event;
                    Path filename = ev.context();
                    if (filename.toString().equals(configFileName)) {
                        fullFillMap(tnsPath);
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

    public Utils.LoadBlanceInfo getTnsProperties(String key) {
        return propertyMap.get(key);
    }

}

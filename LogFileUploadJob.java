package com.virtualoffice.client.datacapture.jobs;

import com.virtualoffice.client.conf.UserPreferences;
import com.virtualoffice.client.conf.UserPreferencesKeys;
import com.virtualoffice.client.datacapture.Constants;
import com.virtualoffice.client.net.PortalClient;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class LogFileUploadJob extends AbstractJob {

    public static final LogFileUploadJob INSTANCE = new LogFileUploadJob();

    private static final Logger LOGGER =
            Logger.getLogger(LogFileUploadJob.class);

    private static final long MILLISECONDS_OF_THIRTY_MINUTES = 30 * 60 * 1000L;

    private boolean isUploading;

    private LogFileUploadJob() {
        // hides the constructor of the singleton class
    }

    @Override
    public void stop() {
        super.stop();

        // upload the log files in stopping
        super.scheduler.execute(this::doUpload);
    }

    @Override
    public void setFrequency(int frequency) {
        // do nothing here, override the super class function
    }

    @Override
    public void run() {
        doUpload();
    }

    private void doUpload() {
        if (!UserPreferences.INSTANCE.getAsBoolean(UserPreferencesKeys.PREF_UPLOAD_LOGS, false)) {
            LOGGER.info("Logs upload disabled");
            return;
        }
        upload("deskapp.log", PortalClient.URL_LOG_FILE);
        upload("root.log", PortalClient.URL_ERROR_LOG_FILE);
    }

    private void upload(String logFileName, String url) {
        if (isUploading) {
            return;
        }

        isUploading = true;
        File logFileCopy = null;
        try {
            String[] arr = {
                    Constants.APP_DIR_PATH,
                    Constants.LOG_DIR_NAME,
                    logFileName
            };
            String path = StringUtils.join(arr, File.separator);
            File logFile = new File(path);

            if (logFile.exists()) {
                String pathCopy = path + ".bak";
                logFileCopy = new File(pathCopy);
                FileUtils.copyFile(logFile, logFileCopy);
                Map<String, File> map = new HashMap<>();
                map.put("log_file", logFileCopy);
                PortalClient.getInstance().upload(url, map);
            }
        } catch (Exception e) {
            LOGGER.error(e);
        } finally {
            if (logFileCopy != null) {
                try {
                    FileUtils.forceDelete(logFileCopy);
                } catch (IOException e) {
                    LOGGER.error(e);
                }
            }
        }
        isUploading = false;
    }


    @Override
    protected long calcInterval() {
        // upload log files per 30 minutes
        return MILLISECONDS_OF_THIRTY_MINUTES;
    }
}

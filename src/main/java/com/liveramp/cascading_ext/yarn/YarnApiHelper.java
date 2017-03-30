package com.liveramp.cascading_ext.yarn;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Optional;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YarnApiHelper {

  public static final String YARN_STATS_GROUP = "YarnStats";
  public static final String YARN_MEM_SECONDS_COUNTER = "MB_SECONDS";
  public static final String YARN_VCORE_SECONDS_COUNTER = "VCORES_SECONDS";

  private static Logger LOG = LoggerFactory.getLogger(YarnApiHelper.class);

  public static class ApplicationInfo {
    Long mbSeconds;
    Long vcoreSeconds;
    String trackingUrl;

    public ApplicationInfo(Long mbSeconds, Long vcoreSeconds, String trackingUrl) {
      this.mbSeconds = mbSeconds;
      this.vcoreSeconds = vcoreSeconds;
      this.trackingUrl = trackingUrl;
    }

    public Long getMbSeconds() {
      return mbSeconds;
    }

    public Long getVcoreSeconds() {
      return vcoreSeconds;
    }

    public String getTrackingUrl() {
      return trackingUrl;
    }

    @Override
    public String toString() {
      return "ApplicationInfo{" +
          "mbSeconds=" + mbSeconds +
          ", vcoreSeconds=" + vcoreSeconds +
          ", trackingUrl='" + trackingUrl + '\'' +
          '}';
    }
  }

  private static String GETRequest(String urlString) throws IOException {
    URL url = new URL(urlString);
    HttpURLConnection urlConnection = (HttpURLConnection)url.openConnection();
    urlConnection.setConnectTimeout(10000);
    urlConnection.setReadTimeout(10000);
    urlConnection.setRequestMethod("GET");
    return IOUtils.toString(urlConnection.getInputStream());
  }

  public static Optional<ApplicationInfo> getYarnAppInfo(JobConf conf, String appId) {
    String yarnApiAddress = conf.get("yarn.resourcemanager.webapp.address", "");
    if (!yarnApiAddress.isEmpty()) {
      return getYarnAppInfo(yarnApiAddress, appId);
    } else {
      return Optional.empty();
    }
  }

  public static Optional<ApplicationInfo> getYarnAppInfo(String yarnApiAddress, String appId) {
    if (yarnApiAddress != null && !yarnApiAddress.isEmpty()) {
      try {
        String urlString = "http://" + yarnApiAddress + "/ws/v1/cluster/apps/" + appId;
        String jsonResponse = GETRequest(urlString);
        JsonObject parsed = (JsonObject)new JsonParser().parse(jsonResponse);
        JsonObject app = parsed.getAsJsonObject("app");
        ApplicationInfo info = new ApplicationInfo(
            app.get("memorySeconds").getAsLong(),
            app.get("vcoreSeconds").getAsLong(),
            app.get("trackingUrl").getAsString()
        );
        LOG.info("Got YARN info: " + info);
        return Optional.of(info);
      } catch (IOException e) {
        LOG.error("Error getting yarn info:", e);
        return Optional.empty();
      }
    } else {
      LOG.error("YARN api address not set");
      return Optional.empty();
    }
  }

  public static Optional<String> getHistoryURLFromTrackingURL(String trackingUrl) throws IOException {

    URL url = new URL(trackingUrl);
    HttpURLConnection urlConnection = (HttpURLConnection)url.openConnection();
    urlConnection.setInstanceFollowRedirects(false);
    urlConnection.connect();
    int responseCode = urlConnection.getResponseCode();
    if (responseCode == 302) {
      return Optional.of(urlConnection.getHeaderField("Location"));
    }
    return Optional.empty();
  }


}

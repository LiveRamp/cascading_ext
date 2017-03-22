package com.liveramp.cascading_ext.util;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Optional;

public class YarnApiUtil {

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

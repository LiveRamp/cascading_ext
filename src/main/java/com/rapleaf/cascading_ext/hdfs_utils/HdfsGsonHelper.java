package com.rapleaf.cascading_ext.hdfs_utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PushbackInputStream;
import java.io.Reader;
import java.io.Writer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jetbrains.annotations.NotNull;

import com.liveramp.java_support.functional.Fns8;

public class HdfsGsonHelper {

  public static void write(FileSystem fs, Gson gson, Object obj, Path path) throws IOException {
    OutputStream metaStream = createHDFSFile(fs, path);
    OutputStreamWriter writer = new OutputStreamWriter(metaStream);
    writer.write(gson.toJson(obj));
    writer.close();
  }

  //Stream a sequence of key value pairs into a json file. pathToObject allows sending the pairs into a nested object if desired
  //i.e. if pathToObject is ["heading1","heading2"] the created json would look like {"heading1":{"heading2" : {<key1>:<value1>, <key2>:<value2>,... }}}
  public static <T> void writeFromStream(FileSystem fs, Gson gson, Path path, Stream<Pair<String, T>> keysAndValues, Class<T> tClass, String... pathToObject) throws IOException {
    OutputStream metaStream = createHDFSFile(fs, path);
    OutputStreamWriter writer = new OutputStreamWriter(metaStream);
    writeFromStream(writer, gson, keysAndValues, tClass, pathToObject);
  }

  public static <T> void writeFromStream(Writer writer, Gson gson, Stream<Pair<String, T>> keysAndValues, Class<T> tClass, String... pathToObject) throws IOException {
    JsonWriter jsonWriter = new JsonWriter(writer);

    jsonWriter.beginObject();
    for (String name : pathToObject) {
      jsonWriter.name(name);
      jsonWriter.beginObject();
    }

    keysAndValues.forEach(p -> {
      try {
        jsonWriter.name(p.getKey());
        gson.toJson(p.getValue(), tClass, jsonWriter);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    for (String name : pathToObject) {
      jsonWriter.endObject();
    }
    jsonWriter.endObject();
    jsonWriter.close();
  }

  public static <T> HdfsGsonFileWriter<T> openForWrite(FileSystem fs, Gson gson, Path path, Class<T> tClass, String... pathToObject) throws IOException {
    OutputStream metaStream = createHDFSFile(fs, path);
    OutputStreamWriter writer = new OutputStreamWriter(metaStream);
    JsonWriter jsonWriter = new JsonWriter(writer);

    jsonWriter.beginObject();
    for (String name : pathToObject) {
      jsonWriter.name(name);
      jsonWriter.beginObject();
    }

    return new HdfsGsonFileWriter<>(jsonWriter, gson, tClass, pathToObject);
  }


  private static OutputStream createHDFSFile(FileSystem fs, Path path) throws IOException {
    return fs.create(path);
  }

  private static InputStream openStreamToHDFS(FileSystem fs, Path path) throws IOException {
    return decompressStream(fs.open(path));
  }

  //Code mostly taken from https://stackoverflow.com/posts/4818946/revisions
  public static InputStream decompressStream(InputStream input) throws IOException {
    PushbackInputStream pb = new PushbackInputStream(input, 2); //we need a pushbackstream to look ahead
    byte[] signature = new byte[2];
    int len = pb.read(signature); //read the signature
    pb.unread(signature, 0, len); //push back the signature to the stream
    if (len == 2 && signature[0] == (byte)0x1f && signature[1] == (byte)0x8b) { //check if matches standard gzip magic number
      return new GZIPInputStream(pb);
    } else {
      return pb;
    }
  }

  public static <T> T read(FileSystem fs, Gson gson, Path path, Class<T> type) throws IOException {
    return gson.fromJson(IOUtils.toString(openStreamToHDFS(fs, path)), type);
  }

  public static <T> T read(InputStream input, Gson gson, Class<T> type) throws IOException {
    return gson.fromJson(IOUtils.toString(input), type);
  }

  //Read the keys and values of a json object located in the file at the supplied path as a stream of String, T pairs
  public static <T> Stream<Pair<String, T>> streamObject(FileSystem fs, Gson gson, Path path, Class<T> type, String... pathToObject) throws IOException {
    InputStreamReader reader = new InputStreamReader(openStreamToHDFS(fs, path));
    return streamObject(gson, type, reader, pathToObject);
  }

  //Read the keys and values of a json object located in the file at the supplied path,
  // passing the json reader to the supplied function at the start of each k/v pair
  // it is the function's responsibility to leave the reader at the start of the next k/v pair
  public static <T> Stream<T> streamObject(FileSystem fs, Path path, Function<JsonReader, T> fn, String... pathToObject) throws IOException {
    InputStreamReader reader = new InputStreamReader(openStreamToHDFS(fs, path));
    return streamObject(reader, fn, pathToObject);
  }

  @NotNull
  public static <T> Stream<Pair<String, T>> streamObject(Gson gson, Class<T> type, Reader reader, String... pathToObject) throws IOException {
    return streamObject(reader, (jsonReader -> {
      try {
        String name = jsonReader.nextName();
        return Pair.of(name, gson.fromJson(jsonReader, type));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }), pathToObject);
  }

  @NotNull
  public static <T> Stream<T> streamObject(Reader reader, Function<JsonReader, T> getValue, String... pathToObject) throws IOException {
    JsonReader jsonReader = new JsonReader(reader);
    travelPath(jsonReader, pathToObject);
    jsonReader.beginObject();
    Iterator<T> resultItr = new Iterator<T>() {

      @Override
      public boolean hasNext() {
        try {
          return jsonReader.hasNext();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public T next() {
        return getValue.apply(jsonReader);
      }
    };

    return Fns8.asStream(resultItr);
  }

  private static void travelPath(JsonReader jsonReader, String[] pathToObject) throws IOException {
    UnmodifiableIterator<String> pathItr = Iterators.forArray(pathToObject);
    while (pathItr.hasNext()) {
      jsonReader.beginObject();
      String currentName = pathItr.next();
      while (jsonReader.hasNext() && (jsonReader.peek() != JsonToken.NAME || !jsonReader.nextName().equals(currentName))) {
        jsonReader.skipValue();
      }
    }
  }

  public static class HdfsGsonFileWriter<T> implements BiConsumer<String, T>, AutoCloseable {
    private final JsonWriter jsonWriter;
    private final Gson gson;
    private final Class<T> tClass;
    private final String[] pathToObject;

    public HdfsGsonFileWriter(JsonWriter jsonWriter, Gson gson, Class<T> tClass, String... pathToObject) {
      this.jsonWriter = jsonWriter;
      this.gson = gson;
      this.tClass = tClass;
      this.pathToObject = pathToObject;
    }

    @Override
    public void accept(String key, T value) {
      try {
        jsonWriter.name(key);
        gson.toJson(value, tClass, jsonWriter);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() throws Exception {
      for (String name : pathToObject) {
        jsonWriter.endObject();
      }
      jsonWriter.endObject();
      jsonWriter.close();
    }
  }
}

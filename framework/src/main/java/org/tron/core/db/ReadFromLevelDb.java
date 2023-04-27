package org.tron.core.db;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.tron.common.storage.leveldb.LevelDbDataSourceImpl;
import org.tron.core.config.args.Args;
import org.xerial.snappy.Snappy;

/**
 * @Author: jiangyuanshu
 * @Date: 2023/4/25
 */
@Slf4j(topic = "DB")
public class ReadFromLevelDb {

  String dbDir = "output-directory/"; //jar包当前平级目录


  public <T> Consumer<T> withCounter(BiConsumer<Integer, T> consumer) {
    AtomicInteger counter = new AtomicInteger(0);
    return item -> consumer.accept(counter.getAndIncrement(), item);
  }

  public void freezeTable(String tableName) {

    logger.info("Start initDB ");

    LevelDbDataSourceImpl dbSource = new LevelDbDataSourceImpl(dbDir, tableName);
    dbSource.initDB();

    logger.info("Start fetch data " + dbDir);

    AtomicInteger fileCount = new AtomicInteger(0);
    AtomicInteger fileSize = new AtomicInteger(0);
    Map<Integer, FileOutputStream> count2FileStream = new HashMap<>();

    int batch = 1;
    int i = 0;

    ByteArrayOutputStream batchData = new ByteArrayOutputStream();
    FileOutputStream fw = null;
    for (Entry<byte[], byte[]> v : dbSource) {
      i += 1;
      try {

        //每个 file 只用打开一次
        fw = count2FileStream.get(fileCount.get());
        if (fw == null) {
          File file = new File(
              String.format("ancient/%s.%04d.cdat", tableName, fileCount.get()));
          logger.info(file.getAbsolutePath());
          if (!file.exists()) {
            file.createNewFile();
          }
          fw = new FileOutputStream(file, true);
          count2FileStream.put(fileCount.get(), fw);
        }

        batchData.write(v.getValue());
        if (i >= batch) {
          byte[] compressData = Snappy.compress(batchData.toByteArray());
          fw.write(compressData);
          batchData.reset();

          fileSize.getAndAdd(compressData.length);
          if (fileSize.get() >= 200 * 1000 * 1000) {
            fw.flush();
            fileCount.incrementAndGet();
            fileSize.set(0);
          }
          i = 0;
        }

        if (i % 1000 == 0) {
          //logger.info("write block {}", i);
        }

      } catch (IOException e) {
        logger.error("", e);
      }
    }

    if (fw != null) {
      try {
        fw.flush();
        fw.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    dbSource.closeDB();
  }

  public static void main(String[] args) {

    Args.setParam(args, "config-nile.conf");
    ReadFromLevelDb readFromLevelDb = new ReadFromLevelDb();
    readFromLevelDb.freezeTable("block");

  }
}

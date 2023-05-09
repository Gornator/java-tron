package org.tron.plugins.ethfreeze;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.tron.common.utils.ByteUtil;
import org.tron.plugins.utils.DBUtils;
import org.xerial.snappy.Snappy;
import picocli.CommandLine;

@Slf4j(topic = "DB")
@CommandLine.Command(name = "freezer", description = "Convert leveldb to ancient store.")
public class TronFreezer implements Callable<Integer> {

  String dbDir = "output-directory"; //jar包当前平级目录
  int maxFileSize = 2 * 1000 * 1000 * 1000;
  long maxBlockNum = 0;
  long minBlockNum = Long.MAX_VALUE;

  private void createDir() {
    File folder = new File("ancient");
    if (!folder.exists() && !folder.isDirectory()) {
      folder.mkdir();
    }
  }

  public void writeIndexAndData(String tableName) throws IOException {
    Path path = Paths.get(dbDir, "database", tableName);

    AtomicInteger fileCount = new AtomicInteger(0);
    AtomicInteger singleFileSize = new AtomicInteger(0);
    Map<Integer, FileOutputStream> count2FileStream = new HashMap<>();

    FileOutputStream dataWrite = null;
    File indexFile = new File(String.format("ancient/%s.cidx", tableName));
    FileOutputStream indexWrite = new FileOutputStream(indexFile, false);

    DB db = DBUtils.newLevelDb(path);
    DBIterator dbIterator = db.iterator();
    dbIterator.seekToFirst();

    long blockNum = 0;
    //get block from 0 ~ n
    while (dbIterator.hasNext()) {

      Entry<byte[], byte[]> v = dbIterator.next();
      byte[] blockByte = new byte[8];
      System.arraycopy(v.getKey(), 0, blockByte, 0, 8);
      blockNum = ByteUtil.byteArrayToLong(blockByte);
      if (blockNum == 0) {
        continue;
      }
      minBlockNum = Math.min(minBlockNum, blockNum);
      maxBlockNum = Math.max(maxBlockNum, blockNum);

      byte[] compressData = Snappy.compress(v.getValue());

      if (singleFileSize.get() + compressData.length >= maxFileSize) {
        if (dataWrite != null) {
          dataWrite.flush();
          dataWrite.close();
        }
        fileCount.incrementAndGet();
        singleFileSize.set(0);
      }

      //file handler
      dataWrite = count2FileStream.get(fileCount.get());
      if (dataWrite == null) {
        File file = new File(String.format("ancient/%s.%04d.cdat", tableName, fileCount.get()));
        dataWrite = new FileOutputStream(file, false);
        count2FileStream.put(fileCount.get(), dataWrite);
      }
      dataWrite.write(compressData);

      //write index
      byte[] fileNum = ByteUtil.intToBytes(fileCount.get()); // 4 -> 2
      byte[] fileOffset = ByteUtil.intToBytes(singleFileSize.get());
      byte[] cidx = new byte[6];
      System.arraycopy(fileNum, 2, cidx, 0, 2);
      System.arraycopy(fileOffset, 0, cidx, 2, 4);
      indexWrite.write(cidx);

      singleFileSize.getAndAdd(compressData.length);

      if ((blockNum - minBlockNum) % 1000 == 0) {
        logger.info("write block {}", blockNum);
      }
    }
    logger.info("write block {}", blockNum);

    if (dataWrite != null) {
      dataWrite.flush();
      dataWrite.close();
    }
    indexWrite.flush();
    indexWrite.close();

    logger.info("minBlockNum:{}, maxBlockNum:{}", minBlockNum, maxBlockNum);
  }

  @Override
  public Integer call() throws Exception {
    createDir();
    writeIndexAndData("block");
    return 0;
  }

  public static void main(String[] args) throws IOException {
    TronFreezer tronFreezer = new TronFreezer();
    tronFreezer.createDir();
    tronFreezer.writeIndexAndData("block");
  }

}

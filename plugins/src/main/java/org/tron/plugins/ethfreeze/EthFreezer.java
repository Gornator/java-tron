package org.tron.plugins.ethfreeze;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
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
public class EthFreezer implements Callable<Integer> {

  String dbDir = "output-directory"; //jar包当前平级目录
  int maxFileSize = 200 * 1000 * 1000;
  long maxBlockNum = 0;
  long minBlockNum = Long.MAX_VALUE;

  private void createDir() {
    File folder = new File("binaryblock");
    if (!folder.exists() && !folder.isDirectory()) {
      folder.mkdir();
    }
    folder = new File("ancient");
    if (!folder.exists() && !folder.isDirectory()) {
      folder.mkdir();
    }
  }

  public void tableToBinaryBlock(String tableName) throws IOException {
    Path path = Paths.get(dbDir, "database", tableName);

    DB db = DBUtils.newLevelDb(path);
    DBIterator dbIterator = db.iterator();
    dbIterator.seekToFirst();

    int i = 0;
    while (dbIterator.hasNext()) {
      i += 1;
      Entry<byte[], byte[]> v = dbIterator.next();
      byte[] blockByte = new byte[8];
      System.arraycopy(v.getKey(), 0, blockByte, 0, 8);
      long blockNum = ByteUtil.byteArrayToLong(blockByte);
      if (blockNum == 0) {
        continue;
      }
      minBlockNum = Math.min(minBlockNum, blockNum);
      maxBlockNum = Math.max(maxBlockNum, blockNum);
      int subPath = (int) (blockNum / 10000);
      File folder = new File(String.format("binaryblock/%04d", subPath));
      if (!folder.exists() && !folder.isDirectory()) {
        folder.mkdir();
      }
      File dataFile = new File(String.format("binaryblock/%04d/%s.data", subPath, blockNum));
      FileOutputStream indexWrite = new FileOutputStream(dataFile, false);
      indexWrite.write(Snappy.compress(v.getValue()));
      indexWrite.flush();
      indexWrite.close();
      if (i % 10000 == 0) {
        logger.info("read block {}", blockNum);
      }
    }
    dbIterator.close();
    db.close();
  }

  public void writeIndexAndData(String tableName) throws IOException {

    AtomicInteger fileCount = new AtomicInteger(0);
    AtomicInteger singleFileSize = new AtomicInteger(0);
    Map<Integer, FileOutputStream> count2FileStream = new HashMap<>();

    FileOutputStream fw = null;
    File indexFile = new File(String.format("ancient/%s.cidx", tableName));
    FileOutputStream indexWrite = new FileOutputStream(indexFile, false);
    for (long blockNum = minBlockNum; blockNum <= maxBlockNum; blockNum += 1) {
      int subPath = (int) (blockNum / 10000);
      File binaryFile = new File(String.format("binaryblock/%04d/%s.data", subPath, blockNum));

      BufferedInputStream in = new BufferedInputStream(new FileInputStream(binaryFile));

      ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
      byte[] temp = new byte[1024];
      int size;
      while ((size = in.read(temp)) != -1) {
        out.write(temp, 0, size);
      }
      in.close();
      byte[] compressData = out.toByteArray();
      out.close();

      if (singleFileSize.get() + compressData.length >= maxFileSize) {
        if (fw != null) {
          fw.flush();
          fw.close();
        }
        fileCount.incrementAndGet();
        singleFileSize.set(0);
      }

      //file handler
      fw = count2FileStream.get(fileCount.get());
      if (fw == null) {
        File file = new File(String.format("ancient/%s.%04d.cdat", tableName, fileCount.get()));
        fw = new FileOutputStream(file, false);
        count2FileStream.put(fileCount.get(), fw);
      }
      fw.write(compressData);

      //write index
      byte[] fileNum = ByteUtil.intToBytes(fileCount.get()); // 4 -> 2
      byte[] fileOffset = ByteUtil.intToBytes(singleFileSize.get());
      byte[] cidx = new byte[6];
      System.arraycopy(fileNum, 2, cidx, 0, 2);
      System.arraycopy(fileOffset, 0, cidx, 2, 4);
      indexWrite.write(cidx);

      singleFileSize.getAndAdd(compressData.length);

      if ((blockNum - minBlockNum) % 1000 == 0 || blockNum == maxBlockNum) {
        logger.info("write block {}", blockNum);
      }
    }

    if (fw != null) {
      fw.flush();
      fw.close();
    }
    indexWrite.flush();
    indexWrite.close();

    logger.info("minBlockNum:{}, maxBlockNum:{}", minBlockNum, maxBlockNum);
  }

  @Override
  public Integer call() throws Exception {
    createDir();
    tableToBinaryBlock("block");
    writeIndexAndData("block");
    return 0;
  }

  public static void main(String[] args) throws IOException {
    EthFreezer readFromLevelDb = new EthFreezer();
    readFromLevelDb.createDir();
    readFromLevelDb.tableToBinaryBlock("block");
    readFromLevelDb.writeIndexAndData("block");
  }

}

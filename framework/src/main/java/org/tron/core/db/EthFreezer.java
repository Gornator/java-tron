package org.tron.core.db;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.tron.common.storage.leveldb.LevelDbDataSourceImpl;
import org.tron.common.utils.ByteUtil;
import org.tron.common.utils.Sha256Hash;
import org.tron.core.capsule.BlockCapsule.BlockId;
import org.tron.core.config.args.Args;
import org.xerial.snappy.Snappy;

@Slf4j(topic = "DB")
public class EthFreezer {

  String dbDir = "output-directory/"; //jar包当前平级目录

  long maxBlockNum = 0; //50536001
  long minBlockNum = Long.MAX_VALUE; //50601536

  public void tableToBinaryBlock(String tableName) throws IOException {
    LevelDbDataSourceImpl dbSource = new LevelDbDataSourceImpl(dbDir, tableName);
    dbSource.initDB();
    for (Entry<byte[], byte[]> v : dbSource) {
      Sha256Hash sha256Hash = new Sha256Hash(v.getKey());
      BlockId blockId = new BlockId(sha256Hash);
      long blockNum = blockId.getNum();
      if (blockNum == 0) {
        continue;
      }
      minBlockNum = Math.min(minBlockNum, blockNum);
      maxBlockNum = Math.max(maxBlockNum, blockNum);
      File indexFile = new File(String.format("binaryblock/%s.data", blockNum));
      FileOutputStream indexWrite = new FileOutputStream(indexFile, false);
      indexWrite.write(Snappy.compress(v.getValue()));
      indexWrite.flush();
      indexWrite.close();
    }
  }

  public void writeIndexAndData(String tableName) throws IOException {

    AtomicInteger fileCount = new AtomicInteger(0);
    AtomicInteger singleFileSize = new AtomicInteger(0);
    Map<Integer, FileOutputStream> count2FileStream = new HashMap<>();

    FileOutputStream fw = null;
    File indexFile = new File(String.format("ancient/%s.cidx", tableName));
    FileOutputStream indexWrite = new FileOutputStream(indexFile, false);
    for (long blockNum = minBlockNum; blockNum <= maxBlockNum; blockNum += 1) {

      File binaryFile = new File(String.format("binaryblock/%s.data", blockNum));
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

      if (singleFileSize.get() + compressData.length >= 2 * 1000 * 1000 * 1000) {
        if (fw != null) {
          fw.flush();
          fw.close();
        }
        fileCount.incrementAndGet();
        singleFileSize.set(0);
      }

      //每个 file 只用打开一次
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

  public static void main(String[] args) throws IOException {
    Args.setParam(args, "config-nile.conf");
    EthFreezer readFromLevelDb = new EthFreezer();
    readFromLevelDb.tableToBinaryBlock("block");
    readFromLevelDb.writeIndexAndData("block");
  }
}

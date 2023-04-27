package org.tron.core.db;


import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.tron.common.utils.ByteUtil;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.exception.BadItemException;
import org.xerial.snappy.Snappy;

@Slf4j(topic = "DB")
public class BenchMarkFreezer {

  public long minBlockNum = 50536001;
  public long maxBlockNum = 50601536;
  private Map<Integer, RandomAccessFile> index2Input = new HashMap<>();
  private byte[] cIdx;

  private void readIndex() throws IOException {
    File indexFile = new File("ancient/block.cidx");
    FileInputStream in = new FileInputStream(indexFile);

    ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
    byte[] temp = new byte[1024];
    int size;
    while ((size = in.read(temp)) != -1) {
      out.write(temp, 0, size);
    }
    in.close();
    cIdx = out.toByteArray();
    out.close();
  }

  public BenchMarkFreezer() throws IOException {
    readIndex();
  }

  private int[] getFileAndOffset(byte[] index) {
    //get file number
    byte[] numberByte = new byte[2];
    System.arraycopy(index, 0, numberByte, 0, 2);
    int fileNum = ByteUtil.byteArrayToInt(numberByte);

    //get start
    byte[] offsetByte = new byte[4];
    System.arraycopy(index, 2, offsetByte, 0, 4);
    int offset = ByteUtil.byteArrayToInt(offsetByte);

    return new int[] {fileNum, offset};
  }

  public void readBlockNum(long blockNum) throws IOException, BadItemException {
    int start = (int) (blockNum - minBlockNum) * 6;
    int middle = (int) (blockNum - minBlockNum + 1) * 6;

    byte[] cur = new byte[6];
    byte[] next = null;
    System.arraycopy(cIdx, start, cur, 0, 6);
    if (middle < cIdx.length) {
      next = new byte[6];
      System.arraycopy(cIdx, middle, next, 0, 6);
    }

    int[] curData = getFileAndOffset(cur);

    int fileNum = curData[0];
    int offset = curData[1];
    //logger.info("block number {}, file number {}, offset {}", blockNum, fileNum, offset);
    int size = 0;
    boolean readToEnd = false;

    //get read size. if no next or next' block != cur, read end
    if (next != null) {
      int[] nextData = getFileAndOffset(next);
      if (curData[0] == nextData[0]) {
        size = nextData[1] - curData[1];
      } else {
        readToEnd = true;
      }
    } else {
      readToEnd = true;
    }

    RandomAccessFile randomAccessFile = index2Input.get(fileNum);
    if (randomAccessFile == null) {
      File dataFile = new File(String.format("ancient/block.%04d.cdat", fileNum));
      randomAccessFile = new RandomAccessFile(dataFile, "r");
      index2Input.put(fileNum, randomAccessFile);
    }

    randomAccessFile.seek(offset);

    if (readToEnd) {
      size = (int) (randomAccessFile.length() - offset);
    }

    byte[] data = new byte[size];
    randomAccessFile.read(data, 0, size);
    byte[] unCompressData = Snappy.uncompress(data);

    //BlockCapsule blockCapsule = new BlockCapsule(unCompressData);
    //Assert.assertEquals(blockNum, blockCapsule.getNum());
  }

  public static void main(String[] args) throws IOException, BadItemException {
    BenchMarkFreezer benchMarkFreezer = new BenchMarkFreezer();
    long start = System.currentTimeMillis();
    //for (long i = benchMarkFreezer.minBlockNum; i < benchMarkFreezer.maxBlockNum; i++) {
    //  benchMarkFreezer.readBlockNum(i);
    //}
    Random random = new Random();
    int count = 0;
    while (count < 1_000_000) {
      int offset = random.nextInt(
          (int) (benchMarkFreezer.maxBlockNum - benchMarkFreezer.minBlockNum));
      benchMarkFreezer.readBlockNum(benchMarkFreezer.minBlockNum + offset);
      count += 1;

      if (count % 10000 == 0) {
        long end = System.currentTimeMillis();
        logger.info("read block count {}, cost {} ms", count, end - start);
      }
    }
  }
}

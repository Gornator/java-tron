package org.tron.common.utils;

import java.util.Map;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.tron.common.setting.RocksDbSettings;
import org.tron.common.args.GenesisBlock;

public class DBConfig {

  //Odyssey3.2 hard fork -- ForkBlockVersionConsts.ENERGY_LIMIT
  @Setter
  public static boolean ENERGY_LIMIT_HARD_FORK = false;
  @Getter
  @Setter
  private static int dbVersion;
  @Getter
  @Setter
  private static String dbEngine;
  @Getter
  @Setter
  private static String outputDirectoryConfig;
  @Getter
  @Setter
  private static Map<String, Property> propertyMap;
  @Getter
  @Setter
  private static GenesisBlock genesisBlock;
  @Getter
  @Setter
  private static boolean dbSync;
  @Getter
  @Setter
  private static RocksDbSettings rocksDbSettings;
  @Getter
  @Setter
  private static int allowMultiSign;
  @Getter
  @Setter
  private static long maintenanceTimeInterval; // (ms)
  @Getter
  @Setter
  private static long allowAdaptiveEnergy; //committee parameter
  @Getter
  @Setter
  private static long allowDelegateResource; //committee parameter
  @Getter
  @Setter
  private static long allowTvmTransferTrc10; //committee parameter
  @Getter
  @Setter
  private static long allowTvmConstantinople; //committee parameter
  @Getter
  @Setter
  private static long allowTvmSolidity059; //committee parameter
  @Getter
  @Setter
  private static long allowSameTokenName; //committee parameter
  @Getter
  @Setter
  private static long allowCreationOfContracts; //committee parameter
  @Getter
  @Setter
  private static long allowShieldedTransaction; //committee parameter
  @Getter
  @Setter
  private static String Blocktimestamp;
  @Getter
  @Setter
  private static long allowAccountStateRoot;
  @Getter
  @Setter
  private static long blockNumForEneryLimit;
}
package org.tron.plugins;

import org.tron.plugins.ethfreeze.BenchFreezer;
import org.tron.plugins.ethfreeze.BenchLevelDb;
import org.tron.plugins.ethfreeze.BenchMapFreezer;
import org.tron.plugins.ethfreeze.Test;
import org.tron.plugins.ethfreeze.TronFreezer;
import picocli.CommandLine;

@CommandLine.Command(name = "db",
    mixinStandardHelpOptions = true,
    version = "db command 1.0",
    description = "An rich command set that provides high-level operations  for dbs.",
    subcommands = {CommandLine.HelpCommand.class,
        DbMove.class,
        DbArchive.class,
        DbConvert.class,
        DbLite.class,
        DbCopy.class,
        TronFreezer.class,
        BenchFreezer.class,
        BenchLevelDb.class,
        BenchMapFreezer.class,
        Test.class
    },
    commandListHeading = "%nCommands:%n%nThe most commonly used db commands are:%n"
)
public class Db {

}

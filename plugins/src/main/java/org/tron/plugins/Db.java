package org.tron.plugins;

import org.tron.plugins.ethfreeze.BenchFreezer;
import org.tron.plugins.ethfreeze.BenchLevelDb;
import org.tron.plugins.ethfreeze.EthFreezer;
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
        EthFreezer.class,
        BenchFreezer.class,
        BenchLevelDb.class
    },
    commandListHeading = "%nCommands:%n%nThe most commonly used db commands are:%n"
)
public class Db {

}

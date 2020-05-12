package gov.nasa.pds.registry.mgr.util.log;

import java.io.File;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.AppenderComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.api.LoggerComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.RootLoggerComponentBuilder;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;


public class Log4jConfigurator
{
    public static void configure(String verbosity, String filePath) 
    {
        ConfigurationBuilder<BuiltConfiguration> cfg = ConfigurationBuilderFactory.newConfigurationBuilder();
        cfg.setStatusLevel(Level.ERROR);
        cfg.setConfigurationName("Harvest");
        
        // Appenders
        addConsoleAppender(cfg, "console");
        addFileAppender(cfg, "file", filePath);

        // Root logger
        RootLoggerComponentBuilder rootLog = cfg.newRootLogger(Level.OFF);
        rootLog.add(cfg.newAppenderRef("console"));
        rootLog.add(cfg.newAppenderRef("file"));
        cfg.add(rootLog);
        
        // Default Harvest logger
        Level level = parseLogLevel(verbosity);
        LoggerComponentBuilder defLog = cfg.newLogger("gov.nasa.pds.registry", level);
        cfg.add(defLog);
        
        // Minimal logger
        LoggerComponentBuilder minLog = cfg.newLogger("registry-min-info", Level.INFO);
        cfg.add(minLog);
        
        // Init Log4j
        Configurator.initialize(cfg.build());
    }
    
    
    private static void addConsoleAppender(ConfigurationBuilder<BuiltConfiguration> cfg, String name)
    {
        AppenderComponentBuilder appender = cfg.newAppender(name, "CONSOLE");
        appender.addAttribute("target", ConsoleAppender.Target.SYSTEM_OUT);
        appender.add(cfg.newLayout("PatternLayout").addAttribute("pattern", "[%level] %msg%n%throwable"));
        cfg.add(appender);
    }
    
    
    private static void addFileAppender(ConfigurationBuilder<BuiltConfiguration> cfg, String name, String filePath)
    {
        // Use default log name if not provided
        if(filePath == null)
        {
            File dir = new File("/tmp/registry");
            dir.mkdirs();
            filePath = "/tmp/registry/registry.log";
        }
        
        AppenderComponentBuilder appender = cfg.newAppender(name, "FILE");
        appender.addAttribute("fileName", filePath);
        appender.addAttribute("append", false);
        appender.add(cfg.newLayout("PatternLayout").addAttribute("pattern", "%d [%level] %msg%n%throwable"));
        cfg.add(appender);
    }
    
    
    private static Level parseLogLevel(String verbosity)
    {
        switch(verbosity)
        {
        case "0": return Level.ALL;
        case "1": return Level.INFO;
        case "2": return Level.WARN;
        case "3": return Level.ERROR;
        }

        // Logger is not setup yet. Print to console.
        System.out.println("[WARNING] Invalid log verbosity '" + verbosity + "'. Will use 1 (Info).");
        return Level.INFO;
    }

}

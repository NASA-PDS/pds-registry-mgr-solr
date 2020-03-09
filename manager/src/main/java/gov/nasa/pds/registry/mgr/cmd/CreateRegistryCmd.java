package gov.nasa.pds.registry.mgr.cmd;

import org.apache.commons.cli.CommandLine;

public class CreateRegistryCmd implements CliCommand
{
    public CreateRegistryCmd()
    {
    }
    
    
    @Override
    public void run(CommandLine cmdLine)
    {
        if(cmdLine.hasOption("help"))
        {
            printHelp();
            return;
        }
        
        
    }

    
    public void printHelp()
    {
        System.out.println("Usage: registry-manager create-registry <options>");

        System.out.println();
        System.out.println("Create registry collection");
        System.out.println();
        System.out.println("Optional parameters:");
        System.out.println("  -zkHost <host>      One or more ZooKeeper hosts separated by comma. Default value is localhost:9983");
        System.out.println("  -zkPath <path>      ZooKeeper path. Default value is /");
        System.out.println("  -configDir <dir>    Configuration directory. Default value is $REGISTRY_HOME/solr/collections/registry");
        System.out.println("  -shards <number>    Number of shards for registry collection. Default value is 1");
        System.out.println("  -replicas <number>  Number of replicas for registry collection. Default value is 1");
    }

}

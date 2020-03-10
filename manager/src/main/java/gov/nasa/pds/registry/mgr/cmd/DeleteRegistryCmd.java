package gov.nasa.pds.registry.mgr.cmd;

import org.apache.commons.cli.CommandLine;

public class DeleteRegistryCmd implements CliCommand
{
    public DeleteRegistryCmd()
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
        System.out.println("Usage: registry-manager delete-registry <options>");

        System.out.println();
        System.out.println("Delete registry collection and all its data");
        System.out.println();
        System.out.println("Optional parameters:");
        System.out.println("  -zkHost <host>   One or more ZooKeeper hosts separated by comma. Default value is localhost:9983");
        System.out.println("  -zkPath <path>   ZooKeeper path. Default value is /");
    }

}

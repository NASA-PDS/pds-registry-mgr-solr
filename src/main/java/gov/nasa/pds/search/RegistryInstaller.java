// Copyright 2019, California Institute of Technology ("Caltech").
// U.S. Government sponsorship acknowledged.
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// * Redistributions of source code must retain the above copyright notice,
// this list of conditions and the following disclaimer.
// * Redistributions must reproduce the above copyright notice, this list of
// conditions and the following disclaimer in the documentation and/or other
// materials provided with the distribution.
// * Neither the name of Caltech nor its operating division, the Jet Propulsion
// Laboratory, nor the names of its contributors may be used to endorse or
// promote products derived from this software without specific prior written
// permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

package gov.nasa.pds.search;

import static java.lang.System.getProperty;
import static java.lang.System.getenv;

import static gov.nasa.pds.search.util.RegistryInstallerUtils.copyDir;
import static gov.nasa.pds.search.util.RegistryInstallerUtils.getPreset;
import static gov.nasa.pds.search.util.RegistryInstallerUtils.print;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.pds.search.util.RegistryInstallerUtils;

import org.apache.commons.io.FilenameUtils;


/*
 * @author hyunlee
 *
 */
public class RegistryInstaller {
	private static final Logger log = LoggerFactory.getLogger(RegistryInstaller.class);

	private static final String SEP = File.separator;

	// Number of bytes per gigabyte
	public static final long BYTES_PER_GIG = (long)1e+9;
	private static String solrCmd = "solr";
	private static String dockerCmd = "deploy-docker.sh";
	private static String envStr = null;
	private static String prompt = null;

	private static String osName;
	private static String termName = null;
	private static int solrPort = 8983;
	private static String solrHost = "localhost";

	private static String registry_version;
	private static String registry_root;
	private static String solrRoot;
	private static String solrBin;
	private static String registry_docker_build;

	private static String installType;

	private static boolean docker_mode = false;
	private static boolean delete = false;

	private static int maxShardsPerNode = 2;
	private static int numShards = 1;
	private static int replicationFactor = 1;

	public RegistryInstaller() {}

	public static void main(String args[]) {
		for (String arg : args) {
            if (arg.equals("--delete") || arg.equals("-d")) {
                delete = true;
                break;
            }
        }
		Scanner reader = new Scanner(System.in);  // Reading from System.in
//		harvest_home = reader.next();

		System.out.print("Enter an installation mode (docker or standalone): ");
		installType = reader.next(); // Scans the next token of the input as a string.			

		if (installType.equalsIgnoreCase("docker"))
			docker_mode = true;

		Path currentRelativePath = Paths.get("");
		registry_root = currentRelativePath.toAbsolutePath().toString() + SEP + "..";

		print("STARTING Registry Installer in " + installType + " mode.");
		getVersion();
		getOsName();
		getTerminal();
		getSolrPort();
		getSolrHost();

		try {
			printWelcomeMessage();
		} catch (Exception ex) {
			ex.printStackTrace();
		}	

		if (!docker_mode) {  // standalone mode
			System.out.print("Enter location of SOLR installation: ");
			solrRoot = reader.next();

			init();
			if (osName.contains("Windows")) {
				solrCmd = "solr.cmd";
			}

			if (delete) {
				deleteRegistrySearchCollection();
				stopSOLRServer();
				exit(1); // to test for now.
			}

			// copy search service confs, *jar and lib into SOLR directories
			setupSOLRDirs();
			startSOLRServer();

			maxShardsPerNode = Integer.parseInt(getPreset("maxShardsPerNode"));
			numShards = Integer.parseInt(getPreset("numShards"));
			replicationFactor = Integer.parseInt(getPreset("replicationFactor"));

			// Create 'registry' collection
			createRegistryCollection();

			// Create 'xpath' collection
			createRegistryXpathCollection();

			// Create 'pds' collection
			createSearchCollection();
		}
		else {
			registry_docker_build = registry_root+SEP+"build";
			prompt = "noPrompt";
            //print("termName = " + termName);
			if (osName.contains("Windows") && termName.contains("cygwin")) {
				envStr = "bash";
				String tmpCmd = FilenameUtils.separatorsToUnix(SEP + registry_docker_build+SEP+dockerCmd);
				print("tmpCmd = " + tmpCmd);
				dockerCmd = tmpCmd.replace(":", "");
				dockerCmd = FilenameUtils.normalize(dockerCmd, true);
			}
			else {
				dockerCmd = FilenameUtils.separatorsToSystem(registry_docker_build+SEP+dockerCmd);
			}
			
			if (delete) {				
				executeDockerBuild("uninstall", envStr);
			}
			else { 
				executeDockerBuild("install", envStr);
			}
		}
		
		reader.close();
	}

	private static void init() {
		solrBin  = solrRoot + SEP + "bin";
	}

	private static void exit(int status) {
		System.exit(status);
	}

	private static void getVersion() {
		//System.out.println("getenv(REGISTRY_VER) = " + getenv("REGISTRY_VER"));
		registry_version = getenv("REGISTRY_VER");

	}

	private static void getOsName() {
		osName = getProperty("os.name");
	}

	private static void getTerminal() {
		if (System.getenv("TERM")!=null)
			termName = System.getenv("TERM");
	}

	private static void getSolrPort() {
		solrPort = Integer.parseInt(getPreset("solr.port"));
	}

	private static void getSolrHost() {
		solrHost = getPreset("solr.host");
	}

	private static void printWelcomeMessage() throws Exception {
		print ("");
		print ("  Registry   .....   ");
		print ("       ( v " + registry_version + " )");
		print ("       ( installing on platform: " + osName + " )");
		InetAddress inetAddress = InetAddress.getLocalHost();
        print ("       ( IP Address:- " + inetAddress.getHostAddress() + " )");
        print ("       ( Host Name:- " + inetAddress.getHostName() + " )");
        print ("");
	}

	private static void setupSOLRDirs() 
	{
        try 
        {
			// Copy PDS plugins
    		String solrLib  = solrRoot + SEP + "contrib" + SEP + "pds" + SEP + "lib";
        	copyDir(registry_root + SEP + "dist", solrLib);

			String solrConfigsets = solrRoot + SEP + "server" + SEP + "solr" + SEP + "configsets";
			
			// Copy config sets
			copyDir(registry_root + SEP + "conf" + SEP + "pds", 
					solrConfigsets + SEP + "pds" + SEP + "conf");
			
			copyDir(registry_root + SEP + "conf" + SEP + "registry", 
					solrConfigsets + SEP + "registry" + SEP + "conf");
		} 
        catch (IOException ex) 
        {
			ex.printStackTrace();
		}
	}

	private static void startSOLRServer() {		
        Process progProcess = null;
        int returnVal = -1;
		try {
			// need to check wheather the SOLR server is running already or not
			// ./solr status       -> "No Solr nodes are running."
			String[] statusCmd = new String[] 
			{
				solrBin + SEP + solrCmd, 
				"status", 
				"-p", String.valueOf(solrPort)
			};
		
			progProcess = Runtime.getRuntime().exec(statusCmd);
			BufferedReader in = new BufferedReader(
                                new InputStreamReader(progProcess.getInputStream()));
            String line = null;
            boolean runningProc = false;
            while ((line = in.readLine()) != null) {
                //System.out.println(line);
                if (line.contains("running on port " + solrPort)) {
                	runningProc = true;
                	System.out.println(line);
                }
            }

			try{
               	returnVal = progProcess.waitFor();

               	if (runningProc) {
               		print("Failed to start the SOLR server. There is already RUNNING SOLR instance.");
               		exit(1);
           		}
            } catch(Exception ex){
               ex.printStackTrace();
            }
            in.close();
            print("\nStarting a SOLR server... Waiting up to 180 seconds to see Solr running on port " + solrPort + "...");

			String[] execCmd = new String[] 
			{ 
				solrBin + SEP + solrCmd, 
				"start", "-c",
		        "-p", String.valueOf(solrPort), 
		        "-s", solrRoot + SEP + "server" + SEP + "solr"};

			progProcess = Runtime.getRuntime().exec(execCmd);		
			in = new BufferedReader(new InputStreamReader(progProcess.getInputStream()));
            
            // SOLR starup hangs for some reason
            // Sleep for x seconds instead of calling waitFor() on Windows platform
            if (osName.contains("Windows")) {
            	Thread.sleep(Integer.parseInt(getPreset("waitTime"))*1000);
            	print("SOLR server is started on " + osName);
            }
            else { 
            	while ((line = in.readLine()) != null) {
                	System.out.println(line);
            	}
            	try{
               		returnVal = progProcess.waitFor();
               		print("Return status from the SOLR server startup = " + returnVal);
               		if (returnVal!=0) {
                  		print("Failed to start the SOLR server....");
                  		exit(1);
               		}
               		else 
               			print("The SOLR server is started successfully.");
            	} catch(InterruptedException ie){
               		ie.printStackTrace();
            	} 
            }       
           	in.close();
		} catch (Exception err) {
			err.printStackTrace();
		} finally {
			RegistryInstallerUtils.safeClose(progProcess);
        }
	}

	private static void stopSOLRServer() {		
        Process progProcess = null;
        int returnVal = -1;
		try {
			// need to check wheather the SOLR server is running already or not
			// ./solr status       -> "No Solr nodes are running."
			progProcess = Runtime.getRuntime().exec(new String[] { solrBin + SEP + solrCmd, "status", "-p", String.valueOf(solrPort)});
			BufferedReader in = new BufferedReader(
                                new InputStreamReader(progProcess.getInputStream()));
            String line = null;
            boolean runningProc = false;
            while ((line = in.readLine()) != null) {
                System.out.println(line);
                if (line.contains("running on port")) {
                	runningProc = true;
                }
            }
			try{
               	returnVal = progProcess.waitFor();
          		if (!runningProc) {
               		print("There is no RUNNING SOLR instance. No need to stop the SOLR server.");
               		exit(1);
           		}
            } catch(Exception ex){
               ex.printStackTrace();
            }
            in.close();

            print("\nStopping the SOLR server...");

            //./bin/solr stop
			String[] execCmd = new String[] { solrBin + SEP + solrCmd, "stop", "-p", String.valueOf(solrPort)};

			progProcess = Runtime.getRuntime().exec(execCmd);		
			in = new BufferedReader(new InputStreamReader(progProcess.getInputStream()));
            while ((line = in.readLine()) != null) {
                System.out.println(line);
            }
            try{
               	returnVal = progProcess.waitFor();
               	if (returnVal!=0) {
                  	print("Failed to stop the SOLR server....");
                  	exit(1);
               	}
               	else {
               		print("The SOLR server is stopped successfully.");
               	}
            } catch(InterruptedException ie){
               	ie.printStackTrace();
            }    
           	in.close();
		} catch (Exception err) {
			err.printStackTrace();
		} finally {
			RegistryInstallerUtils.safeClose(progProcess);
        }
	}

	
	private static void createSearchCollection() 
	{
		String[] execCmd = new String[] 
		{
			solrBin + SEP + solrCmd, 
			"create", 
			"-p", String.valueOf(solrPort), 
			"-c", "pds", "-d", "pds"
		};
						
		execCreateCommand(execCmd, "search collection (pds)");
	}

	
	private static void createRegistryXpathCollection() 
	{
		// Create collection
		String[] execCmd = new String[]
		{
			"curl", 
			"http://" + solrHost + ":" + solrPort
				+ "/solr/admin/collections?action=CREATE&name=xpath" 
				+ "&maxShardsPerNode=" + maxShardsPerNode 
				+ "&numShards=" + numShards 
				+ "&replicationFactor=" + replicationFactor
		};
		
		execCreateCommand(execCmd, "xpath collection");
		
		// Create alias
		execCmd = new String[]
		{
            "curl", 
            "http://" + solrHost + ":" + solrPort
            	+ "/solr/admin/collections?action=CREATEALIAS&name=registry-xpath&collections=xpath"
		};

		execCreateCommand(execCmd, "xpath collection alias");
	}

	
	private static void createRegistryCollection() 
	{
		// Create collection
		String[] execCmd = new String[] 
		{
			solrBin + SEP + solrCmd, 
			"create", 
			"-p", String.valueOf(solrPort), 
			"-c", "registry", "-d", "registry"
		};
				
		execCreateCommand(execCmd, "registry collection");
            

        // Create alias
        execCmd = new String[]
        {
        	"curl", 
        	"http://" + solrHost + ":" + solrPort
        		+ "/solr/admin/collections?action=CREATEALIAS&name=registry-blob&collections=registry"
        };

        execCreateCommand(execCmd, "registry collection alias");
	}

	
	private static void execCreateCommand(String[] cmd, String name)
	{
		Process proc = null;

		print("Creating " + name + "...");
		
        try 
        {
			proc = Runtime.getRuntime().exec(cmd);
			
			BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			String line = null;
	        while ((line = in.readLine()) != null) 
	        {
	            System.out.println(line);
	        }
        
        	int returnVal = proc.waitFor();
            print("Return status from creating " + name + " = " + returnVal);
            if(returnVal != 0) 
            {
                print("Failed to create " + name);
                exit(1);
            }
            else 
            {
            	// Capitalize
            	name = name.substring(0, 1).toUpperCase() + name.substring(1);
            	print(name + " is created successfully.");
            }
            
            RegistryInstallerUtils.safeClose(proc);
        } 
        catch(Exception ex)
        {
        	ex.printStackTrace();
        	exit(1);
        }
	}

	
	private static void execDeleteCommand(String[] cmd, String name)
	{
		Process proc = null;

		print("Deleting " + name + "...");
		
        try 
        {
			proc = Runtime.getRuntime().exec(cmd);
			
			BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			String line = null;
	        while ((line = in.readLine()) != null) 
	        {
	            System.out.println(line);
	        }
        
        	int returnVal = proc.waitFor();
            print("Return status from deleting " + name + " = " + returnVal);
            if(returnVal != 0) 
            {
                print("Failed to delete " + name);
            }
            else 
            {
            	// Capitalize
            	name = name.substring(0, 1).toUpperCase() + name.substring(1);
            	print(name + " is deleted successfully.");
            }
        } 
        catch(Exception ex)
        {
        	ex.printStackTrace();
        }
        finally
        {
        	RegistryInstallerUtils.safeClose(proc);
        }
	}

		
	private static void deleteRegistrySearchCollection() 
	{
		// Registry alias
		String[] execCmd = new String[]
		{
			"curl", 
			"http://" + solrHost + ":" + solrPort
				+ "/solr/admin/collections?action=DELETEALIAS&name=registry-blob&collection=.system"
		};

		execDeleteCommand(execCmd, "registry collection alias");
		
		// Xpath alias
		execCmd = new String[]
		{
			"curl", 
			"http://" + solrHost + ":" + solrPort
				+ "/solr/admin/collections?action=DELETEALIAS&name=registry-xpath&collections=xpath"
		};

		execDeleteCommand(execCmd, "xpath collection alias");
		
        // Registry collection
        execCmd = new String[] 
        { 
        	solrBin + SEP + solrCmd, 
        	"delete", 
        	"-p", String.valueOf(solrPort), 
        	"-c", "registry"
        };

        execDeleteCommand(execCmd, "registry collection");
        
        // XPath collection
        execCmd = new String[] 
        { 
        	solrBin + SEP + solrCmd, 
        	"delete", 
        	"-p", String.valueOf(solrPort), 
        	"-c", "xpath"
        };

        execDeleteCommand(execCmd, "xpath collection");
        
        // Search collection
        execCmd = new String[] 
        { 
        	solrBin + SEP + solrCmd, 
        	"delete", 
        	"-p", String.valueOf(solrPort), 
        	"-c", "pds"
        };
        
        execDeleteCommand(execCmd, "search collection (pds)");
	}

	
	private static void executeDockerBuild(String mode, String envStr) {
		print("Execute to " + mode + " Registry in Docker...");
		Process progProcess = null;
        int returnVal = -1;
		try {
			String[] execCmd = null;
			if (envStr==null) {
				if (prompt==null)
					execCmd = new String[] { dockerCmd, mode, registry_root};
				else
					execCmd = new String[] { dockerCmd, mode, prompt, registry_root};
			}
			else {
				if (prompt==null)
					execCmd = new String[] { envStr, dockerCmd, mode, registry_root};
				else
					execCmd = new String[] { envStr, dockerCmd, mode, prompt, registry_root};

			}

			progProcess = Runtime.getRuntime().exec(execCmd);
			BufferedReader in = new BufferedReader(
                                new InputStreamReader(progProcess.getInputStream()));
			String line = null;
            while ((line = in.readLine()) != null) {
                System.out.println(line);
            }
            try{
                returnVal = progProcess.waitFor();
                if (returnVal!=0) {
                    print("Failed to " + mode + " Registry in Docker.");
                    exit(1);
                }
                else  {
                	Thread.sleep(5000);
               		print("Completed to " + mode + " Registry in Docker.");
                }

            } catch(Exception ex){
               ex.printStackTrace();
            }
			in.close();
		} catch (Exception err) {
			err.printStackTrace();
		} finally {
	        RegistryInstallerUtils.safeClose(progProcess);
        }
	}
	
}

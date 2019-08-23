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

package gov.nasa.pds.search.util;

import java.io.Closeable;
import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegistryInstallerUtils {
	private static final Logger log = LoggerFactory.getLogger(RegistryInstallerUtils.class);
	
	private static final int    MIN_PORT_NUMBER = 1024;
	private static final int    MAX_PORT_NUMBER =  65535;
	private static final String SEP = File.separator;
	private static final String NL  = System.getProperty("line.separator");
	private static Charset charset = StandardCharsets.UTF_8;
	
	private static Console console   = System.console();
	private static InstallerPresets presets = new InstallerPresets();
	
	public static void copy(Path from, Path to) {
		try {
			Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING);
			print(" Copied a file from " + from + " to " + to);
		} catch (IOException e) {
			print("ERROR: Problem copying " + from + " to " + to +" (" + e.toString() + ")");
			System.exit(1);
		}
	}
	
	
	public static void mkDir(String dir) {
		File file = new File(dir);
		if (!file.exists()) {
			if (file.mkdir()) {
				print("  Created a directory: " + dir);
			} else {
				System.err.println("Failed to create directory: " + dir + " !");
				System.exit(1);
			}
		}
		
	}

	public static void mkDirs(String dir) {
		File file = new File(dir);
		if (!file.exists()) {
			if (file.mkdirs()) {
				print(" Created a directory: " + dir);
			} else {
				System.err.println("Failed to create directory: " + dir + " !");
				System.exit(1);
			}
		}
	}
	
	public static void bailOutMissingOption(String option) {
		String exampleURL = "https://wiki.jpl.nasa.gov/display/search/Configure+SEARCH";
		System.out.println("\nWARNING: configuration file is missing the required \"" + option + "\" option.");
		System.out.println("For more information, review the example configuration file at " + exampleURL);
		System.exit(1);
	}
	
	public static void bailOutWithMessage(String msg) {
		System.out.println("\n" + msg);
		System.exit(1);
	}
	
	public static  boolean deleteDirectory(File path) {
		if ( path.exists() ) {
			File[] files = path.listFiles();
			for(int i=0; i<files.length; i++) {
				if (files[i].isDirectory()) {
					deleteDirectory(files[i]);
				}
				else {
					files[i].delete();
				}
			}
		}
		return( path.delete() );
	}
	
	public static String readRequiredLine(String fmt, String requireValueMsg) {
		if (console == null) {
			System.out.println("WARNING: console object is null!  Returning null..");
			return null;
		}
		String input = console.readLine(fmt);
		while (input == null || input.length() < 1) {
			print(requireValueMsg);
			input = console.readLine(fmt);
		}
		return input.trim();
	}
	
	
	public static String readLine(String fmt, String defaultValue) {
		if (console == null) {
			System.out.println("WARNING: console object is null!  Returning null..");
			return null;
		}
		String input = console.readLine(fmt);
		if (input == null || input.length() < 1) {
			input = defaultValue;
		}
		if (input == null) {
			return null;
		}
		return input.trim();
	}
	
	
	public static char[] readPassword(String fmt) {
		if (console == null) {
			System.out.println("WARNING: console object is null!  Returning null..");
			return null;
		}
		return console.readPassword(fmt);
	}
	
	
	public static  void flushConsole() {
		if (console != null) {
			console.flush();
		}
		else {
			System.out.println("WARNING: console object is null!");
		}
	}
	
	
	public static String getPreset(String key) {
		
		if (presets.getProperty(key) == null || presets.getProperty(key).equals("")) {
			return null;
		}
		return presets.getProperty(key).trim();
	}
	
	
	public static boolean isPresetValue(String key) {
		String value = presets.getProperty(key);
		if (value != null && value.length() > 0) {
			return true;
		}
		else {
			return false;
		}
	}
	
	
	/**
	 * Returns true if the specified (local) port is available for use by SEARCH.
	 * 
	 */
	public static boolean isLocalPortAvailable(int port) {
		if (port < MIN_PORT_NUMBER || port > MAX_PORT_NUMBER) {
			print("WARN: port must be between " + MIN_PORT_NUMBER + " and " + MAX_PORT_NUMBER);
			return false;
		}
	
		if (serverListening("localhost", port)) {
			return false;
		}
		
		return true;
	}
	
	/**
	 * Returns true if the specified (remote) port is available for use by SEARCH.
	 * 
	 */
	public static boolean isRemotePortListening(String hostname, int port) {
		if (port < MIN_PORT_NUMBER || port > MAX_PORT_NUMBER) {
			print("WARN: port must be between " + MIN_PORT_NUMBER + " and " + MAX_PORT_NUMBER);
			return false;
		}
	
		return serverListening(hostname, port);
	}
	
	
	/**
	 * Checks to see if a server is listening on the specified port.
	 * 
	 */
	public static boolean serverListening(String host, int port) {
		try {
			Socket s = new Socket(host, port);
			s.close();
			return true;
		}
		catch (IOException ex) {
			// The remote host is not listening on this port
			log.trace("Server '" + host + "' is NOT listening on port " + port + ".");
			return false;
		}
	}

	public static void copyDir(String fromDir, String toDir) throws IOException {
		File srcDir = new File(fromDir);
		File destDir = new File(toDir);

		FileUtils.copyDirectory(srcDir, destDir);
		print(" Copied a directory from " + srcDir + " to " + destDir);
	}
	
	public static void copyAll(String fromDir, String toDir) {
		File srcDir = new File(fromDir);
		File[] srcDirFiles = srcDir.listFiles();
		for (int i = 0; i < srcDirFiles.length; i++) {
			if (srcDirFiles[i].isFile()) {
				String srcFileName = srcDirFiles[i].getName();
				
				copy(Paths.get(fromDir + SEP + srcFileName),
					Paths.get(toDir   + SEP + srcFileName));
			} 
			else if (srcDirFiles[i].isDirectory()) {
				// skip over directories
			}
		}
	}
	
	public static void copyAllType(String fromDir, String toDir, String ext) {
		File srcDir = new File(fromDir);
		File[] srcDirFiles = srcDir.listFiles();
		String extFrom;
		for (int i = 0; i < srcDirFiles.length; i++) {
			if (srcDirFiles[i].isFile()) {
				String srcFileName = srcDirFiles[i].getName();
				extFrom = FilenameUtils.getExtension(srcFileName);
				if (extFrom.equals(ext)) {
					copy(
						Paths.get(fromDir + SEP + srcFileName),
						Paths.get(toDir   + SEP + srcFileName));
				}
			} 
			else if (srcDirFiles[i].isDirectory()) {
				// skip over directories
			}
		}
	}
	
	public static void print(String msg) {
		System.out.println(msg);
	}
	
	public static void openUpPermissions(String filePath) throws IOException {
		print(" Opening up permissions of file: " + filePath + " ...");
		try {
		File file = new File(filePath);
		Set<PosixFilePermission> perms = new HashSet<PosixFilePermission>();
		perms.add(PosixFilePermission.OWNER_READ);
		perms.add(PosixFilePermission.OWNER_WRITE);
		perms.add(PosixFilePermission.OWNER_EXECUTE);
		Files.setPosixFilePermissions(file.toPath(), perms);
		}
		catch (UnsupportedOperationException e) {
			print("ignoring " + e.getMessage());
		}
		catch (Exception e) {
			print("ERROR: " + e.getMessage());
		}
	}
	
	public static String getFileContents(Path filePath) throws IOException {
		return new String(Files.readAllBytes(filePath), charset).replace("\n", NL);
	}
	
	public static void writeToFile(Path filePath, String content) throws IOException {
		Files.write(filePath, content.getBytes(charset));
	}
	
	public static void createFile(Path filePath) throws IOException {
		Files.createFile(filePath);
	}
	
	public static InstallerPresets getInstallerPresets() {
		return presets;
	}

	public static void safeClose(Process proc) 
	{
		if(proc == null) return;
		
        safeClose(proc.getErrorStream());
        safeClose(proc.getInputStream());
        safeClose(proc.getOutputStream());
	}
		
	public static void safeClose(Closeable obj) 
	{
		if(obj == null) return;
		
        try 
        {
            obj.close();
        } 
        catch (Exception ex) 
        {
        	// Ignore
        }
	}

}

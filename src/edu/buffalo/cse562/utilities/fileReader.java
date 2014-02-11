package edu.buffalo.cse562.utilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import edu.buffalo.cse562.exception.fileException;
import edu.buffalo.cse562.logger.logManager;

public class fileReader {

	File file;
	List<String> contents;
	logManager lg;

	public fileReader(String fileName) {
		lg = new logManager();
		String basePath = new File("").getAbsolutePath();
		this.file = new File(basePath+fileName);
		lg.logger.log(Level.INFO, basePath);
		lg.logger.log(Level.INFO, fileName);
	}

	public fileReader(File fileName) {
		this.file = fileName;
	}

	public List<String> readContents() {
		contents = new ArrayList<String>();
		try {
			String line = null;
			BufferedReader buf = new BufferedReader(new FileReader(file));
			while ((line = buf.readLine()) != null) {
				contents.add(line);
			}
			buf.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return contents;

	}

}

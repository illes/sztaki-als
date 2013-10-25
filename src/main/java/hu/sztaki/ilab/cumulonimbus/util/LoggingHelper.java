package hu.sztaki.ilab.cumulonimbus.util;

import java.io.IOException;
import java.util.Enumeration;

import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class LoggingHelper {
	
	public static Logger getFileLogger(String name, String fileName) {
		try {
			Logger logger = Logger.getLogger(name);
			if (fileName == null) return logger;
			Enumeration<Appender> enumeration = logger.getAllAppenders();
			while (enumeration.hasMoreElements()) {
				Appender appender = enumeration.nextElement();
				if (appender instanceof FileAppender) {
					if (((FileAppender)appender).getFile().equals(fileName)) return logger;
				}
			}
//			// setting up a FileAppender dynamically...
//			Layout layout = new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN);//"%d{yy.MM.dd HH:mm:ss.SSS} %-6p [%t] %m%n");
//			FileAppender appender = new FileAppender(layout, fileName, true);
//			logger.addAppender(appender);
			return logger;
		} catch (Exception e) {
			return null;
		}
	}

}

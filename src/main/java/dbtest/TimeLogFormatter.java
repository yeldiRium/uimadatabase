package dbtest;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FileUtils;

public class TimeLogFormatter {

	public static void main(String[] args) throws IOException, ParseException {
		String file = "cassandra.log";
		String modus ="read";
		List<String>lines =  FileUtils.readLines(new File("/home/ahemati/workspace/uimadatabase/dbtest/"+modus+"/"+file));
		File savePath = new File("/home/ahemati/Documents/Papers/textimager_database/logs/"+modus+"/"+file);
		
		StringBuilder sb = new StringBuilder();
		System.out.println("count\ttime");
		sb.append("count\ttime").append("\n");
		for (String string : lines) {
			if(string.length()>0){
				if(!string.split(": ")[0].equals("Finish") && (Integer.parseInt(string.split(": ")[0])-1)%1000==0){
					System.out.println(string.split(": ")[0]+"\t"+(Float.parseFloat(string.split(" ")[1])/1000));
					sb.append(string.split(": ")[0]+"\t"+(Float.parseFloat(string.split(" ")[1])/1000)).append("\n");
				}
			}
		}
		FileUtils.writeStringToFile(savePath, sb.toString());
	}
	
	private static long getSeconds(String iso) throws ParseException{
		DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
		Date reference = dateFormat.parse("00:00:00");
		Date date = dateFormat.parse(iso);
		return (date.getTime() - reference.getTime()) / 1000L;
	}

}

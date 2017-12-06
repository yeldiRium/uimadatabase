package org.hucompute.services.uima.database.neo4j.data;

import java.io.*;
import java.util.Properties;

public class Config extends Properties {


    public Config(String pathFile) throws IOException {
        //System.out.println(System.getProperty("user.dir"));
        String current = new File( "." ).getCanonicalPath();
        BufferedReader lReader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(pathFile)), "UTF-8"));
        this.load(lReader);
        lReader.close();
    }

    public String getMDBConfig() {
        String lResult = getProperty("mdb", "mdb");
        return lResult;
    }

    public String getAppName() {
        String lResult = getProperty("appname", "ExampleCalamariApplication");
        return lResult;
    }

    public String getAppPath() {
        String lResult = getProperty("classpath", "org.hucompute.services.calamari");
        return lResult;
    }

    public String getURL() {
        String lResult = getProperty("url", "141.2.89.20");
        return lResult;
    }

    public String getName() {
        String lResult = getProperty("name", "TTCalmari");
        return lResult;
    }

    public String getDescription() {
        String lResult = getProperty("description", "My TTCalmari");
        return lResult;
    }

    public String getTempFolder() {
        String sResult = getProperty("tmp_folder", "/tmp");
        return sResult;
    }

    public String getHostName() {
        String lResult = getProperty("host_name", "calamari.hucompute.org");
        return lResult;
    }

    public int getPort() {
        int lResult = Integer.valueOf(getProperty("host_port", "8080"));
        return lResult;
    }

    public String getProtokoll() {
        String lResult = getProperty("host_protocol", "http");
        return lResult;
    }

    public int getMinThreads() {
        int lResult = Integer.valueOf(getProperty("minthreads", "10"));
        return lResult;
    }


    public int getMaxThreads() {
        int lResult = Integer.valueOf(getProperty("maxthreads", "20"));
        return lResult;
    }


    public int getTimeOut() {
        int lResult = Integer.valueOf(getProperty("timeout", "6000"));
        return lResult;
    }

    public String getCalamariNS() {
        String lResult = getProperty("calamari_namespace", "element");
        return lResult;
    }


}

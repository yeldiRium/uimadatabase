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
        return getProperty("mdb", "mdb");
    }

    public String getAppName() {
        return getProperty("appname", "ExampleCalamariApplication");
    }

    public String getAppPath() {
        return getProperty("classpath", "org.hucompute.services.calamari");
    }

    public String getURL() {
        return getProperty("url", "141.2.89.20");
    }

    public String getName() {
        return getProperty("name", "TTCalmari");
    }

    public String getDescription() {
        return getProperty("description", "My TTCalmari");
    }

    public String getTempFolder() {
        return getProperty("tmp_folder", "/tmp");
    }

    public String getHostName() {
        return getProperty("host_name", "calamari.hucompute.org");
    }

    public int getPort() {
        return Integer.valueOf(getProperty("host_port", "8080"));
    }

    public String getProtokoll() {
        return getProperty("host_protocol", "http");
    }

    public int getMinThreads() {
        return Integer.valueOf(getProperty("minthreads", "10"));
    }


    public int getMaxThreads() {
        return Integer.valueOf(getProperty("maxthreads", "20"));
    }


    public int getTimeOut() {
        return Integer.valueOf(getProperty("timeout", "6000"));
    }

    public String getCalamariNS() {
        return getProperty("calamari_namespace", "element");
    }
}

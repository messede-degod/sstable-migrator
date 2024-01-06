package com.segfault.sstableMigrator;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;

import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;

import com.maxmind.db.CHMCache;
import com.maxmind.db.MaxMindDbConstructor;
import com.maxmind.db.MaxMindDbParameter;
import com.maxmind.db.Reader;
import java.net.InetAddress;

public class App {

    CQLSSTableWriter writer;
    Reader MMDBReader;
    int processedEntries = 0;
    int processedFiles = 0;
    int lookedUpEntries = 0;

    public static void main(String[] args) throws IOException {

        BufferedReader reader;
        App csw = new App();

        File directory = new File("input/");
        File[] files = directory.listFiles();
        System.out.println("Found: " + files.length + " files in input directory.");

        for (File file : files) {
            try {
                reader = new BufferedReader(new FileReader(file));

                String line = reader.readLine();
                while (line != null) {
                    csw.parseAndInsert(line);
                    line = reader.readLine();
                }

                reader.close();

            } catch (IOException e) {
                System.out.println("During Processing of file: " + file);
                e.printStackTrace();
            } catch (SkippedEntryProcessingException e) {
                System.out.println("During Processing of file: " + file);
                e.printStackTrace();
            }
            csw.processedFiles += 1;

            if (csw.processedFiles % 100 == 0) {
                System.out.println("Processed " + csw.processedFiles + "/" + files.length + " files");
            }

        }

        System.out.println("Finished processing all files.");

        csw.writer.close();
        csw.MMDBReader.close();
        System.out.println("Writer closed Successfully");
        System.out.println("Processed " + csw.processedEntries + " Entries");
        System.out.println("Processed " + csw.processedFiles + " Files");
        System.out.println("LookedUp " + csw.lookedUpEntries + " Records");

    }

    public App() {
        String outputDir = "./output/";
        String keyspace = "ferret";
        String table = "dnsdata";

        String schema = "CREATE TABLE ferret.dnsdata ("
                + " apexDomain VARCHAR,"
                + " recordType VARCHAR,"
                + " subDomain VARCHAR,"
                + " ipAddress INET,"
                + " country VARCHAR,"
                + " city  VARCHAR,"
                + " asn   VARCHAR,"
                + " as_name VARCHAR,"
                + " PRIMARY KEY (ipAddress,apexDomain,subDomain) );";

        String insert = "INSERT INTO ferret.dnsdata (apexDomain, recordType, subDomain, ipAddress, country, city, asn, as_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        File directory = new File(keyspace);
        if (!directory.exists())
            directory.mkdir();

        File filePath = new File(directory, table);
        if (!filePath.exists())
            filePath.mkdir();

        this.writer = CQLSSTableWriter.builder()
                .withPartitioner(Murmur3Partitioner.instance)
                .inDirectory(outputDir)
                .forTable(schema)
                .using(insert).build();

        System.out.println("Writer created Successfully");

        File database = new File("misc/country_asn.mmdb");

        try {
            this.MMDBReader = new Reader(database,new CHMCache(262144));
        } catch (IOException e) {
            System.out.println("Error OPening MMDB :: " + e.toString());
        }
    }

    public void parseAndInsert(String jsonString) throws IOException, SkippedEntryProcessingException {
        JSONObject jo = new JSONObject(jsonString);

        String apexDomain = jo.getString("name");
        JSONArray answers = jo.getJSONObject("data").getJSONArray("answers");

        for (int i = 0; i < answers.length(); i++) {
            JSONObject ans = answers.getJSONObject(i);

            String subdomain = ans.getString("name");
            String ipStr = ans.getString("data");
            String recordType = ans.getString("type");
            String country = "";
            String city = "";
            String asn = "";
            String as_name = "";

            // Read the default Geo Data from DP DB
            try {
                JSONObject geo = ans.getJSONObject("geoIP");
                country = geo.getString("Country");
                city = geo.getString("City");
            } catch (JSONException e) {
                continue;
            }

            // Handle CNAMES properly
            apexDomain = ipStr;
            ipStr = "0.0.0.0";
            InetAddress parsedIpAddress = InetAddress.getByName(ipStr);

            // Lookup GEO and ASN Details using MMDB;
            if (recordType.equals("a")) {
                LookupResult result = this.MMDBReader.get(parsedIpAddress, LookupResult.class);
                if (result != null) {
                    country = result.country;
                    // String asnStr = result.asn.substring(2); // REMOVE AS from string
                    // asn = Integer.parseInt(asnStr);
                    asn = result.asn;
                    as_name = result.as_name;
                }
                this.lookedUpEntries += 1;
                result = null;

            }

            if ( apexDomain != "" && apexDomain != null) {
                this.writeRecord(apexDomain, recordType, subdomain, parsedIpAddress, country, city, asn, as_name);
            } else {
                System.out.println("ip or apexDomain empty!, ignoring record: <" + ipStr + ", " + apexDomain + ">");
            }
        }

    }

    public void writeRecord(String apexDomain, String recordType, String subDomain, InetAddress ipAddress, String country,
            String city, String asn, String as_name)
            throws IOException, SkippedEntryProcessingException {
        try {
            this.writer.addRow(apexDomain, recordType, subDomain, ipAddress, country, city, asn, as_name);
            this.processedEntries++;
        } catch (InvalidRequestException ie) {
            System.out.println("InvalidRequestException: faile to write entry <" + apexDomain + "," + recordType + ","
                    + subDomain + "," + ipAddress + ">");
            ie.printStackTrace();
            System.out.println("Continuing to process other entries...");
            throw new SkippedEntryProcessingException("Skipped Entry Processing");
        }
    }

    public class SkippedEntryProcessingException extends Exception {
        public SkippedEntryProcessingException(String errorMessage) {
            super(errorMessage);
        }
    }

    public static class LookupResult {
        public final String country;
        public final String asn;
        public final String as_name;

        @MaxMindDbConstructor
        public LookupResult(
                @MaxMindDbParameter(name = "country") String country,
                @MaxMindDbParameter(name = "asn") String asn,
                @MaxMindDbParameter(name = "as_name") String as_name) {
            this.country = country;
            this.asn = asn;
            this.as_name = as_name;
        }
    }
}

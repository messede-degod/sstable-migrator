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
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class App {

    CQLSSTableWriter writer;
    Reader MMDBReader;
    int processedEntries = 0;
    int processedFiles = 0;
    int lookedUpEntries = 0;
    static InetAddress zeroAddr = null;

    static Map<String, String> TLDs = new HashMap<String, String>() {
        {
        }
    };

    public static void readTLD() throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader("./TLD.txt"));
        String line = reader.readLine();
        while (line != null) {
            TLDs.put(line, line);
            line = reader.readLine();
        }
        reader.close();
    }

    public static void main(String[] args) throws IOException {

        BufferedReader reader;

        App csw = new App();
        App.zeroAddr = InetAddress.getByName("0.0.0.0");
        App.readTLD();

        File directory = new File("csv_input/");
        File[] files = directory.listFiles();
        System.out.println("Found: " + files.length + " files in input directory.");

        for (File file : files) {
            try {
                reader = new BufferedReader(new FileReader(file));

                String line = reader.readLine();
                while (line != null) {
                    csw.parseAndInsertCSV(line);
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
                + " ip8 INET,"
                + " ip16 INET,"
                + " ip24 INET,"
                + " ipAddress INET,"
                + " country VARCHAR,"
                + " city  VARCHAR,"
                + " asn   VARCHAR,"
                + " as_name VARCHAR,"
                + " tld VARCHAR,"
                + " PRIMARY KEY (ip8,ip16,ip24,ipAddress,tld,apexDomain) );";

        String insert = "INSERT INTO ferret.dnsdata (apexDomain, recordType, subDomain, ip8, ip16, ip24, ipAddress, country, city, asn, as_name,tld) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

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
            this.MMDBReader = new Reader(database, new CHMCache(262144));
        } catch (IOException e) {
            System.out.println("Error OPening MMDB :: " + e.toString());
        }
    }

    public void parseAndInsert(String jsonString) throws IOException, SkippedEntryProcessingException {
        JSONObject jo = new JSONObject(jsonString);

        String domain = jo.getString("name");
        JSONArray answers = jo.getJSONObject("data").getJSONArray("answers");
        String Data[] = App.getTLDAndApexDomain(domain);

        String apexDomain = Data[1];
        String tld = Data[0];

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

            Boolean isARecord = recordType.equals("a");

            InetAddress parsedIpAddress = null;
            InetAddress ip8 = null;
            InetAddress ip16 = null;
            InetAddress ip24 = null;

            if (!isARecord) {
                // Handle CNAME's properly
                apexDomain = ipStr;
                ipStr = "0.0.0.0";
                parsedIpAddress = App.zeroAddr;
                ip8 = App.zeroAddr;
                ip16 = App.zeroAddr;
                ip24 = App.zeroAddr;
            } else {
                parsedIpAddress = InetAddress.getByName(ipStr);
                ip8 = App.getIPBlock(parsedIpAddress, (short) 8);
                ip16 = App.getIPBlock(parsedIpAddress, (short) 16);
                ip24 = App.getIPBlock(parsedIpAddress, (short) 24);

                // Lookup GEO and ASN Details using MMDB;
                LookupResult result = this.MMDBReader.get(parsedIpAddress, LookupResult.class);
                if (result != null) {
                    country = result.country;
                    asn = result.asn;
                    as_name = result.as_name;
                }
                this.lookedUpEntries += 1;
                result = null;

            }

            if (apexDomain != "" && apexDomain != null) {
                this.writeRecord(apexDomain, recordType, subdomain, ip8, ip16, ip24,
                        parsedIpAddress, country, city, asn, as_name, tld);
            } else {
                System.out.println("ip or apexDomain empty!, ignoring record: <" + ipStr + ", " + apexDomain + ">");
            }
        }

    }

    public void parseAndInsertCSV(String csvString) throws IOException, SkippedEntryProcessingException {
        String[] dataParts = csvString.split("\\,");


        if(dataParts.length<2){
            System.out.println("Ignoring Partial Record: "+csvString);
            return;
        }

        String ipStr = dataParts[0];
        String domain = dataParts[1];

        String Data[] = App.getTLDAndApexDomain(domain);
        String apexDomain = Data[1];
        String tld = Data[0];

        String recordType = "a";
        String country = "";
        String city = "";
        String asn = "";
        String as_name = "";

        // Parse Ip Address and Extract Blocks
        InetAddress parsedIpAddress = null;
        InetAddress ip8 = null;
        InetAddress ip16 = null;
        InetAddress ip24 = null;

        try{
        parsedIpAddress = InetAddress.getByName(ipStr);
        }catch(UnknownHostException e){
            System.out.println("Invalid Ip Address: "+csvString);
            return;
        }

        ip8 = App.getIPBlock(parsedIpAddress, (short) 8);
        ip16 = App.getIPBlock(parsedIpAddress, (short) 16);
        ip24 = App.getIPBlock(parsedIpAddress, (short) 24);

        // Lookup GEO and ASN Details using MMDB;
        LookupResult result = this.MMDBReader.get(parsedIpAddress, LookupResult.class);
        if (result != null) {
            country = result.country;
            asn = result.asn;
            as_name = result.as_name;
        }
        this.lookedUpEntries += 1;
        result = null;

        if (apexDomain != "" && apexDomain != null) {
            this.writeRecord(apexDomain, recordType, domain, ip8, ip16, ip24,
                    parsedIpAddress, country, city, asn, as_name, tld);
        } else {
            System.out.println("ip or apexDomain empty!, ignoring record: <" + ipStr + ", " + apexDomain + ">");
        }

    }

    public void writeRecord(String apexDomain, String recordType, String subDomain, InetAddress ip8, InetAddress ip16,
            InetAddress ip24, InetAddress ipAddress,
            String country,
            String city, String asn, String as_name, String tld)
            throws IOException, SkippedEntryProcessingException {
        try {
            this.writer.addRow(apexDomain, recordType, subDomain, ip8, ip16, ip24, ipAddress, country, city, asn,
                    as_name, tld);
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

    private static InetAddress getIPBlock(InetAddress ipAddress, short prefixLength) {
        byte[] addressBytes = ipAddress.getAddress();
        int mask = (0xFFFFFFFF << (32 - prefixLength)) & 0xFFFFFFFF;

        int networkAddress = (addressBytes[0] & 0xFF) << 24 |
                (addressBytes[1] & 0xFF) << 16 |
                (addressBytes[2] & 0xFF) << 8 |
                (addressBytes[3] & 0xFF);

        int maskedNetworkAddress = networkAddress & mask;

        try {
            byte[] maskedAddressBytes = new byte[] {
                    (byte) ((maskedNetworkAddress >> 24) & 0xFF),
                    (byte) ((maskedNetworkAddress >> 16) & 0xFF),
                    (byte) ((maskedNetworkAddress >> 8) & 0xFF),
                    (byte) (maskedNetworkAddress & 0xFF)
            };
            return InetAddress.getByAddress(maskedAddressBytes);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static String[] getTLDAndApexDomain(String domain) {
        String parts[] = domain.split("\\.");
        int tldIndex = parts.length - 1;
        boolean tldExists = tldIndex > 0;

        if (tldExists) {

            StringBuilder apexDomain = new StringBuilder();

            // abc.co.de }---> already extracted as tld
            // |-------> we must check for second level tld

            if (TLDs.get(parts[tldIndex - 1]) == null) { // not a tld
                apexDomain.append(String.join(".", Arrays.copyOfRange(parts, tldIndex - 1, tldIndex + 1)));
            } else { // is a tld
                int startIndex = tldIndex - 2;
                if (startIndex < 0) {
                    startIndex = 0;
                }
                apexDomain.append(String.join(".", Arrays.copyOfRange(parts, startIndex, tldIndex + 1)));
            }
            apexDomain.append(".");

            return new String[] { parts[tldIndex], apexDomain.toString() };
        }

        return new String[] { "", domain };
    }

}

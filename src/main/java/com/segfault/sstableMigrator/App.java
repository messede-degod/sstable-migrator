package com.segfault.sstableMigrator;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.commons.lang3.ArrayUtils;
import org.json.JSONObject;
import org.json.JSONArray;

import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;

import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.model.CityResponse;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class App {

    CQLSSTableWriter RDNSWriter;
    CQLSSTableWriter CNAMEWriter;
    CQLSSTableWriter SubDomainWriter;

    DatabaseReader MMDBCityReader;
    DatabaseReader MMDBASNReader;

    int processedEntries = 0;
    int processedFiles = 0;
    int lookedUpEntries = 0;
    static InetAddress zeroAddr = null;

    // https://data.iana.org/TLD/tlds-alpha-by-domain.txt
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
        // File directory = new File("input/");

        File[] files = directory.listFiles();
        System.out.println("Found: " + files.length + " files in input directory.");

        for (File file : files) {
            try {
                reader = new BufferedReader(new FileReader(file));

                String line = reader.readLine();
                while (line != null) {
                    csw.parseAndInsertCSV(line);
                    // csw.parseAndInsertJSON(line);
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

        csw.RDNSWriter.close();
        csw.CNAMEWriter.close();
        csw.SubDomainWriter.close();

        csw.MMDBCityReader.close();
        csw.MMDBASNReader.close();

        System.out.println("Writer(s) closed Successfully");
        System.out.println("Processed " + csw.processedEntries + " Entries");
        System.out.println("Processed " + csw.processedFiles + " Files");
        System.out.println("LookedUp " + csw.lookedUpEntries + " Records");

    }

    public App() {
        String keyspace = "ferret";
        String outputDir = "./output/";

        // RDNS table
        String RDNSSchema = "CREATE TABLE ferret.dnsdata ("
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
                + " PRIMARY KEY (ip8,ip16,ip24,ipAddress,tld,apexDomain,subDomain) );";

        String RDNSInsert = "INSERT INTO ferret.dnsdata (apexDomain, recordType, subDomain, ip8, ip16, ip24, ipAddress, country, city, asn, as_name,tld) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String RDNSTable = "dnsdata";
        String RDNSOutputDir = outputDir + RDNSTable + "/";

        // CNAME table
        String CNAMESchema = "CREATE TABLE ferret.cnames ("
                + " target VARCHAR,"
                + " apexDomain VARCHAR,"
                + " domain VARCHAR,"
                + " PRIMARY KEY (target,apexDomain,domain) );";

        String CNAMEInsert = "INSERT INTO ferret.cnames (target,apexDomain,domain) VALUES (?,?,?)";
        String CNAMETable = "cnames";
        String CNAMEOutputDir = outputDir + CNAMETable + "/";

        // Subdomain table
        String SubDomainSchema = "CREATE TABLE ferret.subdomains ("
                + " p1 VARCHAR,"
                + " p2 VARCHAR,"
                + " p3 VARCHAR,"
                + " p4 VARCHAR,"
                + " p5 VARCHAR,"
                + " p6 VARCHAR,"
                + " p7 VARCHAR,"
                + " lastSeen date,"
                + " PRIMARY KEY ((p1,p2,p3),p4,p5,p6,p7) );";

        String SubDomainInsert = "INSERT INTO ferret.subdomains (p1, p2, p3, p4, p5, p6, p7, lastSeen) VALUES (?,?,?,?,?,?,?,toDate(now()))";
        String SubDomainTable = "subdomains";
        String SubDomainOutputDir = outputDir + SubDomainTable + "/";

        File directory = new File(keyspace);
        if (!directory.exists())
            directory.mkdir();

        String[] paths = { RDNSTable, CNAMETable, SubDomainTable };
        for (String path : paths) {
            File filePath = new File(directory, path);
            if (!filePath.exists())
                filePath.mkdir();
        }

        String[] opaths = { RDNSOutputDir, CNAMEOutputDir, SubDomainOutputDir };
        for (String path : opaths) {
            File filePath = new File(path);
            if (!filePath.exists())
                filePath.mkdir();
        }

        this.RDNSWriter = CQLSSTableWriter.builder()
                .withPartitioner(Murmur3Partitioner.instance)
                .inDirectory(RDNSOutputDir)
                .forTable(RDNSSchema)
                .using(RDNSInsert).build();

        this.CNAMEWriter = CQLSSTableWriter.builder()
                .withPartitioner(Murmur3Partitioner.instance)
                .inDirectory(CNAMEOutputDir)
                .forTable(CNAMESchema)
                .using(CNAMEInsert).build();

        this.SubDomainWriter = CQLSSTableWriter.builder()
                .withPartitioner(Murmur3Partitioner.instance)
                .inDirectory(SubDomainOutputDir)
                .forTable(SubDomainSchema)
                .using(SubDomainInsert).build();

        System.out.println("Writer(s) created Successfully");

        File cityDatabase = new File("misc/geocity.mmdb");
        File asnDatabase = new File("misc/geoasn.mmdb");

        try {
            this.MMDBCityReader = new DatabaseReader.Builder(cityDatabase).withCache(new CHMCache(262144)).build();
            this.MMDBASNReader = new DatabaseReader.Builder(asnDatabase).withCache(new CHMCache(262144)).build();
        } catch (IOException e) {
            System.out.println("Error Opening MMDB :: " + e.toString());
        }
    }

    public void parseAndInsertJSON(String jsonString) throws IOException, SkippedEntryProcessingException {
        JSONObject jo = null;
        try {
            jo = new JSONObject(jsonString);
        } catch (Exception e) {
            System.out.println(jsonString + " - " + e.toString());
            return;
        }

        JSONArray answers = jo.getJSONObject("data").getJSONArray("answers");

        for (int i = 0; i < answers.length(); i++) {
            JSONObject ans = answers.getJSONObject(i);

            String subdomain = ans.getString("name");

            ArrayList<Object> Data = App.getDomainParts(subdomain);
            if (Data.get(0).equals(false)) {
                System.out.println("Ignoring Record - getDomainParts failed : " + subdomain);
                return;
            }

            String apexDomain = Data.get(1).toString();
            String tld = Data.get(3).toString();

            String ipStr = ans.getString("data");
            String recordType = ans.getString("type");
            String country = "";
            String city = "";
            String asn = "";
            String as_name = "";

            InetAddress parsedIpAddress = null;
            InetAddress ip8 = null;
            InetAddress ip16 = null;
            InetAddress ip24 = null;

            Boolean isCNAME = !recordType.equals("a");

            if (isCNAME) {
                // Handle CNAME's properly
                recordType = "CNAME";
                parsedIpAddress = App.zeroAddr;
                ip8 = App.zeroAddr;
                ip16 = App.zeroAddr;
                ip24 = App.zeroAddr;
            } else {
                parsedIpAddress = InetAddress.getByName(ipStr);
                recordType = "A";
                ip8 = App.getIPBlock(parsedIpAddress, (short) 8);
                ip16 = App.getIPBlock(parsedIpAddress, (short) 16);
                ip24 = App.getIPBlock(parsedIpAddress, (short) 24);

                // Lookup GEO and ASN Details using MMDB;
                try {
                    CityResponse cityResult = this.MMDBCityReader.city(parsedIpAddress);
                    AsnResponse asnResult = this.MMDBASNReader.asn(parsedIpAddress);

                    if (cityResult != null) {
                        country = cityResult.getCountry().getIsoCode();
                        city = cityResult.getCity().getName();
                    }

                    if (asnResult != null) {
                        asn = asnResult.getAutonomousSystemNumber().toString();
                        as_name = asnResult.getAutonomousSystemOrganization();
                    }

                    this.lookedUpEntries += 1;
                } catch (GeoIp2Exception e) {
                    // System.out.println(ipStr + " - " + e.toString());
                }
            }

            if (apexDomain != "" && apexDomain != null) {

                // if CNAME only add entry to cnames table and not to reverse RDNS or subdomain
                // table
                if (isCNAME) {
                    this.writeCNAMERecord(ipStr, apexDomain, subdomain);
                } else {
                    this.writeRDNSRecord(apexDomain, recordType, subdomain, ip8, ip16, ip24,
                            parsedIpAddress, country, city, asn, as_name, tld);

                    // add a entry to subdomains table
                    this.writeSubDomainRecord(
                            Data.get(2).toString(),
                            Data.get(3).toString(),
                            Data.get(4).toString(),
                            Data.get(5).toString(),
                            Data.get(6).toString(),
                            Data.get(7).toString(),
                            Data.get(8).toString());
                }

            } else {
                System.out.println("ip or apexDomain empty!, ignoring record: <" + ipStr + ", " + apexDomain + ">");
            }

            this.processedEntries++;
        }

    }

    public void parseAndInsertCSV(String csvString) throws IOException, SkippedEntryProcessingException {
        String[] dataParts = csvString.split("\\,");

        // Domain,RecordType,IP

        if (dataParts.length < 3) {
            // System.out.println("Ignoring Partial Record: "+csvString);
            return;
        }

        String ipStr = dataParts[2];
        String recordType = dataParts[1];
        String domain = dataParts[0];

        ArrayList<Object> Data = App.getDomainParts(domain);

        if (Data.get(0).equals(false)) {
            System.out.println("Ignoring Record - getDomainParts failed : " + csvString);
            return;
        }


        String apexDomain = Data.get(1).toString();
        String tld = Data.get(3).toString();

        String country = "";
        String city = "";
        String asn = "";
        String as_name = "";

        // Parse Ip Address and Extract Blocks
        InetAddress parsedIpAddress = null;
        InetAddress ip8 = null;
        InetAddress ip16 = null;
        InetAddress ip24 = null;

        Boolean isCNAME = !recordType.equals("A");

        if (isCNAME) {
            // Handle CNAME's properly
            apexDomain = ipStr;
            recordType = "CNAME";
            parsedIpAddress = App.zeroAddr;
            ip8 = App.zeroAddr;
            ip16 = App.zeroAddr;
            ip24 = App.zeroAddr;
        } else {

            try {
                parsedIpAddress = InetAddress.getByName(ipStr);
            } catch (UnknownHostException e) {
                System.out.println("Invalid Ip Address: " + csvString);
                return;
            }

            ip8 = App.getIPBlock(parsedIpAddress, (short) 8);
            ip16 = App.getIPBlock(parsedIpAddress, (short) 16);
            ip24 = App.getIPBlock(parsedIpAddress, (short) 24);

            // Lookup GEO and ASN Details using MMDB;
            try {
                CityResponse cityResult = this.MMDBCityReader.city(parsedIpAddress);
                AsnResponse asnResult = this.MMDBASNReader.asn(parsedIpAddress);

                if (cityResult != null) {
                    country = cityResult.getCountry().getIsoCode();
                    city = cityResult.getCity().getName();
                }

                if (asnResult != null) {
                    asn = asnResult.getAutonomousSystemNumber().toString();
                    as_name = asnResult.getAutonomousSystemOrganization();
                }

                this.lookedUpEntries += 1;
            } catch (GeoIp2Exception e) {
                // System.out.println(ipStr + " - " + e.toString());
            }
        }

        if (apexDomain != "" && apexDomain != null) {
            // if CNAME only add entry to cnames table and not to reverse RDNS table or
            // subdomain table
            if (isCNAME) {
                this.writeCNAMERecord(ipStr, apexDomain, domain);
            } else {
                this.writeRDNSRecord(apexDomain, recordType, domain, ip8, ip16, ip24,
                        parsedIpAddress, country, city, asn, as_name, tld);

                // add a entry to subdomains table in all cases
                this.writeSubDomainRecord(
                        Data.get(2).toString(),
                        Data.get(3).toString(),
                        Data.get(4).toString(),
                        Data.get(5).toString(),
                        Data.get(6).toString(),
                        Data.get(7).toString(),
                        Data.get(8).toString());
            }
        } else {
            System.out.println("ip or apexDomain empty!, ignoring record: <" + ipStr + ", " + apexDomain + ">");
        }

    }

    public void writeRDNSRecord(String apexDomain, String recordType, String subDomain, InetAddress ip8,
            InetAddress ip16,
            InetAddress ip24, InetAddress ipAddress,
            String country,
            String city, String asn, String as_name, String tld)
            throws IOException, SkippedEntryProcessingException {
        try {
            this.RDNSWriter.addRow(apexDomain, recordType, subDomain, ip8, ip16, ip24, ipAddress, country, city, asn,
                    as_name, tld);
        } catch (InvalidRequestException ie) {
            System.out.println("writeRDNSRecord - InvalidRequestException: faile to write entry <" + apexDomain + ","
                    + recordType + ","
                    + subDomain + "," + ipAddress + ">");
            ie.printStackTrace();
            System.out.println("Continuing to process other entries...");
            throw new SkippedEntryProcessingException("Skipped Entry Processing");
        }
    }

    public void writeCNAMERecord(String apexDomain, String domain, String subDomain)
            throws IOException, SkippedEntryProcessingException {
        try {
            this.CNAMEWriter.addRow(apexDomain, domain, subDomain);
        } catch (InvalidRequestException ie) {
            System.out.println("writeCNAMERecord - InvalidRequestException: faile to write entry <" + apexDomain + ","
                    + domain + ","
                    + subDomain + ">");
            ie.printStackTrace();
            System.out.println("Continuing to process other entries...");
            throw new SkippedEntryProcessingException("Skipped Entry Processing");
        }
    }

    public void writeSubDomainRecord(String label, String tld, String p1, String p2, String p3, String p4, String p5)
            throws IOException, SkippedEntryProcessingException {
        try {
            this.SubDomainWriter.addRow(label, tld, p1, p2, p3, p4, p5);
        } catch (InvalidRequestException ie) {
            System.out.println("InvalidRequestException: faile to write entry <" + label + "," + tld + ">");
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
            // \/----> we must check for second level tld

            if (TLDs.get(parts[tldIndex - 1]) == null) { // not a tld
                apexDomain.append(String.join(".", ArrayUtils.subarray(parts, tldIndex - 1, tldIndex + 1)));
            } else { // is a tld
                int startIndex = tldIndex - 2;
                if (startIndex < 0) {
                    startIndex = 0;
                }
                apexDomain.append(String.join(".", ArrayUtils.subarray(parts, startIndex, tldIndex + 1)));
            }
            apexDomain.append(".");

            return new String[] { parts[tldIndex], apexDomain.toString() };
        }

        return new String[] { "", domain };
    }

    /*
     * abc.bca.abc.co.com
     *          |   |  |-> TLD
     *          |   -----> Level 2 tld
     *          ---------> Label
     */

    public static ArrayList<Object> getDomainParts(String domain) {
        // success,apexDomain,label, tld, p1, p2, p3, p4, p5
        ArrayList<Object> returnData = new ArrayList<Object>(9);

        // split domain to parts sep: .
        String parts[] = domain.split("\\.");
        int tldIndex = parts.length - 1;
        int l2TldIndex = tldIndex;
        int labelIndex = 0;
        boolean tldExists = tldIndex > 0;

        if (!tldExists) {
            returnData.add(false);
            return returnData;
        }

        //
        // output
        StringBuilder apexDomain = new StringBuilder();
        //
        //

        returnData.add(true);

        // If level 2 tld exists
        if (TLDs.get(parts[tldIndex - 1]) != null) {
            l2TldIndex = tldIndex - 1;
        }

        labelIndex = Math.max(Math.min(tldIndex, l2TldIndex) - 1, 0);

        // Extract ApexDomain
        apexDomain.append(String.join(".",
                ArrayUtils.subarray(parts, labelIndex, tldIndex + 1))); // tldIndex + 1 -> since end is exclusive

        returnData.add(apexDomain);

        int maxParts = 7;
        int addedParts = 0;
        int lastPartIndex = Math.max(tldIndex - 5, 0);

        // add tld
        returnData.add(parts[tldIndex]);
        addedParts++;

        // add l2tld or empty string
        if (l2TldIndex != tldIndex) {
            returnData.add(parts[l2TldIndex]);
            returnData.add("");
        } else {
            returnData.add("");
            returnData.add(parts[tldIndex-1]);
        }
        addedParts += 2;

        // add remaining parts
        for (int i = tldIndex - 2; i >= lastPartIndex; i--) {
            addedParts++;
            returnData.add(parts[i]);
        }

        if (lastPartIndex > 0) {
            returnData.add(String.join(".", ArrayUtils.subarray(parts, 0, lastPartIndex)));
            addedParts++;
        }

        // add dummy parts
        for (int j = addedParts; j < maxParts; j++) {
            returnData.add("");
        }

        return returnData;
    }

}

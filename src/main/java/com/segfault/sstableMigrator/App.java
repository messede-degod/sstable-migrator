package com.segfault.sstableMigrator;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.json.JSONObject;
import org.json.JSONArray;

import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;

public class App {

    CQLSSTableWriter writer;
    int processedEntries = 0;
    int processedFiles = 0;

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
        System.out.println("Writer closed Successfully");
        System.out.println("Processed " + csw.processedEntries + " Entries");
        System.out.println("Processed " + csw.processedFiles + " Files");

    }

    public App() {
        String outputDir = "./output/";
        String keyspace = "ferret";
        String table = "dnsdata";

        String schema = "CREATE TABLE ferret.dnsdata ("
                + " apexDomain VARCHAR,"
                + " recordType VARCHAR,"
                + " subDomain VARCHAR,"
                + " ipAddress VARCHAR,"
                + " PRIMARY KEY (ipAddress,apexDomain) )";

        String insert = "INSERT INTO ferret.dnsdata (apexDomain, recordType, subDomain, ipAddress) VALUES (?, ?, ?,?)";

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
    }

    public void parseAndInsert(String jsonString) throws IOException, SkippedEntryProcessingException {
        JSONObject jo = new JSONObject(jsonString);

        String apexDomain = jo.getString("name");
        JSONArray answers = jo.getJSONObject("data").getJSONArray("answers");

        for (int i = 0; i < answers.length(); i++) {
            JSONObject ans = answers.getJSONObject(i);

            String subdomain = ans.getString("name");
            String ip = ans.getString("data");
            String recordType = ans.getString("type");

            if (ip != "" && ip != null && apexDomain != "" && apexDomain != null) {
                this.writeRecord(apexDomain, recordType, subdomain, ip);
            } else {
                System.out.println("ip or apexDomain empty!, ignoring record: <" + ip + ", " + apexDomain + ">");
            }
        }

    }

    public void writeRecord(String apexDomain, String recordType, String subDomain, String ipAddress)
            throws IOException, SkippedEntryProcessingException {
        try {
            this.writer.addRow(apexDomain, recordType, subDomain, ipAddress);
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

}
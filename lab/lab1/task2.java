/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hr.fer.tel.rovkp.lab1.zad2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;

/**
 *
 * @author fhrenic
 */
public class Test {

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Expecting 2 arguments: <path-to-local-folder> <hdfs-output-file>");
            return;
        }

        Path directory = Paths.get(args[0]);
        org.apache.hadoop.fs.Path hdfsFile = new org.apache.hadoop.fs.Path(args[1]);

        if (!Files.isDirectory(directory)) {
            System.err.println("Local file isn't a directory");
            return;
        }

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://cloudera2:8020");
        LocalFileSystem local = LocalFileSystem.getLocal(configuration);
        FileSystem hdfs = FileSystem.get(configuration);

        if (hdfs.exists(hdfsFile)) {
            System.err.println("HDFS file already exists");
            return;
        }
        hdfs.createNewFile(hdfsFile);

        OutputStream os = hdfs.create(hdfsFile);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os));
        Copier copier = new Copier(writer);

        long startTime = System.nanoTime();
        Files.walkFileTree(directory, copier);
        long endTime = System.nanoTime();

        long elapsed = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
        System.out.println("Copied " + copier.numLines + " lines");
        System.out.println("Copied " + copier.numFiles + " files");
        System.out.println("Time elapsed: " + elapsed + " ms");
    }

    private static class Copier extends SimpleFileVisitor<Path> {

        private BufferedWriter writer;
        private long numLines;
        private long numFiles;

        Copier(BufferedWriter writer) {
            this.writer = writer;
            numLines = 0;
            numFiles = 0;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {

            BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.ISO_8859_1);

            while (true) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                writer.write(line);
                numLines++;
            }
            writer.flush();

            numFiles++;

            return FileVisitResult.CONTINUE;
        }
    }
}

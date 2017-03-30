/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hr.fer.tel.rovkp.lab1.zad3;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.VIntWritable;

/**
 *
 * @author fhrenic
 */
public class Test {

    public static void main(String[] args) throws IOException {

        if (args.length != 1) {
            System.err.println("Expecting path to file which will be created");
            return;
        }

        Path sensorReadings = new Path(args[0]);
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://cloudera2:8020");

        if (FileSystem.get(conf).exists(sensorReadings)) {
            System.err.println("Given file already exists");
            return;
        }

        Writer writer = SequenceFile.createWriter(
                conf,
                Writer.file(sensorReadings),
                Writer.keyClass(VIntWritable.class),
                Writer.valueClass(FloatWritable.class)
        );

        VIntWritable key = new VIntWritable();
        FloatWritable value = new FloatWritable();
        for (int i = 0; i < NUM_READINGS; i++) {
            key.set(sensorID());
            value.set(reading());
            writer.append(key, value);
        }
        writer.close();

        Reader reader = new Reader(
                conf,
                Reader.file(sensorReadings)
        );

        int[] count = new int[NUM_SENSORS];
        float[] sum = new float[NUM_SENSORS];
        for (int i = 0; i < NUM_SENSORS; i++) {
            count[i] = 0;
            sum[i] = 0f;
        }

        while (reader.next(key, value)) {
            count[key.get()]++;
            sum[key.get()] += value.get();
        }

        for (int i = 0; i < NUM_SENSORS; i++) {
            float avg = count[i] == 0 ? 0 : sum[i] / count[i];
            System.out.printf("Senzor %d: %.2f%n", i + 1, avg);
        }

    }

    private static final Random RANDOM = ThreadLocalRandom.current();

    private static final int NUM_SENSORS = 100;
    private static final int NUM_READINGS = 100000;

    private static int sensorID() {
        return RANDOM.nextInt(NUM_SENSORS);
    }

    private static float reading() {
        return RANDOM.nextFloat() * 100;
    }

}

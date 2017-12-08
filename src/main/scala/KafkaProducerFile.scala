package com.kal.kafkaproducer;
/** 
 * Transactions Simulator and Producer
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducerFile
{
    private static String kafkaTopic;

    private static void run(String[] args)
    {
        kafkaTopic = args[0]; // args-  Transactions 3000
        
        Properties properties = new Properties();
        properties.put("metadata.broker.list", "192.168.162.161:6667");
        properties.put("partitioner.class", "com.kal.kafkaproducer.SimplePartitioner");
        properties.put("producer.type", "sync");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("client.id", "camus");
        System.out.println("Topic :: " + kafkaTopic);

        ProducerConfig producerConfig = new ProducerConfig(properties);
        kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);

        int count = 1;

        while (true)
        {

            try
                    
            {
                BufferedReader br = new BufferedReader(
                       new FileReader(new File("/root/IBMzSystems/IBMdataSuper.csv")));
                    
                SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd");
                SimpleDateFormat time = new SimpleDateFormat("HH:mm:ss");
                String line;
                int totalRecords = 600;
                String[] records = new String[totalRecords];
                int i = 0;
                while ((line = br.readLine()) != null)
                {
                    records[i++] = line;
                }
                while (true)
                {
                    String record = records[new Random().nextInt(totalRecords)];
                    String[] fields = record.split(",");

                    String trx = fields[0] + "," + fields[1] + "," + fields[2] + "," + fields[3] + "," + fields[4] + "," + date.format(new Date())
                            + " " + time.format(new Date()) + "," + fields[6] + "," + fields[7] + "," + fields[8] + "," + fields[9] + "," + fields[10]
                            + "," + Math.round(getRandomFloat(18.0f, 9999.0f) * 100.0) / 100.0 + "," + fields[12] + "," + fields[13] + ","
                            + fields[14] + "," + fields[15] + "," + fields[16];
                    KeyedMessage<String, String> message = new KeyedMessage<String, String>(kafkaTopic, trx);
                    producer.send(message);
                    System.out.println("msg:" + trx);
                    System.out.println("Total Produced messages: " + count++);
                    try
                    {
                        Thread.sleep(new Integer(args[1]));
                    } catch (InterruptedException e)
                    {
                        e.printStackTrace();
                    }
                }

            } catch (IOException e1)
            {
                e1.printStackTrace();
            }
        }
        
    }

    private static float getRandomFloat(float minX, float maxX)
    {
        Random rand = new Random();
        float finalX = minX + rand.nextFloat() / 3 * (maxX - minX + 1);
        return finalX;
    }

    public static void main(String[] args)
    {
        KafkaProducerFile.run(args);
    }

}

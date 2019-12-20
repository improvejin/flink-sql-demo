package com.jjt.flink.demo.jobs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Time;
import java.util.Random;

/**
 * Created by mustafa on 01/07/2017.
 */
public class SQLTester {
    private static void listenAndGenerateNumbers(int port) {
        try {
            ServerSocket serverSocket = new ServerSocket(port);
            Socket clientSocket = serverSocket.accept();
            System.out.println("Accepted connection");

            Random random = new Random();
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
            String rooms[] = new String[]{"living room", "kitchen", "outside", "bedroom", "attic"};

            for (int i = 0; i < 10000; i++) {
                String room = rooms[random.nextInt(rooms.length)];
                double temp = random.nextDouble() * 30 + 20;

                out.println(room + "," + temp);
                Thread.sleep(random.nextInt(10) + 50);
            }

            System.out.println("Closing server");
            clientSocket.close();
            serverSocket.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static final MapFunction<String, Tuple3<String, Double, Time>> mapFunction
            = new MapFunction<String, Tuple3<String, Double, Time>>() {

        @Override
        public Tuple3<String, Double, Time> map(String s) throws Exception {
            // data is: <roomname>,<temperature>
            String p[] = s.split(",");
            String room = p[0];
            Double temperature = Double.parseDouble(p[1]);
            Time creationDate = new Time(System.currentTimeMillis());
            return new Tuple3<>(room, temperature, creationDate);
        }
    };

    private final static AscendingTimestampExtractor extractor = new AscendingTimestampExtractor<Tuple3<String, Double, Time>>() {
        @Override
        public long extractAscendingTimestamp(Tuple3<String, Double, Time> element) {
            return element.f2.getTime();
        }
    };

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final int port = 3901;
        new Thread(() -> listenAndGenerateNumbers(port)).start();
        Thread.sleep(1000); // wait the socket for a little;

        DataStream<String> text = env.socketTextStream("localhost", port, "\n");
        DataStream<Tuple3<String, Double, Time>> dataset = text
                .map(mapFunction)
                .assignTimestampsAndWatermarks(extractor);

        // Register it so we can use it in SQL
        tableEnv.registerDataStream("sensors", dataset, "room, temperature, creationDate, rowtime.rowtime");

        String query = "SELECT room, TUMBLE_END(rowtime, INTERVAL '10' SECOND), AVG(temperature) AS avgTemp FROM sensors GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND), room";
        Table table = tableEnv.sqlQuery(query);

        // Just for printing purposes, in reality you would need something other than Row
        tableEnv.toAppendStream(table, Row.class).print();

        env.execute();
    }
}
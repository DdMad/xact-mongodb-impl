import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;

/**
 * Created by ddmad on 11/11/16.
 */
public class Processor {
    public static void main(String[] args) throws IOException {
        // For compute performance
        long count = 0;
        long start = System.currentTimeMillis();

        long[] xactCount = new long[7];
        long[] xactTime = new long[7];

        if (args[0].toLowerCase().equals("d8")) {
            try {
                XactProcessor processor = new XactProcessor(System.getProperty("user.dir") + "/d8-xact/" + args[1], "d8");
                count = processor.processXact(xactCount, xactTime);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if (args[0].toLowerCase().equals("d40")) {
            try {
                XactProcessor processor = new XactProcessor(System.getProperty("user.dir") + "/d40-xact/" + args[1], "d40");
                count = processor.processXact(xactCount, xactTime);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("Argument Error");
        }

        long end = System.currentTimeMillis();
        long duration = end - start;

        if (args[0].toLowerCase().equals("d8")) {
            try {
                System.setErr(new PrintStream(System.getProperty("user.dir") + "/d8-xact/" + "result-d8-" + args[1] + ".out"));
                System.err.println("Total transactions processed: " + count);
                System.err.println("Total time (second) used: " + duration/1000);
                System.err.println("Transaction throughput (xact per second): " + ((double) count)/((double) duration) * 1000);

                System.out.println("Total transactions processed: " + count);
                System.out.println("Total time (second) used: " + duration/1000);
                System.out.println("Transaction throughput (xact per second): " + ((double) count)/((double) duration) * 1000);
                for (int i = 0; i < 7; i++) {
                    System.out.println((i + 1) + " transactions processed: " + xactCount[i]);
                    System.out.println("Subtotal time (second) used: " + xactTime[i]/1000);
                    System.out.println((i + 1) + " transaction throughput (xact per second): " + ((double) xactCount[i])/((double) xactTime[i]) * 1000);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        } else if (args[0].toLowerCase().equals("d40")) {
            try {
                System.setErr(new PrintStream(System.getProperty("user.dir") + "/d40-xact/" + "result-d40-" + args[1] + ".out"));
                System.err.println("Total transactions processed: " + count);
                System.err.println("Total time (second) used: " + duration/1000);
                System.err.println("Transaction throughput (xact per second): " + ((double) count)/((double) duration) * 1000);

                System.out.println("Total transactions processed: " + count);
                System.out.println("Total time (second) used: " + duration/1000);
                System.out.println("Transaction throughput (xact per second): " + ((double) count)/((double) duration) * 1000);
                for (int i = 0; i < 7; i++) {
                    System.out.println((i + 1) + " transactions processed: " + xactCount[i]);
                    System.out.println("Subtotal time (second) used: " + xactTime[i]/1000);
                    System.out.println((i + 1) + " transaction throughput (xact per second): " + ((double) xactCount[i])/((double) xactTime[i]) * 1000);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("Error");
        }
    }
}

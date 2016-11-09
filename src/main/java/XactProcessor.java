import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by ddmad on 9/11/16.
 */
public class XactProcessor {
    private String xactFileDir;
    private BufferedReader br;
    private BufferedWriter bw;

    private Logger logger;

    private MongoDatabase db;

    public XactProcessor(String dir, String dbName) {
        logger = Logger.getLogger(XactProcessor.class.getName());
        String log4JPropertyFile = System.getProperty("user.dir") + "/log4j.properties";
        Properties p = new Properties();
        try {
            p.load(new FileInputStream(log4JPropertyFile));
            PropertyConfigurator.configure(p);
            logger.info("Log file is configured!");
        } catch (IOException e) {
            logger.error("Log file is not configured!");
        }

        xactFileDir = dir;
        MongoClient mongoClient = new MongoClient();
        db = mongoClient.getDatabase(dbName);
    }

    public long processXact(long[] xactCount, long[] xactTime) throws IOException {
        long count = 0;

        br = new BufferedReader(new FileReader(xactFileDir));

        File output = new File(xactFileDir + "-out.txt");
        if (!output.exists()) {
            output.createNewFile();
        }
        bw = new BufferedWriter(new FileWriter(output));

        String dataLine = br.readLine();
        while (dataLine != null) {
            String[] data = dataLine.split(",");
            String type = data[0];
            if (type.equals("N")) {
                long start = System.currentTimeMillis();

                processNewOrderXact(data);

                long end = System.currentTimeMillis();
                long duration = end - start;
                xactCount[0]++;
                xactTime[0] += duration;
            } else if (type.equals("P")) {
                long start = System.currentTimeMillis();

                processPaymentXact(data);

                long end = System.currentTimeMillis();
                long duration = end - start;
                xactCount[1]++;
                xactTime[1] += duration;
            } else if (type.equals("D")) {
                long start = System.currentTimeMillis();

                processDeliveryXact(data);

                long end = System.currentTimeMillis();
                long duration = end - start;
                xactCount[2]++;
                xactTime[2] += duration;
            } else if (type.equals("O")) {
                long start = System.currentTimeMillis();

                processOrderStatusXact(data);

                long end = System.currentTimeMillis();
                long duration = end - start;
                xactCount[3]++;
                xactTime[3] += duration;
            } else if (type.equals("S")) {
                long start = System.currentTimeMillis();

                processStockLevelXact(data);

                long end = System.currentTimeMillis();
                long duration = end - start;
                xactCount[4]++;
                xactTime[4] += duration;
            } else if (type.equals("I")) {
                long start = System.currentTimeMillis();

                processPopularItemXact(data);

                long end = System.currentTimeMillis();
                long duration = end - start;
                xactCount[5]++;
                xactTime[5] += duration;
            } else if (type.equals("T")) {
                long start = System.currentTimeMillis();

                processTopBalanceXact();

                long end = System.currentTimeMillis();
                long duration = end - start;
                xactCount[6]++;
                xactTime[6] += duration;
            } else {
                logger.warn("Wrong transaction type!");
            }

            logger.info("Finish processing transaction: " + dataLine);

            count++;
            dataLine = br.readLine();
        }

        return count;
    }

    private void processTopBalanceXact() {
    }

    private void processPopularItemXact(String[] data) {
    }

    private void processStockLevelXact(String[] data) {
    }

    private void processOrderStatusXact(String[] data) {
    }

    private void processDeliveryXact(String[] data) {
    }

    private void processPaymentXact(String[] data) {
    }

    private void processNewOrderXact(String[] data) {
        String cId = data[1];
        String wId = data[2];
        String dId = data[3];
        int m = Integer.parseInt(data[4]);
        double totalAmount = 0;
        int oAllLocal = 1;

        List<String> itemNumberList = new ArrayList<String>();
        List<String> iNameList = new ArrayList<String>();
        List<String> supplyWarehouseList = new ArrayList<String>();
        List<String> quantityList = new ArrayList<String>();
        List<String> olAmountList = new ArrayList<String>();
        List<String> sQuantityList = new ArrayList<String>();
    }

    public static void main(String[] args) {
        XactProcessor processor = new XactProcessor("/d8-data", "d8_1");
    }
}

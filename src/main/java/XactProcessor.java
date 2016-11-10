import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import com.sun.javadoc.Doc;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.bson.Document;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by ddmad on 9/11/16.
 */
public class XactProcessor {
    private static final String COLLECTION_CUSTOMER = "customer";
    private static final String COLLECTION_CUSTOMER_STATIC = "customer_static";
    private static final String COLLECTION_CUSTOMER_UNUSED = "customer_unused";
    private static final String COLLECTION_DISTRICT = "district";
    private static final String COLLECTION_DISTRICT_STATIC = "district_static";
    private static final String COLLECTION_DISTRICT_NEXT_O_ID = "district_next_o_id";
    private static final String COLLECTION_ITEM = "item";
    private static final String COLLECTION_ITEM_UNUSED = "item_unused";
    private static final String COLLECTION_ORDER_LINE_UNUSED = "orderLine_unused";
    private static final String COLLECTION_ORDER = "orders";
    private static final String COLLECTION_STOCK = "stock";
    private static final String COLLECTION_STOCK_STATIC = "stock_static";
    private static final String COLLECTION_STOCK_UNUSED = "stock_unused";
    private static final String COLLECTION_WAREHOUSE = "warehouse";
    private static final String COLLECTION_WAREHOUSE_STATIC = "warehouse_static";

    private String xactFileDir;
    private BufferedReader br;
    private BufferedWriter bw;

    private Logger logger;

    private MongoDatabase db;

    private MongoCollection<Document> warehouseCollection;
    private MongoCollection<Document> warehouseStaticCollection;
    private MongoCollection<Document> districtCollection;
    private MongoCollection<Document> districtStaticCollection;
    private MongoCollection<Document> districtNextOIdCollection;
    private MongoCollection<Document> customerCollection;
    private MongoCollection<Document> customerStaticCollection;
    private MongoCollection<Document> customerUnusedCollection;
    private MongoCollection<Document> orderCollection;
    private MongoCollection<Document> orderLineUnusedCollection;
    private MongoCollection<Document> itemCollection;
    private MongoCollection<Document> itemUnusedCollection;
    private MongoCollection<Document> stockCollection;
    private MongoCollection<Document> stockStaticCollection;
    private MongoCollection<Document> stockUnusedCollection;

    public XactProcessor(String dir, String dbName) throws IOException {
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

        warehouseCollection = db.getCollection(COLLECTION_WAREHOUSE);
        warehouseStaticCollection = db.getCollection(COLLECTION_WAREHOUSE_STATIC);
        districtCollection = db.getCollection(COLLECTION_DISTRICT);
        districtStaticCollection = db.getCollection(COLLECTION_DISTRICT_STATIC);
        districtNextOIdCollection = db.getCollection(COLLECTION_DISTRICT_NEXT_O_ID);
        customerCollection = db.getCollection(COLLECTION_CUSTOMER);
        customerStaticCollection = db.getCollection(COLLECTION_CUSTOMER_STATIC);
        customerUnusedCollection = db.getCollection(COLLECTION_CUSTOMER_UNUSED);
        orderCollection = db.getCollection(COLLECTION_ORDER);
        orderLineUnusedCollection = db.getCollection(COLLECTION_ORDER_LINE_UNUSED);
        itemCollection = db.getCollection(COLLECTION_ITEM);
        itemUnusedCollection = db.getCollection(COLLECTION_ITEM_UNUSED);
        stockCollection = db.getCollection(COLLECTION_STOCK);
        stockStaticCollection = db.getCollection(COLLECTION_STOCK_STATIC);
        stockUnusedCollection = db.getCollection(COLLECTION_STOCK_UNUSED);

        File output = new File(xactFileDir + "-out.txt");
        if (!output.exists()) {
            output.createNewFile();
        }
        bw = new BufferedWriter(new FileWriter(output));
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
        // Get top 10 customers
        MongoCursor<Document> top10Customers = customerCollection.find().limit(10).iterator();
    }

    private void processPopularItemXact(String[] data) {
        String wId = data[1];
        String dId = data[2];
        int lastOrderAmount = Integer.parseInt(data[3]);

        long wdId = Long.parseLong(wId);
        wdId <<= 4;
        wdId += Long.parseLong(dId);

        // Get d_next_o_id
        int dNextOId = districtNextOIdCollection.find(Filters.eq("_id", wdId)).first().get("d_next_o_id", Integer.class);

        long wdoId = wdId;
        wdoId <<= 24;
        wdoId += dNextOId;

        // Get L orders
        MongoCursor<Document> lastLOrders = orderCollection.find(new Document("$gte", new Document("_id", wdoId - lastOrderAmount)).append("$lt", new Document("_id", wdoId))).iterator();
        List<Document> lastLOrderList = new ArrayList<Document>();
        while (lastLOrders.hasNext()) {
            lastLOrderList.add(lastLOrders.next());
        }

        // Calculate popular item percentage
        Map<String, Double> popularity = new HashMap<String, Double>();

        for (Document o1 : lastLOrderList) {
            String iName = o1.get("o_popular_i_name", String.class);
            if (popularity.containsKey(iName)) {
                continue;
            } else {
                popularity.put(iName, 0.0);
            }

            for (Document o2: lastLOrderList) {
                boolean find = false;
                List<Document> olList = o2.get("ol_list", List.class);

                for (Document ol : olList) {
                    String s = ol.get("ol_item", Document.class).get("i_name", String.class);

                    if (s.equals(iName)) {
                        find = true;
                        break;
                    }
                }

                if (find) {
                    popularity.put(iName, popularity.get(iName) + 1.0);
                }
            }
        }
    }

    private void processStockLevelXact(String[] data) {
        String wId = data[1];
        String dId = data[2];
        double stockThreshold = Double.parseDouble(data[3]);
        int lastOrderAmount = Integer.parseInt(data[4]);

        long wdId = Long.parseLong(wId);
        wdId <<= 4;
        wdId += Long.parseLong(dId);

        // Get d_next_o_id
        int dNextOId = districtNextOIdCollection.find(Filters.eq("_id", wdId)).first().get("d_next_o_id", Integer.class);

        long wdoId = wdId;
        wdoId <<= 24;
        wdoId += dNextOId;

        // Get L orders
        MongoCursor<Document> lastLOrders = orderCollection.find(new Document("$gte", new Document("_id", wdoId - lastOrderAmount)).append("$lt", new Document("_id", wdoId))).iterator();

        // Calculate total number of items that has less stock
        int count = 0;

        while (lastLOrders.hasNext()) {
            Document order = lastLOrders.next();
            List<Document> olList = order.get("ol_list", List.class);

            for (Document ol : olList) {
                // Get stock
                long wiId = Long.parseLong(wId);
                wiId <<= 17;
                wiId += ol.get("ol_item", Document.class).get("i_id", Integer.class);
                double sQuantity = stockCollection.find(Filters.eq("_id", wiId)).first().get("s_quantity", Double.class);

                if (sQuantity < stockThreshold) {
                    count++;
                }
            }
        }
    }

    private void processOrderStatusXact(String[] data) {
        String wId = data[1];
        String dId = data[2];
        String cId = data[3];
    }

    private void processDeliveryXact(String[] data) {
        String wId = data[1];
        int carrierId = Integer.parseInt(data[2]);

        List<WriteModel<Document>> districtUpdates = new ArrayList<WriteModel<Document>>();
        List<WriteModel<Document>> orderUpdates = new ArrayList<WriteModel<Document>>();
        List<WriteModel<Document>> customerUpdates = new ArrayList<WriteModel<Document>>();

        for (int i = 1; i <= 10; i++) {
            long wdId = Long.parseLong(wId);
            wdId <<= 4;
            wdId += i;

            List<Document> districtList = districtCollection.find(new Document("_id", wdId)).projection(new Document("d_o_to_c_list", new Document("$slice", 1))).into(new ArrayList<Document>());
            if (!districtList.isEmpty()) {
                Document district = districtList.get(0);
                long wdoId = wdId;
                wdoId <<= 24;
                wdoId += (long)((Document)district.get("d_o_to_c_list", List.class).get(0)).get("o_id", Integer.class);

                long wdcId = wdId;
                wdcId <<= 21;
                wdcId += (long)((Document)district.get("d_o_to_c_list", List.class).get(0)).get("c_id", Integer.class);

                // Get order
                Document order = orderCollection.find(Filters.eq("_id", wdoId)).first();
                int m = order.get("o_ol_cnt", Integer.class);
                double totalAmount = order.get("o_total_amount", Double.class);

                // Add order update
                orderUpdates.add(new UpdateOneModel<Document>(new Document("_id", wdoId), new Document("$set", new Document("o_carrier_id", carrierId))));
                String olDeliveryD = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(Calendar.getInstance());
                for (int j = 0; j < m; j++) {
                    orderUpdates.add(new UpdateOneModel<Document>(new Document("_id", wdoId), new Document("$set", new Document("ol_list."+j+"ol_delivery_d", olDeliveryD))));
                }

                // Add customer update
                customerUpdates.add(new UpdateOneModel<Document>(new Document("_id", wdcId), new Document("$inc", new Document("c_balance", totalAmount).append("c_delivery_cnt", 1))));

                // Add district update
                districtUpdates.add(new UpdateOneModel<Document>(new Document("_id", wdId), new Document("$pop", new Document("d_o_to_c_list", -1))));
            }
        }

        districtCollection.bulkWrite(districtUpdates, new BulkWriteOptions().ordered(false));
    }

    private void processPaymentXact(String[] data) {
        String wId = data[1];
        String dId = data[2];
        String cId = data[3];
        double payment = Double.parseDouble(data[4]);

        long wdId = Long.parseLong(wId);
        wdId <<= 4;
        wdId += Long.parseLong(dId);

        long wdcId = wdId;
        wdcId <<= 21;
        wdcId += Long.parseLong(cId);

        // Update w_ytd
        warehouseCollection.updateOne(Filters.eq("_id", Integer.parseInt(wId)), new Document("$inc", new Document("w_ytd", payment)));

        // Update d_ytd
        districtCollection.updateOne(Filters.eq("_id", wdId), new Document("$inc", new Document("d_ytd", payment)));

        // Update customer
        customerCollection.updateOne(Filters.eq("_id", wdcId), new Document("$inc", new Document("c_balance", -payment)
                .append("c_ytd_payment", payment)
                .append("c_payment_cnt", 1)
        ));
    }

    private void processNewOrderXact(String[] data) throws IOException {
        String cId = data[1];
        String wId = data[2];
        String dId = data[3];
        int m = Integer.parseInt(data[4]);
        int oAllLocal = 1;

        // Store output info
        ArrayList<String> itemNumberList = new ArrayList<>();
        ArrayList<String> iNameList = new ArrayList<>();
        ArrayList<String> supplyWarehouseList = new ArrayList<>();
        ArrayList<String> quantityList = new ArrayList<>();
        ArrayList<String> olAmountList = new ArrayList<>();
        ArrayList<String> sQuantityList = new ArrayList<>();

        long wdId = Long.parseLong(wId);
        wdId <<= 4;
        wdId += Long.parseLong(dId);

        // Get and update d_next_o_id
        int dNextOId = districtNextOIdCollection.findOneAndUpdate(Filters.eq("_id", wdId), new Document("$inc", new Document("d_next_o_id", 1))).get("d_next_o_id", Integer.class);

        long wdoId = wdId;
        wdoId <<= 24;
        wdoId += dNextOId;

        long wdcId = wdId;
        wdcId <<= 21;
        wdcId += Long.parseLong(cId);

        List<Document> orderLineList = new ArrayList<Document>();
        List<Document> orderLineUnusedList = new ArrayList<Document>();

        List<WriteModel<Document>> stockUpdates = new ArrayList<WriteModel<Document>>();

        List<String> popularIName = new ArrayList<String>();
        double maxQuantity = 0;
        double oTotalAmount = 0;

        for (int i = 1; i <= m; i++) {
            String[] itemData = br.readLine().split(",");
            String olIId = itemData[0];
            String olSupplyWId = itemData[1];
            String olQuantity = itemData[2];

            oAllLocal = olSupplyWId.equals(wId) ? 1 : 0;
            int iIdInt = Integer.parseInt(olIId);
            double olQuantityDouble = Double.parseDouble(olQuantity);

            // Get i_price
            Document item = itemCollection.find(Filters.eq("_id", iIdInt)).first();
            double iPrice = item.get("i_price", Double.class);
            String iName = item.get("i_name", String.class);

            // Calculate ol_amount
            double olAmount = olQuantityDouble * iPrice;

            // Calculate order total amount
            oTotalAmount += olAmount;

            // Get stock static info
            long wiId = Long.parseLong(olSupplyWId);
            wiId <<= 17;
            wiId += iIdInt;
            Document stockStatic = stockStaticCollection.find(Filters.eq("_id", wiId)).first();
            String stockInfo = stockStatic.get("s_dist_" + dId, String.class);

            // Get stock info
            Document stock = stockCollection.find(Filters.eq("_id", wiId)).first();
            double adjustedQty = stock.get("s_quantity", Double.class) - olQuantityDouble;
            adjustedQty = adjustedQty < 10 ? adjustedQty + 100 : adjustedQty;

            // Add stock update
            stockUpdates.add(new UpdateOneModel<Document>(new Document("_id", wiId), new Document("$set", new Document("s_quantity", adjustedQty))));
            stockUpdates.add(new UpdateOneModel<Document>(new Document("_id", wiId), new Document("$inc", new Document("s_ytd", olQuantityDouble).append("s_order_cnt", 1))));
            if (!wId.equals(olSupplyWId)) {
                stockUpdates.add(new UpdateOneModel<Document>(new Document("_id", wiId), new Document("$inc", new Document("s_remote_cnt", 1))));
            }

            // Calculate popular
            double quantity = Double.parseDouble(olQuantity);
            if (quantity > maxQuantity) {
                maxQuantity = quantity;
                popularIName.clear();
                popularIName.add(item.get("i_name", String.class));
            } else if (quantity == maxQuantity) {
                popularIName.add(item.get("i_name", String.class));
            }

            // Add to orderLineUnusedList
            long wdoolId = wdoId;
            wdoolId <<= 5;
            wdoolId += i;
            orderLineUnusedList.add(DocCreator.createOrderLineUnusedDoc(wdoolId, stockInfo));

            // Add to orderLineList
            orderLineList.add(DocCreator.createOrderLineDoc(Integer.toString(i),
                    DocCreator.createItemDoc(olIId, item.get("i_name", String.class), iPrice, item.get("i_im_id", Integer.class)),
                    null,
                    Double.toString(olAmount),
                    olSupplyWId,
                    olQuantity
                ));

            // Add output info
            itemNumberList.add(olIId);
            iNameList.add(iName);
            supplyWarehouseList.add(olSupplyWId);
            quantityList.add(olQuantity);
            olAmountList.add(Double.toString(olAmount));
            sQuantityList.add(Double.toString(adjustedQty));
        }

        // Insert to orderLineUnusedCollection
        orderLineUnusedCollection.insertMany(orderLineUnusedList, new InsertManyOptions().ordered(false));

        // Insert to orderCollection
        String oEntryD = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(Calendar.getInstance());
        orderCollection.insertOne(DocCreator.createOrderWithOrderLinePopularTotalAmountDoc(wdoId, cId, null, Integer.toString(m), Integer.toString(oAllLocal), oEntryD, orderLineList, popularIName, maxQuantity, oTotalAmount));

        // Update stock
        stockCollection.bulkWrite(stockUpdates, new BulkWriteOptions().ordered(false));

        // Update district
        List<Document> list = new ArrayList<Document>();
        list.add(new Document("o_id", dNextOId).append("c_id", Integer.parseInt(cId)));
        districtCollection.updateOne(Filters.eq("_id", wdId),
                new Document("$push",
                        new Document("d_o_to_c_list",
                                new Document("$each", list).append("$sort", new Document("o_id", 1)))));

        /**************** Output ****************/
        // Get static
        Document customerStatic = customerStaticCollection.find(new Document("_id", wdcId)).first();
        Document warehouseStatic = warehouseStaticCollection.find(new Document("_id", Integer.parseInt(wId))).first();
        Document districtStatic = districtCollection.find(new Document("_id", wdId)).first();

        bw.write(String.format("%s,%s,%s,%s,%s,%s", wId, dId, cId, customerStatic.get("c_last", String.class), customerStatic.get("c_credit", String.class), Double.toString(customerStatic.get("c_discount", Double.class))));
        bw.newLine();
        bw.write(String.format("%s,%s", Double.toString(warehouseStatic.get("w_tax", Double.class)), Double.toString(districtStatic.get("d_tax", Double.class))));
        bw.newLine();
        bw.write(String.format("%d,%s", dNextOId, oEntryD));
        bw.newLine();
        bw.write(String.format("%d,%s", m, Double.toString(oTotalAmount * (1 + warehouseStatic.get("w_tax", Double.class) + districtStatic.get("d_tax", Double.class)) * (1 - customerStatic.get("c_discount", Double.class)))));
        bw.newLine();
        for (int i = 0; i < itemNumberList.size(); i++) {
            bw.write(String.format("%s,%s,%s,%s,%s,%s", itemNumberList.get(i), iNameList.get(i), sQuantityList.get(i), quantityList.get(i), olAmountList.get(i), sQuantityList.get(i)));
            bw.newLine();
        }
        bw.flush();
    }

    public static void main(String[] args) {
        XactProcessor processor = new XactProcessor("/d8-data", "d8_1");
    }
}

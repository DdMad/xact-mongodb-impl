import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.bson.Document;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.*;

import java.util.*;
import java.util.stream.Stream;

/**
 * Created by ddmad on 7/11/16.
 */
public class DatabaseBuilder {
    private static final String DATA_CUSTOMER = "customer.csv";
    private static final String DATA_DISTRICT = "district.csv";
    private static final String DATA_ITEM = "item.csv";
    private static final String DATA_ORDER_LINE = "order-line.csv";
    private static final String DATA_ORDER = "order.csv";
    private static final String DATA_STOCK = "stock.csv";
    private static final String DATA_WAREHOUSE = "warehouse.csv";

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

    private static final int INSERT_THRESHOLD = 1000;

    private String dataFileDir;

    private MongoDatabase db;

    private Logger logger;

    public DatabaseBuilder(String dir, String dbName) {
        setupLogger();

        dataFileDir = dir;

        MongoClient mongoClient = new MongoClient();
        db = mongoClient.getDatabase(dbName);
    }

    public void loadData() throws IOException {
        Path dir = FileSystems.getDefault().getPath(dataFileDir);
        DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.csv");

        Path orderPath = null;
        Path orderLinePath = null;

        for (Path path : stream) {
            String name = path.getFileName().toString();
            logger.info("Process file: " + name);

            if (name.equals(DATA_CUSTOMER)) {
                loadCustomerData(path);
            } else if (name.equals(DATA_DISTRICT)) {
                loadDistrictData(path);
            } else if (name.equals(DATA_ITEM)) {
                loadItemData(path);
            } else if (name.equals(DATA_ORDER)) {
                orderPath = path;
            } else if (name.equals(DATA_ORDER_LINE)) {
                orderLinePath = path;
            } else if (name.equals(DATA_STOCK)) {
                loadStockData(path);
            } else if (name.equals(DATA_WAREHOUSE)) {
                loadWarehouseData(path);
            } else {
                logger.warn("Wrong data file: " + name + "!");
            }
        }

        if (orderPath != null) {
            loadOrderData(orderPath);
        }

        if (orderLinePath != null) {
            loadOrderLineData(orderLinePath);
        }

        postProcess();
    }

    private void setupLogger() {
        logger = Logger.getLogger(DatabaseBuilder.class.getName());
        String log4JPropertyFile = System.getProperty("user.dir") + "/log4j.properties";
        Properties p = new Properties();
        try {
            p.load(new FileInputStream(log4JPropertyFile));
            PropertyConfigurator.configure(p);
            logger.info("Log file is configured!");
        } catch (IOException e) {
            logger.error("Log file is not configured!");
        }
    }

    private void loadCustomerData(Path path) throws IOException {
        logger.info("Start processing customer collection");

        db.getCollection(COLLECTION_CUSTOMER).drop();
        db.getCollection(COLLECTION_CUSTOMER_STATIC).drop();
        db.getCollection(COLLECTION_CUSTOMER_UNUSED).drop();
        db.createCollection(COLLECTION_CUSTOMER);
        db.createCollection(COLLECTION_CUSTOMER_STATIC);
        db.createCollection(COLLECTION_CUSTOMER_UNUSED);

        MongoCollection<Document> customerCollection = db.getCollection(COLLECTION_CUSTOMER);
        MongoCollection<Document> customerStaticCollection = db.getCollection(COLLECTION_CUSTOMER_STATIC);
        MongoCollection<Document> customerUnusedCollection = db.getCollection(COLLECTION_CUSTOMER_UNUSED);

        Stream<String> customers = Files.lines(path);
        List<Document> customerList = new ArrayList<Document>();
        List<Document> customerStaticList = new ArrayList<Document>();
        List<Document> customerUnusedList = new ArrayList<Document>();

        customers.forEach(s -> {
            String[] content = s.split(",");
            String wId = content[0];
            String dId = content[1];
            String cId = content[2];
            String cFirst = content[3];
            String cMiddle = content[4];
            String cLast = content[5];
            String cStreet1 = content[6];
            String cStreet2 = content[7];
            String cCity = content[8];
            String cState = content[9];
            String cZip = content[10];
            String cPhone = content[11];
            String cSince = content[12];
            String cCredit = content[13];
            String cCreditLim = content[14];
            String cDiscount = content[15];
            String cBalance = content[16];
            String cYtdPayment = content[17];
            String cPaymentCnt = content[18];
            String cDeliveryCnt = content[19];
            String cData = content[20];

            long wdcId = Long.parseLong(wId);
            wdcId <<= 4;
            wdcId += Long.parseLong(dId);
            wdcId <<= 21;
            wdcId += Long.parseLong(cId);

            customerList.add(DocCreator.createCustomerDoc(wdcId, cBalance, cYtdPayment, cPaymentCnt, cDeliveryCnt));
            customerStaticList.add(DocCreator.createCustomerStaticDoc(wdcId, cFirst, cMiddle, cLast, cStreet1, cStreet2, cCity, cState, cZip, cPhone, cSince, cCredit, cCreditLim, cDiscount));
            customerUnusedList.add(DocCreator.createCustomerUnusedDoc(wdcId, cData));

            if (customerList.size() >= INSERT_THRESHOLD) {
                customerCollection.insertMany(customerList, new InsertManyOptions().ordered(false));
                customerList.clear();
                customerStaticCollection.insertMany(customerStaticList, new InsertManyOptions().ordered(false));
                customerStaticList.clear();
                customerUnusedCollection.insertMany(customerUnusedList, new InsertManyOptions().ordered(false));
                customerUnusedList.clear();
            }
        });

        if (!customerList.isEmpty()) {
            customerCollection.insertMany(customerList, new InsertManyOptions().ordered(false));
            customerStaticCollection.insertMany(customerStaticList, new InsertManyOptions().ordered(false));
            customerUnusedCollection.insertMany(customerUnusedList, new InsertManyOptions().ordered(false));
        }

        // Create index on c_balance
        customerCollection.createIndex(new Document("c_balance", -1));

        logger.info("Complete loading custom collection!");
    }

    private void loadDistrictData(Path path) throws IOException {
        logger.info("Start processing district collection");

        db.getCollection(COLLECTION_DISTRICT).drop();
        db.getCollection(COLLECTION_DISTRICT_NEXT_O_ID).drop();
        db.getCollection(COLLECTION_DISTRICT_STATIC).drop();
        db.createCollection(COLLECTION_DISTRICT);
        db.createCollection(COLLECTION_DISTRICT_NEXT_O_ID);
        db.createCollection(COLLECTION_DISTRICT_STATIC);

        MongoCollection<Document> districtCollection = db.getCollection(COLLECTION_DISTRICT);
        MongoCollection<Document> districtNextOIdCollection = db.getCollection(COLLECTION_DISTRICT_NEXT_O_ID);
        MongoCollection<Document> districtStaticCollection = db.getCollection(COLLECTION_DISTRICT_STATIC);

        Stream<String> districts = Files.lines(path);
        List<Document> districtList = new ArrayList<Document>();
        List<Document> districtNextOIdList = new ArrayList<Document>();
        List<Document> districtStaticList = new ArrayList<Document>();

        districts.forEach(d -> {
            String[] content = d.split(",");
            String wId = content[0];
            String dId = content[1];
            String dName = content[2];
            String dStreet1 = content[3];
            String dStreet2 = content[4];
            String dCity = content[5];
            String dState = content[6];
            String dZip = content[7];
            String dTax = content[8];
            String dYtd = content[9];
            String dNextOId = content[10];

            long wdId = Long.parseLong(wId);
            wdId <<= 4;
            wdId += Long.parseLong(dId);

            districtList.add(DocCreator.createDistrictDoc(wdId, dYtd));
            districtNextOIdList.add(DocCreator.createDistrictNextOIdDoc(wdId, dNextOId));
            districtStaticList.add(DocCreator.createDistrictStatic(wdId, dName, dStreet1, dStreet2, dCity, dState, dZip, dTax));

            if (districtList.size() >= INSERT_THRESHOLD) {
                districtCollection.insertMany(districtList, new InsertManyOptions().ordered(false));
                districtList.clear();
                districtNextOIdCollection.insertMany(districtNextOIdList, new InsertManyOptions().ordered(false));
                districtNextOIdList.clear();
                districtStaticCollection.insertMany(districtStaticList, new InsertManyOptions().ordered(false));
                districtStaticList.clear();
            }
        });

        if (!districtList.isEmpty()) {
            districtCollection.insertMany(districtList, new InsertManyOptions().ordered(false));
            districtNextOIdCollection.insertMany(districtNextOIdList, new InsertManyOptions().ordered(false));
            districtStaticCollection.insertMany(districtStaticList, new InsertManyOptions().ordered(false));
        }

        logger.info("Complete loading district collection!");
    }

    private void loadItemData(Path path) throws IOException {
        logger.info("Start processing item collection");

        db.getCollection(COLLECTION_ITEM).drop();
        db.getCollection(COLLECTION_ITEM_UNUSED).drop();
        db.createCollection(COLLECTION_ITEM);
        db.createCollection(COLLECTION_ITEM_UNUSED);

        MongoCollection<Document> itemCollection = db.getCollection(COLLECTION_ITEM);
        MongoCollection<Document> itemUnusedCollection = db.getCollection(COLLECTION_ITEM_UNUSED);

        Stream<String> items = Files.lines(path);
        List<Document> itemList = new ArrayList<Document>();
        List<Document> itemUnusedList = new ArrayList<Document>();

        items.forEach(i -> {
            String[] content = i.split(",");
            String iId = content[0];
            String iName = content[1];
            String iPrice = content[2];
            String iImId = content[3];
            String iData = content[4];

            itemList.add(DocCreator.createItemDoc(iId, iName, iPrice, iImId));
            itemUnusedList.add(DocCreator.createItemUnusedDoc(iId, iData));

            if (itemList.size() >= INSERT_THRESHOLD) {
                itemCollection.insertMany(itemList, new InsertManyOptions().ordered(false));
                itemList.clear();
                itemUnusedCollection.insertMany(itemUnusedList, new InsertManyOptions().ordered(false));
                itemUnusedList.clear();
            }
        });

        if (!itemList.isEmpty()) {
            itemCollection.insertMany(itemList, new InsertManyOptions().ordered(false));
            itemUnusedCollection.insertMany(itemUnusedList, new InsertManyOptions().ordered(false));
        }

        logger.info("Complete loading item collection!");
    }

    private void loadOrderData(Path path) throws IOException {
        logger.info("Start processing orders collection");

        db.getCollection(COLLECTION_ORDER).drop();
        db.createCollection(COLLECTION_ORDER);

        MongoCollection<Document> orderCollection = db.getCollection(COLLECTION_ORDER);
        MongoCollection<Document> customerCollection = db.getCollection(COLLECTION_CUSTOMER);
        MongoCollection<Document> districtCollection = db.getCollection(COLLECTION_DISTRICT);

        Stream<String> orders = Files.lines(path);
        List<Document> orderList = new ArrayList<Document>();

        // Updates for district
        List<WriteModel<Document>> districtUpdates = new ArrayList<WriteModel<Document>>();

        // c_last_o_id map
        Map<Long, Integer> cLastOIdMap = new HashMap<Long, Integer>();

        orders.forEach(o -> {
            String[] content = o.split(",");
            String wId = content[0];
            String dId = content[1];
            String oId = content[2];
            String cId = content[3];
            String oCarrierId = content[4];
            String oOlCnt = content[5];
            String oAllLocal = content[6];
            String oEntryD = content[7];

            long wdoId = Long.parseLong(wId);
            wdoId <<= 4;
            wdoId += Long.parseLong(dId);

            long wdId = wdoId;

            wdoId <<= 24;
            wdoId += Long.parseLong(oId);

            long wdcId = Long.parseLong(wId);
            wdcId <<= 4;
            wdcId += Long.parseLong(dId);
            wdcId <<= 21;
            wdcId += Long.parseLong(cId);

            orderList.add(DocCreator.createOrderDoc(wdoId, cId, oCarrierId, oOlCnt, oAllLocal, oEntryD));

            // Add to districtUpdates if carrier id is null
            List<Document> list = new ArrayList<Document>();
            list.add(new Document("o_id", Integer.parseInt(oId)).append("c_id", Integer.parseInt(cId)));

            if (oCarrierId.equals("null")) {
                districtUpdates.add(new UpdateOneModel<Document>(Filters.eq("_id", wdId),
                        new Document("$push",
                                new Document("d_o_to_c_list",
                                        new Document("$each", list).append("$sort", new Document("o_id", 1))))));
            }

            // Update c_last_o_id
            int oIdInt = Integer.parseInt(oId);
            if (cLastOIdMap.containsKey(wdcId)) {
                if (cLastOIdMap.get(wdcId) < oIdInt) {
                    cLastOIdMap.put(wdcId, oIdInt);
                }
            } else {
                cLastOIdMap.put(wdcId, oIdInt);
            }

            if (orderList.size() >= INSERT_THRESHOLD) {
                orderCollection.insertMany(orderList, new InsertManyOptions().ordered(false));
                orderList.clear();
                if (!districtUpdates.isEmpty()) {
                    districtCollection.bulkWrite(districtUpdates);
                    districtUpdates.clear();
                }
            }
        });

        if (!orderList.isEmpty()) {
            orderCollection.insertMany(orderList, new InsertManyOptions().ordered(false));
        }

        if (!districtUpdates.isEmpty()) {
            districtCollection.bulkWrite(districtUpdates);
        }

        // Prepare bulk updates
        List<WriteModel<Document>> cLastOIdUpdates = new ArrayList<WriteModel<Document>>();
        for (Map.Entry<Long, Integer> entry : cLastOIdMap.entrySet()) {
            cLastOIdUpdates.add(new UpdateOneModel<Document>(Filters.eq("_id", entry.getKey()),
                    new Document("$set", new Document("c_last_o_id", entry.getValue())))
            );
        }

        if (!cLastOIdUpdates.isEmpty()) {
            customerCollection.bulkWrite(cLastOIdUpdates, new BulkWriteOptions().ordered(false));
        }

        logger.info("Complete loading orders collection!");
    }

    private void loadOrderLineData(Path path) throws IOException {
        logger.info("Start processing orderLine collection");

        db.getCollection(COLLECTION_ORDER_LINE_UNUSED).drop();
        db.createCollection(COLLECTION_ORDER_LINE_UNUSED);

        MongoCollection<Document> orderLineUnusedCollection = db.getCollection(COLLECTION_ORDER_LINE_UNUSED);
        MongoCollection<Document> orderCollection = db.getCollection(COLLECTION_ORDER);
        MongoCollection<Document> itemCollection = db.getCollection(COLLECTION_ITEM);

        Stream<String> orderLines = Files.lines(path);
        List<Document> orderLineUnusedList = new ArrayList<Document>();

//        List<Long> wdoIdList = new ArrayList<Long>();
        List<WriteModel<Document>> updates = new ArrayList<WriteModel<Document>>();

        orderLines.forEach(o -> {
            String[] content = o.split(",");
            String wId = content[0];
            String dId = content[1];
            String oId = content[2];
            String olNumber = content[3];
            String iId = content[4];
            String olDeliveryD = content[5];
            String olAmount = content[6];
            String olSupplyWId = content[7];
            String olQuantity = content[8];
            String olDistInfo = content[9];

            long wdoolId = Long.parseLong(wId);
            wdoolId <<= 4;
            wdoolId += Long.parseLong(dId);
            wdoolId <<= 24;
            wdoolId += Long.parseLong(oId);

            // Add update model
            long wdoId = wdoolId;
            Document item = itemCollection.find(Filters.eq("_id", Integer.parseInt(iId))).first();

            updates.add(new UpdateOneModel<Document>(Filters.eq("_id", wdoId), new Document("$push", new Document("ol_list",
                    DocCreator.createOrderLineDoc(olNumber,
                            DocCreator.createItemDoc(iId, item.get("i_name", String.class), item.get("i_price", Double.class), item.get("i_im_id", Integer.class)),
                            olDeliveryD,
                            olAmount,
                            olSupplyWId,
                            olQuantity
                    )))));

            wdoolId <<= 5;
            wdoolId += Long.parseLong(olNumber);

            orderLineUnusedList.add(DocCreator.createOrderLineUnusedDoc(wdoolId, olDistInfo));

            if (orderLineUnusedList.size() >= INSERT_THRESHOLD) {
                orderLineUnusedCollection.insertMany(orderLineUnusedList, new InsertManyOptions().ordered(false));
                orderLineUnusedList.clear();
                orderCollection.bulkWrite(updates, new BulkWriteOptions().ordered(false));
                updates.clear();
            }
        });

        if (!orderLineUnusedList.isEmpty()) {
            orderLineUnusedCollection.insertMany(orderLineUnusedList, new InsertManyOptions().ordered(false));
            orderCollection.bulkWrite(updates, new BulkWriteOptions().ordered(false));
        }

        logger.info("Complete loading orderLine collection!");
    }

    private void loadStockData(Path path) throws IOException {
        logger.info("Start processing stock collection");

        db.getCollection(COLLECTION_STOCK).drop();
        db.getCollection(COLLECTION_STOCK_STATIC).drop();
        db.getCollection(COLLECTION_STOCK_UNUSED).drop();
        db.createCollection(COLLECTION_STOCK);
        db.createCollection(COLLECTION_STOCK_STATIC);
        db.createCollection(COLLECTION_STOCK_UNUSED);

        MongoCollection<Document> stockCollection = db.getCollection(COLLECTION_STOCK);
        MongoCollection<Document> stockStaticCollection = db.getCollection(COLLECTION_STOCK_STATIC);
        MongoCollection<Document> stockUnusedCollection = db.getCollection(COLLECTION_STOCK_UNUSED);

        Stream<String> stocks = Files.lines(path);
        List<Document> stockList = new ArrayList<Document>();
        List<Document> stockStaticList = new ArrayList<Document>();
        List<Document> stockUnusedList = new ArrayList<Document>();

        stocks.forEach(s -> {
            String[] content = s.split(",");
            String wId = content[0];
            String iId = content[1];
            String sQuantity = content[2];
            String sYtd = content[3];
            String sOrderCnt = content[4];
            String sRemoteCnt = content[5];
            String sDist01 = content[6];
            String sDist02 = content[7];
            String sDist03 = content[8];
            String sDist04 = content[9];
            String sDist05 = content[10];
            String sDist06 = content[11];
            String sDist07 = content[12];
            String sDist08 = content[13];
            String sDist09 = content[14];
            String sDist10 = content[15];
            String sData = content[16];

            long wiId = Long.parseLong(wId);
            wiId <<= 17;
            wiId += Long.parseLong(iId);

            stockList.add(DocCreator.createStockDoc(wiId, sQuantity, sYtd, sOrderCnt, sRemoteCnt));
            stockStaticList.add(DocCreator.createStockStaticDoc(wiId, sDist01, sDist02, sDist03, sDist04, sDist05, sDist06, sDist07, sDist08, sDist09, sDist10));
            stockUnusedList.add(DocCreator.createStockUnusedDoc(wiId, sData));

            if (stockList.size() >= INSERT_THRESHOLD) {
                stockCollection.insertMany(stockList, new InsertManyOptions().ordered(false));
                stockList.clear();
                stockStaticCollection.insertMany(stockStaticList, new InsertManyOptions().ordered(false));
                stockStaticList.clear();
                stockUnusedCollection.insertMany(stockUnusedList, new InsertManyOptions().ordered(false));
                stockUnusedList.clear();
            }
        });

        if (!stockList.isEmpty()) {
            stockCollection.insertMany(stockList, new InsertManyOptions().ordered(false));
            stockStaticCollection.insertMany(stockStaticList, new InsertManyOptions().ordered(false));
            stockUnusedCollection.insertMany(stockUnusedList, new InsertManyOptions().ordered(false));
        }

        logger.info("Complete loading stock collection!");
    }

    private void loadWarehouseData(Path path) throws IOException {
        logger.info("Start processing warehouse collection");

        db.getCollection(COLLECTION_WAREHOUSE).drop();
        db.getCollection(COLLECTION_WAREHOUSE_STATIC).drop();
        db.createCollection(COLLECTION_WAREHOUSE);
        db.createCollection(COLLECTION_WAREHOUSE_STATIC);

        MongoCollection<Document> warehouseCollection = db.getCollection(COLLECTION_WAREHOUSE);
        MongoCollection<Document> warehouseStaticCollection = db.getCollection(COLLECTION_WAREHOUSE_STATIC);

        Stream<String> warehouses = Files.lines(path);
        List<Document> warehouseList = new ArrayList<Document>();
        List<Document> warehouseStaticList = new ArrayList<Document>();

        warehouses.forEach(w -> {
            String[] content = w.split(",");
            String wId = content[0];
            String wName = content[1];
            String wStreet1 = content[2];
            String wStreet2 = content[3];
            String wCity = content[4];
            String wState = content[5];
            String wZip = content[6];
            String wTax = content[7];
            String wYtd = content[8];

            warehouseList.add(DocCreator.createWarehouseDoc(wId, wYtd));
            warehouseStaticList.add(DocCreator.createWarehouseStaticDoc(wId, wName, wStreet1, wStreet2, wCity, wState, wZip, wTax));

            if (warehouseList.size() >= INSERT_THRESHOLD) {
                warehouseCollection.insertMany(warehouseList, new InsertManyOptions().ordered(false));
                warehouseList.clear();
                warehouseStaticCollection.insertMany(warehouseStaticList, new InsertManyOptions().ordered(false));
                warehouseStaticList.clear();
            }
        });

        if (!warehouseList.isEmpty()) {
            warehouseCollection.insertMany(warehouseList, new InsertManyOptions().ordered(false));
            warehouseStaticCollection.insertMany(warehouseStaticList, new InsertManyOptions().ordered(false));
        }

        warehouseCollection.updateOne(Filters.eq("_id", 1), new Document("$push", new Document("test", 1)));

        warehouseCollection.updateOne(Filters.eq("_id", 1), new Document("$push", new Document("test", 2)));

        logger.info("Complete loading warehouse collection!");
    }

    private void postProcess() {
        MongoCollection<Document> orderCollection = db.getCollection(COLLECTION_ORDER);

        List<WriteModel<Document>> updates = new ArrayList<WriteModel<Document>>();

        MongoCursor<Document> allOrder = orderCollection.find().iterator();
        while (allOrder.hasNext()) {
            Document order = allOrder.next();
            List<Document> orderLineList = order.get("ol_list", List.class);

            List<String> popularIName = new ArrayList<String>();
            double maxQuantity = 0;
            double oTotalAmount = 0;

            for (Document ol : orderLineList) {
                double quantity = ol.get("ol_quantity", Double.class);
                if (quantity > maxQuantity) {
                    maxQuantity = quantity;
                    popularIName.clear();
                    popularIName.add(ol.get("ol_item", Document.class).get("i_name", String.class));
                } else if (quantity == maxQuantity) {
                    popularIName.add(ol.get("ol_item", Document.class).get("i_name", String.class));
                }

                oTotalAmount += ol.get("ol_amount", Double.class);
            }

            // Add update to updates
            updates.add(new UpdateOneModel<Document>(Filters.eq("_id", order.get("_id", Long.class)),
                    new Document("$set", new Document("o_popular_i_name", popularIName).append("o_popular_ol_quantity", maxQuantity).append("o_total_amount", oTotalAmount))));

            if (updates.size() >= INSERT_THRESHOLD) {
                orderCollection.bulkWrite(updates, new BulkWriteOptions().ordered(false));
                updates.clear();
            }
        }

        if (!updates.isEmpty()) {
            orderCollection.bulkWrite(updates, new BulkWriteOptions().ordered(false));
        }
    }

//    public void test() {
//        MongoCollection<Document> t = db.getCollection(COLLECTION_WAREHOUSE);
//        ArrayList<Document> d = t.find(new Document("_id", 1)).projection(new Document("test", new Document("$slice", 1))).into(new ArrayList<>());
//        System.out.println(d.get(0).toJson());
//    }

    public static void main(String[] args) throws IOException {
        DatabaseBuilder builder = new DatabaseBuilder(System.getProperty("user.dir") + "/d8", "d8_1");
        builder.loadData();
//        builder.test();
    }
}

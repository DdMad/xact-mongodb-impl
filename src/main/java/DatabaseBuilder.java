import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.bson.Document;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
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
    private static final String COLLECTION_ORDER_LINE = "orderLine";
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
                loadOrderData(path);
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

        if (orderLinePath != null) {
            loadOrderLineData(orderLinePath);
        }
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

            customerList.add(createCustomerDoc(wdcId, cBalance, cYtdPayment, cPaymentCnt, cDeliveryCnt));
            customerStaticList.add(createCustomerStaticDoc(wdcId, cFirst, cMiddle, cLast, cStreet1, cStreet2, cCity, cState, cZip, cPhone, cSince, cCredit, cCreditLim, cDiscount));
            customerUnusedList.add(createCustomerUnusedDoc(wdcId, cData));

            if (customerList.size() >= INSERT_THRESHOLD) {
                customerCollection.insertMany(customerList);
                customerList.clear();
                customerStaticCollection.insertMany(customerStaticList);
                customerStaticList.clear();
                customerUnusedCollection.insertMany(customerUnusedList);
                customerUnusedList.clear();
            }
        });

        if (!customerList.isEmpty()) {
            customerCollection.insertMany(customerList);
            customerStaticCollection.insertMany(customerStaticList);
            customerUnusedCollection.insertMany(customerUnusedList);
        }

        logger.info("Complete loading custom collection!");
    }

    private Document createCustomerDoc(long wdcId, String cBalance, String cYtdPayment, String cPaymentCnt, String cDeliveryCnt) {
        return new Document("_id", wdcId)
                .append("c_balance", Double.parseDouble(cBalance))
                .append("c_ytd_payment", Float.parseFloat(cYtdPayment))
                .append("c_payment_cnt", Integer.parseInt(cPaymentCnt))
                .append("c_delivery_cnt", Integer.parseInt(cDeliveryCnt))
                .append("c_last_o_id", -1);
    }

    private Document createCustomerStaticDoc(long wdcId, String cFirst, String cMiddle, String cLast, String cStreet1, String cStreet2, String cCity, String cState, String cZip, String cPhone, String cSince, String cCredit, String cCreditLim, String cDiscount) {
        try {
            return new Document("_id", wdcId)
                    .append("c_first", cFirst)
                    .append("c_middle", cMiddle)
                    .append("c_last", cLast)
                    .append("c_street_1", cStreet1)
                    .append("c_street_2", cStreet2)
                    .append("c_city", cCity)
                    .append("c_state", cState)
                    .append("c_zip", cZip)
                    .append("c_phone", cPhone)
                    .append("c_since", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(cSince))
                    .append("c_credit", cCredit)
                    .append("c_credit_lim", Double.parseDouble(cCreditLim))
                    .append("c_discount", Double.parseDouble(cDiscount));
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return new Document("_id", wdcId)
                .append("c_first", cFirst)
                .append("c_middle", cMiddle)
                .append("c_last", cLast)
                .append("c_street_1", cStreet1)
                .append("c_street_2", cStreet2)
                .append("c_city", cCity)
                .append("c_state", cState)
                .append("c_zip", cZip)
                .append("c_phone", cPhone)
                .append("c_since", cSince)
                .append("c_credit", cCredit)
                .append("c_credit_lim", Double.parseDouble(cCreditLim))
                .append("c_discount", Double.parseDouble(cDiscount));
    }

    private Document createCustomerUnusedDoc(long wdcId, String cData) {
        return new Document("_id", wdcId)
                .append("c_data", cData);
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

            districtList.add(createDistrictDoc(wdId, dYtd));
            districtNextOIdList.add(createDistrictNextOIdDoc(wdId, dNextOId));
            districtStaticList.add(createDistrictStatic(wdId, dName, dStreet1, dStreet2, dCity, dState, dZip, dTax));

            if (districtList.size() >= INSERT_THRESHOLD) {
                districtCollection.insertMany(districtList);
                districtList.clear();
                districtNextOIdCollection.insertMany(districtNextOIdList);
                districtNextOIdList.clear();
                districtStaticCollection.insertMany(districtStaticList);
                districtStaticList.clear();
            }
        });

        if (!districtList.isEmpty()) {
            districtCollection.insertMany(districtList);
            districtNextOIdCollection.insertMany(districtNextOIdList);
            districtStaticCollection.insertMany(districtStaticList);
        }

        logger.info("Complete loading district collection!");
    }

    private Document createDistrictDoc(long wdId, String dYtd) {
        return new Document("_id", wdId)
                .append("d_ytd", Double.parseDouble(dYtd));
    }

    private Document createDistrictNextOIdDoc(long wdId, String dNextOId) {
        return new Document("_id", wdId)
                .append("d_next_o_id", Integer.parseInt(dNextOId));
    }

    private Document createDistrictStatic(long wdId, String dName, String dStreet1, String dStreet2, String dCity, String dState, String dZip, String dTax) {
        return new Document("_id", wdId)
                .append("d_name", dName)
                .append("d_street_1", dStreet1)
                .append("d_street_2", dStreet2)
                .append("d_city", dCity)
                .append("d_state", dState)
                .append("d_zip", dZip)
                .append("d_tax", Double.parseDouble(dTax));
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

            itemList.add(createItemDoc(iId, iName, iPrice, iImId));
            itemUnusedList.add(createItemUnusedDoc(iId, iData));

            if (itemList.size() >= INSERT_THRESHOLD) {
                itemCollection.insertMany(itemList);
                itemList.clear();
                itemUnusedCollection.insertMany(itemUnusedList);
                itemUnusedList.clear();
            }
        });

        if (!itemList.isEmpty()) {
            itemCollection.insertMany(itemList);
            itemUnusedCollection.insertMany(itemUnusedList);
        }

        logger.info("Complete loading item collection!");
    }

    private Document createItemDoc(String iId, String iName, String iPrice, String iImId) {
        return new Document("_id", Integer.parseInt(iId))
                .append("i_name", iName)
                .append("i_price", Double.parseDouble(iPrice))
                .append("i_im_id", Integer.parseInt(iImId));
    }

    private Document createItemUnusedDoc(String iId, String iData) {
        return new Document("_id", Integer.parseInt(iId))
                .append("i_data", iData);
    }

    private void loadOrderData(Path path) throws IOException {
        logger.info("Start processing orders collection");

        db.getCollection(COLLECTION_ORDER).drop();
        db.createCollection(COLLECTION_ORDER);

        MongoCollection<Document> orderCollection = db.getCollection(COLLECTION_ORDER);

        Stream<String> orders = Files.lines(path);
        List<Document> orderList = new ArrayList<Document>();

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
            wdoId <<= 24;
            wdoId += Long.parseLong(oId);

            orderList.add(createOrderDoc(wdoId, cId, oCarrierId, oOlCnt, oAllLocal, oEntryD));

            if (orderList.size() >= INSERT_THRESHOLD) {
                orderCollection.insertMany(orderList);
                orderList.clear();
            }
        });

        if (!orderList.isEmpty()) {
            orderCollection.insertMany(orderList);
        }

        logger.info("Complete loading orders collection!");
    }

    private Document createOrderDoc(long wdoId, String cId, String oCarrierId, String oOlCnt, String oAllLocal, String oEntryD) {
        try {
            return new Document("_id", wdoId)
                    .append("c_id", Integer.parseInt(cId))
                    .append("o_carrier_id", oCarrierId.equals("null") ? null : Integer.parseInt(oCarrierId))
                    .append("o_ol_cnt", Double.parseDouble(oOlCnt))
                    .append("o_all_local", Double.parseDouble(oAllLocal))
                    .append("o_entry_d", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(oEntryD));
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return new Document("_id", wdoId)
                .append("c_id", cId)
                .append("o_carrier_id", oCarrierId.equals("null") ? null : Integer.parseInt(oCarrierId))
                .append("o_ol_cnt", Double.parseDouble(oOlCnt))
                .append("o_all_local", Double.parseDouble(oAllLocal))
                .append("o_entry_d", oEntryD);
    }

    private void loadOrderLineData(Path path) throws IOException {
        logger.info("Start processing order-line collection");

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
            MongoCursor<Document> itemResult = itemCollection.find(Filters.eq("_id", Integer.parseInt(iId))).iterator();
            Document item = null;
            if (itemResult.hasNext()) {
                item = itemResult.next();
            }

            updates.add(new UpdateOneModel<Document>(Filters.eq("_id", wdoId), new Document("$push", new Document("ol_list", new Document("ol_number", Integer.parseInt(olNumber))
                    .append("ol_item", new Document("i_id", Integer.parseInt(iId))
                            .append("i_name", item.get("i_name", String.class))
                            .append("i_price", item.get("i_price", Double.class))
                            .append("i_im_id", item.get("i_im_id", Integer.class))
                    )
                    .append("ol_delivery_d", olDeliveryD.equals("null") ? null : olDeliveryD)
                    .append("ol_amount", Double.parseDouble(olAmount))
                    .append("ol_supply_w_id", Integer.parseInt(olSupplyWId))
                    .append("ol_quantity", Double.parseDouble(olQuantity))
            ))));

            wdoolId <<= 5;
            wdoolId += Long.parseLong(olNumber);

            orderLineUnusedList.add(createOrderLineUnusedDoc(wdoolId, olDistInfo));

            if (orderLineUnusedList.size() >= INSERT_THRESHOLD) {
                orderLineUnusedCollection.insertMany(orderLineUnusedList);
                orderLineUnusedList.clear();
                orderCollection.bulkWrite(updates);
                updates.clear();
            }
        });

        if (!orderLineUnusedList.isEmpty()) {
            orderLineUnusedCollection.insertMany(orderLineUnusedList);
            orderCollection.bulkWrite(updates);
        }

        logger.info("Complete loading order_line collection!");
    }

    private Document createOrderLineUnusedDoc(long wdoolId, String olDistInfo) {
        return new Document("_id", wdoolId)
                .append("ol_dist_info", olDistInfo);
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

            stockList.add(createStockDoc(wiId, sQuantity, sYtd, sOrderCnt, sRemoteCnt));
            stockStaticList.add(createStockStaticDoc(wiId, sDist01, sDist02, sDist03, sDist04, sDist05, sDist06, sDist07, sDist08, sDist09, sDist10));
            stockUnusedList.add(createStockUnusedDoc(wiId, sData));

            if (stockList.size() >= INSERT_THRESHOLD) {
                stockCollection.insertMany(stockList);
                stockList.clear();
                stockStaticCollection.insertMany(stockStaticList);
                stockStaticList.clear();
                stockUnusedCollection.insertMany(stockUnusedList);
                stockUnusedList.clear();
            }
        });

        if (!stockList.isEmpty()) {
            stockCollection.insertMany(stockList);
            stockStaticCollection.insertMany(stockStaticList);
            stockUnusedCollection.insertMany(stockUnusedList);
        }

        logger.info("Complete loading stock collection!");
    }

    private Document createStockDoc(long wiId, String sQuantity, String sYtd, String sOrderCnt, String sRemoteCnt) {
        return new Document("_id", wiId)
                .append("s_quantity", Double.parseDouble(sQuantity))
                .append("s_ytd", Double.parseDouble(sYtd))
                .append("s_order_cnt", Integer.parseInt(sOrderCnt))
                .append("s_remote_cnt", Integer.parseInt(sRemoteCnt));
    }

    private Document createStockStaticDoc(long wiId, String sDist01, String sDist02, String sDist03, String sDist04, String sDist05, String sDist06, String sDist07, String sDist08, String sDist09, String sDist10) {
        return new Document("_id", wiId)
                .append("s_dist_01", sDist01)
                .append("s_dist_02", sDist02)
                .append("s_dist_03", sDist03)
                .append("s_dist_04", sDist04)
                .append("s_dist_05", sDist05)
                .append("s_dist_06", sDist06)
                .append("s_dist_07", sDist07)
                .append("s_dist_08", sDist08)
                .append("s_dist_09", sDist09)
                .append("s_dist_10", sDist10);
    }

    private Document createStockUnusedDoc(long wiId, String sData) {
        return new Document("_id", wiId)
                .append("s_data", sData);
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

            warehouseList.add(createWarehouseDoc(wId, wYtd));
            warehouseStaticList.add(createWarehouseStaticDoc(wId, wName, wStreet1, wStreet2, wCity, wState, wZip, wTax));

            if (warehouseList.size() >= INSERT_THRESHOLD) {
                warehouseCollection.insertMany(warehouseList);
                warehouseList.clear();
                warehouseStaticCollection.insertMany(warehouseStaticList);
                warehouseStaticList.clear();
            }
        });

        if (!warehouseList.isEmpty()) {
            warehouseCollection.insertMany(warehouseList);
            warehouseStaticCollection.insertMany(warehouseStaticList);
        }

        warehouseCollection.updateOne(Filters.eq("_id", 1), new Document("$push", new Document("test", 1)));

        warehouseCollection.updateOne(Filters.eq("_id", 1), new Document("$push", new Document("test", 2)));

        logger.info("Complete loading warehouse collection!");
    }

    private Document createWarehouseDoc(String wId, String wYtd) {
        return new Document("_id", Integer.parseInt(wId))
                .append("w_ytd", Double.parseDouble(wYtd));
    }

    private Document createWarehouseStaticDoc(String wId, String wName, String wStreet1, String wStreet2, String wCity, String wState, String wZip, String wTax) {
        return new Document("_id", Integer.parseInt(wId))
                .append("w_name", wName)
                .append("w_street_1", wStreet1)
                .append("w_street_2", wStreet2)
                .append("w_city", wCity)
                .append("w_state", wState)
                .append("w_zip", wZip)
                .append("w_tax", Double.parseDouble(wTax));
    }

    public static void main(String[] args) throws IOException {
        DatabaseBuilder builder = new DatabaseBuilder(System.getProperty("user.dir") + "/d8", "d8_1");
        builder.loadData();
    }
}

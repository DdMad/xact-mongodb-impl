import org.bson.Document;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ddmad on 10/11/16.
 */
public class DocCreator {

    public static Document createWarehouseDoc(String wId, String wYtd) {
        return new Document("_id", Integer.parseInt(wId))
                .append("w_ytd", Double.parseDouble(wYtd));
    }

    public static Document createWarehouseStaticDoc(String wId, String wName, String wStreet1, String wStreet2, String wCity, String wState, String wZip, String wTax) {
        return new Document("_id", Integer.parseInt(wId))
                .append("w_name", wName)
                .append("w_street_1", wStreet1)
                .append("w_street_2", wStreet2)
                .append("w_city", wCity)
                .append("w_state", wState)
                .append("w_zip", wZip)
                .append("w_tax", Double.parseDouble(wTax));
    }

    public static Document createStockDoc(long wiId, String sQuantity, String sYtd, String sOrderCnt, String sRemoteCnt) {
        return new Document("_id", wiId)
                .append("s_quantity", Double.parseDouble(sQuantity))
                .append("s_ytd", Double.parseDouble(sYtd))
                .append("s_order_cnt", Integer.parseInt(sOrderCnt))
                .append("s_remote_cnt", Integer.parseInt(sRemoteCnt));
    }

    public static Document createStockStaticDoc(long wiId, String sDist01, String sDist02, String sDist03, String sDist04, String sDist05, String sDist06, String sDist07, String sDist08, String sDist09, String sDist10) {
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

    public static Document createStockUnusedDoc(long wiId, String sData) {
        return new Document("_id", wiId)
                .append("s_data", sData);
    }

    public static Document createOrderLineDoc(String olNumber, Document olItem, String olDeliveryD, String olAmount, String olSupplyWId, String olQuantity) {
        return new Document("ol_number", Integer.parseInt(olNumber))
                .append("ol_item", olItem)
                .append("ol_delivery_d", olDeliveryD)
                .append("ol_amount", Double.parseDouble(olAmount))
                .append("ol_supply_w_id", Integer.parseInt(olSupplyWId))
                .append("ol_quantity", Double.parseDouble(olQuantity));
    }

    public static Document createOrderLineUnusedDoc(long wdoolId, String olDistInfo) {
        return new Document("_id", wdoolId)
                .append("ol_dist_info", olDistInfo);
    }

    public static Document createOrderDoc(long wdoId, String cId, String oCarrierId, String oOlCnt, String oAllLocal, String oEntryD) {
        try {
            return new Document("_id", wdoId)
                    .append("c_id", Integer.parseInt(cId))
                    .append("o_carrier_id", oCarrierId == null || oCarrierId.equals("null") ? null : Integer.parseInt(oCarrierId))
                    .append("o_ol_cnt", Integer.parseInt(oOlCnt))
                    .append("o_all_local", Integer.parseInt(oAllLocal))
                    .append("o_entry_d", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(oEntryD));
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return new Document("_id", wdoId)
                .append("c_id", cId)
                .append("o_carrier_id", oCarrierId == null || oCarrierId.equals("null") ? null : Integer.parseInt(oCarrierId))
                .append("o_ol_cnt", Integer.parseInt(oOlCnt))
                .append("o_all_local", Integer.parseInt(oAllLocal))
                .append("o_entry_d", oEntryD);
    }

    public static Document createOrderWithOrderLinePopularTotalAmountDoc(long wdoId, String cId, String oCarrierId, String oOlCnt, String oAllLocal, String oEntryD, List<Document> orderLineList, List<String> oPopularIName, double oPopularOlQuantity, double oTotalAmount) {
        try {
            return new Document("_id", wdoId)
                    .append("c_id", Integer.parseInt(cId))
                    .append("o_carrier_id", oCarrierId == null || oCarrierId.equals("null") ? null : Integer.parseInt(oCarrierId))
                    .append("o_ol_cnt", Integer.parseInt(oOlCnt))
                    .append("o_all_local", Integer.parseInt(oAllLocal))
                    .append("o_entry_d", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(oEntryD))
                    .append("ol_list", orderLineList)
                    .append("o_popular_i_name", oPopularIName)
                    .append("o_popular_ol_quantity", oPopularOlQuantity)
                    .append("o_total_amount", oTotalAmount);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return new Document("_id", wdoId)
                .append("c_id", cId)
                .append("o_carrier_id", oCarrierId == null || oCarrierId.equals("null") ? null : Integer.parseInt(oCarrierId))
                .append("o_ol_cnt", Integer.parseInt(oOlCnt))
                .append("o_all_local", Integer.parseInt(oAllLocal))
                .append("o_entry_d", oEntryD)
                .append("ol_list", orderLineList)
                .append("o_popular_i_name", oPopularIName)
                .append("o_popular_ol_quantity", oPopularOlQuantity)
                .append("o_total_amount", oTotalAmount);
    }

    public static Document createItemDoc(String iId, String iName, String iPrice, String iImId) {
        return new Document("_id", Integer.parseInt(iId))
                .append("i_name", iName)
                .append("i_price", Double.parseDouble(iPrice))
                .append("i_im_id", Integer.parseInt(iImId));
    }

    public static Document createItemDoc(String iId, String iName, Double iPrice, Integer iImId) {
        return new Document("_id", Integer.parseInt(iId))
                .append("i_name", iName)
                .append("i_price", iPrice)
                .append("i_im_id", iImId);
    }

    public static Document createItemUnusedDoc(String iId, String iData) {
        return new Document("_id", Integer.parseInt(iId))
                .append("i_data", iData);
    }

    public static Document createDistrictDoc(long wdId, String dYtd) {
        return new Document("_id", wdId)
                .append("d_ytd", Double.parseDouble(dYtd));
    }

    public static Document createDistrictNextOIdDoc(long wdId, String dNextOId) {
        return new Document("_id", wdId)
                .append("d_next_o_id", Integer.parseInt(dNextOId));
    }

    public static Document createDistrictStatic(long wdId, String dName, String dStreet1, String dStreet2, String dCity, String dState, String dZip, String dTax) {
        return new Document("_id", wdId)
                .append("d_name", dName)
                .append("d_street_1", dStreet1)
                .append("d_street_2", dStreet2)
                .append("d_city", dCity)
                .append("d_state", dState)
                .append("d_zip", dZip)
                .append("d_tax", Double.parseDouble(dTax))
                .append("d_o_to_c_list", new ArrayList<Document>());
    }

    public static Document createCustomerDoc(long wdcId, String cBalance, String cYtdPayment, String cPaymentCnt, String cDeliveryCnt) {
        return new Document("_id", wdcId)
                .append("c_balance", Double.parseDouble(cBalance))
                .append("c_ytd_payment", Float.parseFloat(cYtdPayment))
                .append("c_payment_cnt", Integer.parseInt(cPaymentCnt))
                .append("c_delivery_cnt", Integer.parseInt(cDeliveryCnt))
                .append("c_last_o_id", -1);
    }

    public static Document createCustomerStaticDoc(long wdcId, String cFirst, String cMiddle, String cLast, String cStreet1, String cStreet2, String cCity, String cState, String cZip, String cPhone, String cSince, String cCredit, String cCreditLim, String cDiscount) {
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

    public static Document createCustomerUnusedDoc(long wdcId, String cData) {
        return new Document("_id", wdcId)
                .append("c_data", cData);
    }
}

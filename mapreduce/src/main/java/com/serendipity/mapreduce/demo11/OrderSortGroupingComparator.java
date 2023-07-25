package com.serendipity.mapreduce.demo11;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderSortGroupingComparator extends WritableComparator {

    protected OrderSortGroupingComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;
        /**
         * 如果orderId 相同则表示同一组
         */
        return aBean.getOrderId() - bBean.getOrderId();
    }
}

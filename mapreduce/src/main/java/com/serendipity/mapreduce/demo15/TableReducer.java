package com.serendipity.mapreduce.demo15;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {

        List<TableBean> tableBeanList = new ArrayList<TableBean>();
        String productName = "";

        for (TableBean bean : values) {
            if (bean.getTableFlag().equals("pd")) {
                productName = bean.getProductName();
            } else {
                TableBean tableBean = new TableBean();
                try {
                    BeanUtils.copyProperties(bean, tableBean);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
                tableBean.setProductName(productName);
                tableBeanList.add(tableBean);
            }
        }
        for (TableBean bean : tableBeanList) {
            bean.setProductName(productName);
            context.write(bean, NullWritable.get());
        }
    }
}

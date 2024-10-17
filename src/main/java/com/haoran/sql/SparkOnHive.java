package com.haoran.sql;

import com.haoran.utils.ConnectUtil;
import org.apache.spark.sql.*;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

public class SparkOnHive {
    public static void main(String[] args) {
        SparkSession ss = new ConnectUtil().getHiveSparkSession();

        String sqlTmp = "show tables ";

        ss.sql(sqlTmp).show(false);
        ss.close();
    }
}

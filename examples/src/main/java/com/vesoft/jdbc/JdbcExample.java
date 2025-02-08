/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.jdbc;

import com.vesoft.nebula.jdbc.NebulaDriver;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JdbcExample {
    public static void main(String[] args) throws SQLException {
        DriverManager.registerDriver(new NebulaDriver());
        String url = "jdbc:nebula://192.168.8.6:3820/?user=root&password=Nebula123";
        try {
            Connection con    = DriverManager.getConnection(url);
            ResultSet  result = con.createStatement().executeQuery("use movie match(v) return v.id,v.name limit 2");
            while (result.next()) {
                System.out.print("id:" + result.getLong(1) + ", ");
                System.out.println("name:" + result.getString(2));
            }

            result = con.createStatement().executeQuery("use movie match(v1)-[e]-(v2) return v1,e,v2 limit 2");
            while (result.next()) {
                System.out.print("v1:" + result.getObject("v1").toString() + ", ");
                System.out.print("e:" + result.getObject("e").toString() + ", ");
                System.out.println("v2:" + result.getObject("v2").toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

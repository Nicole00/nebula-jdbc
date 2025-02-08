/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import java.util.Properties;

public class NebulaStatementTest {
    @Test
    public void testExecuteQuery() {
        Properties props = new Properties();
        props.setProperty("user", "root");
        props.setProperty("password", "Nebula123");
        try {
            NebulaConnection   connection = new NebulaConnection("jdbc:nebula://192.168.8.6:3820/movie", props);
            java.sql.ResultSet res        = connection.createStatement().executeQuery("for i in range(1,100) return i as c");
            assertTrue(res.isBeforeFirst());
            assertTrue(res.next());
            assertEquals(1, res.getInt("c"));
        } catch (Exception e) {
            e.printStackTrace();
            assert false;
        }
    }

    @Test
    public void testExecuteUpdate() {
        Properties props = new Properties();
        props.setProperty("user", "root");
        props.setProperty("password", "Nebula123");
        try {
            NebulaConnection   connection = new NebulaConnection("jdbc:nebula://192.168.8.6:3820/movie", props);
            int res        = connection.createStatement().executeUpdate("for i in range(1,100) return i as c");
            assertEquals(0, res);
            res = connection.createStatement().executeUpdate("insert or replace (@User{id:10})");
            assertEquals(1, res);
        } catch (Exception e) {
            e.printStackTrace();
            assert false;
        }
    }
}

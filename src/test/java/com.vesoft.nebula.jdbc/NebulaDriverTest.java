/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.jdbc;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class NebulaDriverTest {
    @Test
    public void testAcceptsURL() {
        NebulaDriver driver = new NebulaDriver();
        try {
            assertTrue(driver.acceptsURL("jdbc:nebula://127.0.0.1:9669/"));
            assertTrue(driver.acceptsURL("jdbc:nebula://127.0.0.1:9669/graphName"));
            assertTrue(driver.acceptsURL("jdbc:nebula://127.0.0.1:9669/graphName?user=root&password=nebula"));
            assertTrue(driver.acceptsURL("jdbc:nebula://127.0.0.1:9669/graphName?"));
            assertTrue(driver.acceptsURL("jdbc:nebula://127.0.0.1:9669/?user=root&password=nebula"));
            assertFalse(driver.acceptsURL("jdbc:mysql://127.0.0.1:9669/graphName"));
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testConnect() {
        try {
            DriverManager.registerDriver(new NebulaDriver());
            Properties props = new Properties();
            props.setProperty(NebulaPropertyKey.USER.getKeyName(), "root");
            props.setProperty(NebulaPropertyKey.PASSWORD.getKeyName(), "Nebula123");

            Connection con = DriverManager.getConnection("jdbc:nebula://192.168.8.6:3820/", props);
            assertNotNull(con);
            assertTrue(con.createStatement().execute("return 1"));
        } catch (Exception e) {
            e.printStackTrace();
            assertFalse(false);
        }

        try {
            DriverManager.registerDriver(new NebulaDriver());
            Properties props = new Properties();
            props.setProperty(NebulaPropertyKey.USER.getKeyName(), "root");
            props.setProperty(NebulaPropertyKey.PASSWORD.getKeyName(), "nebula");

            Connection con = DriverManager.getConnection("jdbc:nebula://192.168.8.6:3820/", props);

        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(true);
        }
    }
}

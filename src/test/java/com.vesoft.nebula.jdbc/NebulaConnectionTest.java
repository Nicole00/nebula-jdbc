/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.jdbc;

import com.vesoft.nebula.jdbc.statement.NebulaStatementImpl;
import org.junit.Test;
import java.util.Properties;

public class NebulaConnectionTest {
    @Test
    public void testExecuteWithWorkingGraph() {
        Properties props = new Properties();
        props.setProperty("user", "root");
        props.setProperty("password", "Nebula123");
        try {
            NebulaConnection connection = new NebulaConnection("jdbc:nebula://192.168.8.6:3820/movie", props);
            assert (connection.createStatement() instanceof NebulaStatementImpl);
            assert (connection.createStatement().execute("match(v) return v limit 1"));
        } catch (Exception e) {
            e.printStackTrace();
            assert false;
        }
    }

    @Test
    public void testExecuteWithoutWorkingGraph() {
        try {
            Properties props = new Properties();
            props.setProperty("user", "root");
            props.setProperty("password", "Nebula123");
            NebulaConnection connection = new NebulaConnection("jdbc:nebula://192.168.8.6:3820/", props);
            assert (connection.createStatement() instanceof NebulaStatementImpl);
            connection.createStatement().execute("match(v) return v limit 1");
        } catch (Exception e) {
            e.printStackTrace();
            assert (e.getMessage().contains("Current working graph not found"));
        }
    }

    @Test
    public void testExecuteWithSchema() {

        try {
            Properties props = new Properties();
            props.setProperty("user", "root");
            props.setProperty("password", "Nebula123");
            props.setProperty("schema", "/default_schema");
            NebulaConnection connection = new NebulaConnection("jdbc:nebula://192.168.8.6:3820/movie", props);
            assert (connection.createStatement() instanceof NebulaStatementImpl);
            assert (connection.createStatement().execute("match(v) return v limit 1"));
        } catch (Exception e) {
            e.printStackTrace();
            assert false;
        }
    }


    @Test
    public void testExecuteWithTimezone() {
        try {
            Properties props = new Properties();
            props.setProperty("user", "root");
            props.setProperty("password", "Nebula123");
            props.setProperty("timezone", "UTC");
            NebulaConnection connection = new NebulaConnection("jdbc:nebula://192.168.8.6:3820/movie", props);
            assert (connection.createStatement() instanceof NebulaStatementImpl);
            assert (connection.createStatement().execute("match(v) return v limit 1"));
        } catch (Exception e) {
            e.printStackTrace();
            assert false;
        }
    }
}

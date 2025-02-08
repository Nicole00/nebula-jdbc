/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.regex.Pattern;

public class UrlParser {
    private final static Logger  logger             = LoggerFactory.getLogger(UrlParser.class);
    public static final  String  JDBC_PREFIX        = "jdbc:";
    public static final  String  JDBC_NEBULA_PREFIX = JDBC_PREFIX + "nebula:";
    public static final  Pattern DB_PATH_PATTERN    = Pattern.compile("/([a-zA-Z0-9_*\\-]+)");

    public static Properties parse(String jdbcUrl, Properties properties) throws URISyntaxException {
        String uriString = jdbcUrl.substring(JDBC_PREFIX.length());
        URI    uri       = new URI(uriString);

        Properties jdbcProps = parseUriQuery(uri.getQuery(), properties);
        // parse graph name from url
        String path      = uri.getPath();
        String graphName = null;
        if (path != null && !path.isEmpty() && !path.equals("/")) {
            graphName = path.substring(1);
            jdbcProps.put(NebulaPropertyKey.DBNAME.getKeyName(), graphName);
        }
        jdbcProps.put(NebulaPropertyKey.ADDRESS.getKeyName(), getAddress(jdbcUrl));
        return jdbcProps;
    }

    private static Properties parseUriQuery(String query, Properties defaults) {
        if (query == null) {
            return defaults;
        }

        Properties urlProps  = new Properties(defaults);
        String[]   keyValues = query.split("&");
        for (String kv : keyValues) {
            String[] kvTokens = kv.split("=");
            if (kvTokens.length == 2) {
                urlProps.put(kvTokens[0].trim(), kvTokens[1].trim());
            } else {
                logger.error("cannot parse parameter pair:{}", kv);
                throw new RuntimeException("invalid url parameter:" + kv);
            }
        }
        return urlProps;
    }


    public static String getAddress(String url) {
        int startIndex = url.indexOf("//") + 2;
        int endIndex   = url.lastIndexOf("/");
        return url.substring(startIndex, endIndex);
    }
}

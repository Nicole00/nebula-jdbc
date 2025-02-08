# NebulaGraph JDBC Driver
> This is the description for the NebulaGraph JDBC Driver for NebulaGraph 5.0.

## Introduction
JDBC stands for "Java Database Connectivity" and is thus not bound exclusively to relational databases.
Nevertheless, JDBC’s terms, definitions, and behavior are highly influenced by SQL and relational databases. 
As NebulaGraph is a graph database with quite a different paradigm than relational and a non-standardized behaviour in some areas, 
there might be some details that don’t map 100% in each place, for example graph database has node, edge and path data type.

>Note: The NebulaGraph JDBC Driver requires JDK 8 on the client side and NebulaGraph 5.0+ on the server side.

## Download
### Include in a Maven build
```agsl
<dependency>
            <groupId>com.vesoft</groupId>
            <artifactId>nebula-jdbc</artifactId>
            <version>5.0-SNAPSHOT</version>
        </dependency>
```

### Include in a Gradle build
```agsl
 dependecies {
    implementation 'com.vesoft:nebula-jdbc:5.0-SNAPSHOT'
 }
```

## Quickstart
After adding the dependency to your application, you can use the NebulaGraph JDBC driver as any other JDBC driver.

TIP: The class name of the driver class is `com.vesoft.nebula.jdbc.NebulaDriver`.

Usage example:
```agsl
        Class.forName("com.vesoft.nebula.jdbc.NebulaDriver");
        String     url = "jdbc:nebula://192.168.8.6:3820/movie?user=root&password=Nebula123";
        Connection con = DriverManager.getConnection(url);

        Statement statement = con.createStatement();
        ResultSet result    = statement.executeQuery("use movie match(v) return v.id,v.name limit 2");
        while (result.next()) {
            System.out.println("id:" + result.getLong(1));
            System.out.println("name:" + result.getString(2));
        }
        statement.close();
        con.close();
```
1. Instantiate a JDBC connection.
2. Create a statement.
3. Execute a query.
4. Iterate over the result, as with any other JDBC result set.
5. JDBC's indexing starts at 1.
6. JDBC allows retrieval of result columns by index and name, the NebulaGraph JDBC driver also supports Node,Edge,Path,List and Record.

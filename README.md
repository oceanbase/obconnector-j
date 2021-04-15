<p align="center">
  <a href="http://oceanbase.com/">
    <img src="https://gw.alipayobjects.com/zos/bmw-prod/d6c1a0b7-c714-4429-8a33-2b394a5c1bf1.svg">
  </a>
</p>

# Oceanbase Client for Java
OceanBase Client for Java is a JDBC 4.2 compatible driver, used to connect applications developed in Java to OceanBase Database Server.

# Compatibility
## Server Compatibility
OceanBase Client for Java is compatible with all OceanBase Database Server versions.
## Java Compatibility
OceanBase Client for Java is developed based on java 8, please confirm your java version.
## Obtaining the driver
**For users who upgrade from version prior to 2.2.0:**

- Upgrade to JDK 8 if you are running any lower version.
- Change package path to `com.oceanbase.jdbc` .
- Change Maven groupId to `com.oceanbase` .

maven : 
```script
<dependency>
    <groupId>com.oceanbase</groupId>
    <artifactId>oceanbase-client</artifactId>
</dependency>
```
## Build from source
```script
 mvn clean package -DskipTests
 ```
## Documentation
For more information about this project, please refer to: 
* [About OceanBase](https://www.oceanbase.com/)
* [OceanBase documents](https://www.oceanbase.com/docs)
## License

Distributed under the LGPL License. See `LICENSE` for more information.
## Acknowledgement

OceanBase Connector/J was ported from MariaDB Connector/J with some OceanBase protocol support and improvement. Thanks to the MariaDB for opening up such a great Database Connector implementation.

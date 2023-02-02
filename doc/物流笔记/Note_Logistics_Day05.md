---
stypora-copy-images-to: img
typora-root-url: ./
---



# Logistics_Day05：实时增量ETL存储Kudu

![1612344442449](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612344442449.png)



## 01-[复习]-上次课程内容回顾 

> ​		主要讲解存储引擎：Kudu 入门使用，属于HDFS文件系统和HBase数据库这种产品，既能够批量数据加载分析，又能进行快速随机读写。
>

![1612485395935](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612485395935.png)

> 目前很多公司项目，都是用Kudu进行存储数据，使用Impala和SparkSQL进行分析查询数据：
>
> - 1）、Impala与Kudu集成，一对CP，满足OLAP分析：即席查询
> - 2）、Spark与Kudu集成，离线保存数据分析

![1612486606681](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612486606681.png)





## 02-[了解]-第5天：课程内容提纲 

> 主要讲解2个方面内容：搭建项目开发环境（Maven Project和相关配置）和数据实时ETL存储Kudu。

![1612486770785](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612486770785.png)

> - 1）、构建开发环境（在Windows下IDEA工具中开发项目，编写代码和测试）
>
>   - 第一、开发环境初始化，HADOOP在WINDOWS下设置
>   - 第二、项目工程Project初始化：创建Maven Project和Maven Module以及添加基础配
>
> - 2）、实时ETL模块开发
>
>   [编写StructuredStreaming结构化程序，实时从Kafka消费业务数据，进行ETL解析转换，存储到Kudu]()

![1612486928417](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612486928417.png)

> - 首先、构建实时ETL模块相关环境
> - 然后、编写流式计算程序，消费Kafka数据，打印控制台
> - 继续、编写工具类和代码抽象重构
> - 接着，编写KuduStreamApp流计算程序
> - 最后，解析消费Kafka的JSON数据，使用FastJson类库解析为JavaBean对象



## 03-[理解]-项目准备之开发环境初始化 

> ​		由于开发Spark程序在Windows开发，并且依赖HADOOP相关包（尤其HDFS 类库JAR包），所以需要配置HADOOP 在Windows下开发配置：
>
> - 1）、`winutils.exe`：放入在一个目录中【HADOOP_HOME】有bin目录，放入改文件

![1612487552862](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612487552862.png)

> - 2）、`hadoop.dll`：最好拷贝到`C:\Windows\System32`

![1612487662087](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612487662087.png)



## 04-[理解]-项目初始化之创建Maven工程及模块

> 打开IDEA，按照如下说明创建Maven Project和相关Maven Module（模块）。

![1612487779132](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612487779132.png)

> - 1）、创建父模块`itcast-logistics-parent`，删除总工程的`src`目录

![1612487800189](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612487800189.png)



> - 2）、创建`logistics-common`公共模块

![1612487912381](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612487912381.png)



> - 3）、创建`logistics-etl`实时ETL处理模块

![1612487966490](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612487966490.png)

> - 4）、创建`logistics-offline`离线指标计算模块

![1612488011362](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612488011362.png)

> ​		当上述三个模块创建完成以后，需要给每个模块（Module）添加Maven 工程目录结构（`scala、java、resources`）。

![1612488157177](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612488157177.png)



## 05-[理解]-项目初始化之导入POM依赖 

> ​		当项目MavenProject工程和Maven Module模块全部创建完成以后，需要添加依赖，[此处将后续需要依赖全部添加，在实际项目中，往往需要什么添加什么依赖即可。]()

- 1）、父工程【`itcast-logistics-parent`】依赖

```xml
    <repositories>
        <repository>
            <id>aliyun</id>
            <url>http://maven.aliyun.com/nexus/content/groups/public/</url>
        </repository>
        <repository>
            <id>cloudera</id>
            <url>https://repository.cloudera.com/artifactory/cloudera-repos/</url>
        </repository>
        <repository>
            <id>jboss</id>
            <url>http://repository.jboss.com/nexus/content/groups/public</url>
        </repository>
        <repository>
            <id>mvnrepository</id>
            <url>https://mvnrepository.com/</url>
            <!--<layout>default</layout>-->
        </repository>
        <repository>
            <id>elastic.co</id>
            <url>https://artifacts.elastic.co/maven</url>
        </repository>
    </repositories>

    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <!-- SDK -->
        <java.version>1.8</java.version>
        <scala.version>2.11</scala.version>
        <!-- Junit -->
        <junit.version>4.12</junit.version>
        <!-- HTTP Version -->
        <http.version>4.5.11</http.version>
        <!-- Hadoop -->
        <hadoop.version>3.0.0-cdh6.2.1</hadoop.version>
        <!-- Spark -->
        <spark.version>2.4.0-cdh6.2.1</spark.version>
       <!-- <spark.version>2.4.0</spark.version>-->
        <!-- Spark Graph Visual -->
        <gs.version>1.3</gs.version>
        <breeze.version>1.0</breeze.version>
        <jfreechart.version>1.5.0</jfreechart.version>
        <!-- Parquet -->
        <parquet.version>1.9.0-cdh6.2.1</parquet.version>
        <!-- Kudu -->
        <kudu.version>1.9.0-cdh6.2.1</kudu.version>
        <!-- Hive -->
        <hive.version>2.1.1-cdh6.2.1</hive.version>
        <!-- Kafka -->
        <!--<kafka.version>2.1.0-cdh6.2.1</kafka.version>-->
        <kafka.version>2.1.0</kafka.version>
        <!-- ClickHouse -->
        <clickhouse.version>0.2.2</clickhouse.version>
        <!-- ElasticSearch -->
        <es.version>7.6.1</es.version>
        <!-- JSON Version -->
        <fastjson.version>1.2.62</fastjson.version>
        <!-- Apache Commons Version -->
        <commons-io.version>2.6</commons-io.version>
        <commons-lang3.version>3.10</commons-lang3.version>
        <commons-beanutils.version>1.9.4</commons-beanutils.version>
        <!-- JDBC Drivers Version-->
        <ojdbc.version>12.2.0.1</ojdbc.version>
        <mysql.version>5.1.44</mysql.version>
        <!-- Other -->
        <jtuple.version>1.2</jtuple.version>
        <!-- Maven Plugins Version -->
        <maven-compiler-plugin.version>3.1</maven-compiler-plugin.version>
        <maven-surefire-plugin.version>2.19.1</maven-surefire-plugin.version>
        <maven-shade-plugin.version>3.2.1</maven-shade-plugin.version>
    </properties>

    <dependencyManagement>

        <dependencies>
            <!-- Scala -->
            <dependency>
                <groupId>org.scala-lang</groupId>
                <artifactId>scala-library</artifactId>
                <version>2.11.12</version>
            </dependency>
            <!-- Test -->
            <dependency>
                <groupId>junit</groupId>
                <artifactId>junit</artifactId>
                <version>${junit.version}</version>
                <scope>test</scope>
            </dependency>
            <!-- JDBC -->
            <dependency>
                <groupId>com.oracle.jdbc</groupId>
                <artifactId>ojdbc8</artifactId>
                <version>${ojdbc.version}</version>
                <systemPath>D:/BigdataUser/jdbc-drivers/ojdbc8-12.2.0.1.jar</systemPath>
                <scope>system</scope>
            </dependency>
            <dependency>
                <groupId>mysql</groupId>
                <artifactId>mysql-connector-java</artifactId>
                <version>${mysql.version}</version>
            </dependency>
            <!-- Http -->
            <dependency>
                <groupId>org.apache.httpcomponents</groupId>
                <artifactId>httpclient</artifactId>
                <version>${http.version}</version>
            </dependency>
            <!-- Apache Kafka -->
            <dependency>
                <groupId>org.apache.kafka</groupId>
                <artifactId>kafka_${scala.version}</artifactId>
                <version>${kafka.version}</version>
                <exclusions>
                    <exclusion>
                        <groupId>com.fasterxml.jackson.core</groupId>
                        <artifactId>jackson-core</artifactId>
                    </exclusion>
                </exclusions>
            </dependency>
            <!-- Spark -->
            <dependency>
                <groupId>org.apache.spark</groupId>
                <artifactId>spark-sql_${scala.version}</artifactId>
                <version>${spark.version}</version>
            </dependency>
            <dependency>
                <groupId>org.apache.spark</groupId>
                <artifactId>spark-sql-kafka-0-10_2.11</artifactId>
                <version>${spark.version}</version>
            </dependency>
            <dependency>
                <groupId>org.apache.parquet</groupId>
                <artifactId>parquet-common</artifactId>
                <version>${parquet.version}</version>
            </dependency>
            <dependency>
                <groupId>net.jpountz.lz4</groupId>
                <artifactId>lz4</artifactId>
                <version>1.3.0</version>
            </dependency>
            <!-- Graph Visual -->
            <dependency>
                <groupId>org.graphstream</groupId>
                <artifactId>gs-core</artifactId>
                <version>${gs.version}</version>
            </dependency>
            <dependency>
                <groupId>org.graphstream</groupId>
                <artifactId>gs-ui</artifactId>
                <version>${gs.version}</version>
            </dependency>
            <dependency>
                <groupId>org.scalanlp</groupId>
                <artifactId>breeze_${scala.version}</artifactId>
                <version>${breeze.version}</version>
            </dependency>
            <dependency>
                <groupId>org.scalanlp</groupId>
                <artifactId>breeze-viz_${scala.version}</artifactId>
                <version>${breeze.version}</version>
            </dependency>
            <dependency>
                <groupId>org.jfree</groupId>
                <artifactId>jfreechart</artifactId>
                <version>${jfreechart.version}</version>
            </dependency>
            <!-- JSON -->
            <dependency>
                <groupId>com.alibaba</groupId>
                <artifactId>fastjson</artifactId>
                <version>${fastjson.version}</version>
            </dependency>
            <!-- Kudu -->
            <dependency>
                <groupId>org.apache.kudu</groupId>
                <artifactId>kudu-client</artifactId>
                <version>${kudu.version}</version>
            </dependency>
            <dependency>
                <groupId>org.apache.kudu</groupId>
                <artifactId>kudu-spark2_2.11</artifactId>
                <version>${kudu.version}</version>
            </dependency>
            <!-- Hive -->
            <dependency>
                <groupId>org.apache.hive</groupId>
                <artifactId>hive-jdbc</artifactId>
                <version>${hive.version}</version>
            </dependency>
            <!-- Clickhouse -->
            <dependency>
                <groupId>ru.yandex.clickhouse</groupId>
                <artifactId>clickhouse-jdbc</artifactId>
                <version>${clickhouse.version}</version>
                <exclusions>
                    <exclusion>
                        <groupId>com.fasterxml.jackson.core</groupId>
                        <artifactId>jackson-databind</artifactId>
                    </exclusion>
                    <exclusion>
                        <groupId>com.fasterxml.jackson.core</groupId>
                        <artifactId>jackson-core</artifactId>
                    </exclusion>
                </exclusions>
            </dependency>
            <!-- ElasticSearch -->
            <dependency>
                <groupId>org.elasticsearch</groupId>
                <artifactId>elasticsearch</artifactId>
                <version>${es.version}</version>
            </dependency>
            <dependency>
                <groupId>org.elasticsearch.client</groupId>
                <artifactId>elasticsearch-rest-high-level-client</artifactId>
                <version>${es.version}</version>
            </dependency>
            <dependency>
                <groupId>org.elasticsearch.plugin</groupId>
                <artifactId>x-pack-sql-jdbc</artifactId>
                <version>${es.version}</version>
            </dependency>
            <dependency>
                <groupId>org.elasticsearch</groupId>
                <artifactId>elasticsearch-spark-20_2.11</artifactId>
                <version>${es.version}</version>
            </dependency>
            <!-- Alibaba Json -->
            <dependency>
                <groupId>com.alibaba</groupId>
                <artifactId>fastjson</artifactId>
                <version>${fastjson.version}</version>
            </dependency>
            <!-- Apache Commons -->
            <dependency>
                <groupId>commons-io</groupId>
                <artifactId>commons-io</artifactId>
                <version>${commons-io.version}</version>
            </dependency>
            <dependency>
                <groupId>org.apache.commons</groupId>
                <artifactId>commons-lang3</artifactId>
                <version>${commons-lang3.version}</version>
            </dependency>
            <dependency>
                <groupId>commons-beanutils</groupId>
                <artifactId>commons-beanutils</artifactId>
                <version>${commons-beanutils.version}</version>
            </dependency>
            <!-- Other -->
            <dependency>
                <groupId>org.javatuples</groupId>
                <artifactId>javatuples</artifactId>
                <version>${jtuple.version}</version>
            </dependency>
            <!--
            <dependency>
                <groupId>org.apache.httpcomponents</groupId>
                <artifactId>httpclient</artifactId>
                <version>4.5.3</version>
            </dependency>
            -->
            <dependency>
                <groupId>commons-httpclient</groupId>
                <artifactId>commons-httpclient</artifactId>
                <version>3.0.1</version>
            </dependency>
        </dependencies>
    </dependencyManagement>
```

> 注意：Oracle JDBC/ODBC驱动，没有提供Maven依赖，自己设置本地目录：`D:/BigdataUser/jdbc-drivers/ojdbc8-12.2.0.1.jar`

![1612488348184](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612488348184.png)



- 2）、公共模块【`logistics-common`】依赖

```xml
    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <maven.compiler.source>1.8</maven.compiler.source>
        <maven.compiler.target>1.8</maven.compiler.target>
    </properties>

    <dependencies>
        <!-- Scala -->
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
        </dependency>
        <!-- Test -->
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <scope>test</scope>
        </dependency>
        <!-- JDBC -->
        <dependency>
            <groupId>com.oracle.jdbc</groupId>
            <artifactId>ojdbc8</artifactId>
            <scope>system</scope>
        </dependency>
        <dependency>
            <groupId>mysql</groupId>
            <artifactId>mysql-connector-java</artifactId>
        </dependency>
        <!-- Http -->
        <dependency>
            <groupId>org.apache.httpcomponents</groupId>
            <artifactId>httpclient</artifactId>
        </dependency>
        <!-- Apache Commons -->
        <dependency>
            <groupId>commons-beanutils</groupId>
            <artifactId>commons-beanutils</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.commons</groupId>
            <artifactId>commons-lang3</artifactId>
        </dependency>
        <dependency>
            <groupId>commons-io</groupId>
            <artifactId>commons-io</artifactId>
        </dependency>
        <!-- Java Tuples -->
        <dependency>
            <groupId>org.javatuples</groupId>
            <artifactId>javatuples</artifactId>
        </dependency>
        <!-- Alibaba Json -->
        <dependency>
            <groupId>com.alibaba</groupId>
            <artifactId>fastjson</artifactId>
        </dependency>
        <!-- Apache Kafka -->
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka_${scala.version}</artifactId>
        </dependency>
        <!-- Spark -->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_${scala.version}</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql-kafka-0-10_2.11</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.parquet</groupId>
            <artifactId>parquet-common</artifactId>
        </dependency>
        <!-- Graph Visual -->
        <dependency>
            <groupId>org.graphstream</groupId>
            <artifactId>gs-core</artifactId>
        </dependency>
        <dependency>
            <groupId>org.graphstream</groupId>
            <artifactId>gs-ui</artifactId>
        </dependency>
        <dependency>
            <groupId>org.scalanlp</groupId>
            <artifactId>breeze_${scala.version}</artifactId>
        </dependency>
        <dependency>
            <groupId>org.scalanlp</groupId>
            <artifactId>breeze-viz_${scala.version}</artifactId>
        </dependency>
        <dependency>
            <groupId>org.jfree</groupId>
            <artifactId>jfreechart</artifactId>
        </dependency>
        <!-- Kudu -->
        <dependency>
            <groupId>org.apache.kudu</groupId>
            <artifactId>kudu-client</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.kudu</groupId>
            <artifactId>kudu-spark2_2.11</artifactId>
        </dependency>
        <!-- Clickhouse -->
        <dependency>
            <groupId>ru.yandex.clickhouse</groupId>
            <artifactId>clickhouse-jdbc</artifactId>
        </dependency>
        <!-- ElasticSearch -->
        <dependency>
            <groupId>org.elasticsearch</groupId>
            <artifactId>elasticsearch</artifactId>
        </dependency>
        <dependency>
            <groupId>org.elasticsearch.client</groupId>
            <artifactId>elasticsearch-rest-high-level-client</artifactId>
        </dependency>
        <!--
            <dependency>
                <groupId>org.elasticsearch.plugin</groupId>
                <artifactId>x-pack-sql-jdbc</artifactId>
            </dependency>
        -->
        <dependency>
            <groupId>org.elasticsearch</groupId>
            <artifactId>elasticsearch-spark-20_2.11</artifactId>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <configuration>
                    <source>8</source>
                    <target>8</target>
                </configuration>
            </plugin>
            <plugin>
                <groupId>net.alchim31.maven</groupId>
                <artifactId>scala-maven-plugin</artifactId>
                <version>3.2.0</version>
                <executions>
                    <execution>
                        <goals>
                            <goal>compile</goal>
                            <goal>testCompile</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
```



- 3）、实时ETL模块【`logistics-etl`】依赖

```xml
    <repositories>
        <repository>
            <id>mvnrepository</id>
            <url>https://mvnrepository.com/</url>
            <layout>default</layout>
        </repository>
        <repository>
            <id>cloudera</id>
            <url>https://repository.cloudera.com/artifactory/cloudera-repos/</url>
        </repository>
        <repository>
            <id>elastic.co</id>
            <url>https://artifacts.elastic.co/maven</url>
        </repository>
    </repositories>

    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <maven.compiler.source>1.8</maven.compiler.source>
        <maven.compiler.target>1.8</maven.compiler.target>
    </properties>

    <dependencies>
        <dependency>
            <groupId>cn.itcast.logistics</groupId>
            <artifactId>logistics-common</artifactId>
            <version>1.0.0</version>
        </dependency>
        <!-- Scala -->
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
        </dependency>
        <!-- Structured Streaming -->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_${scala.version}</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql-kafka-0-10_2.11</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.parquet</groupId>
            <artifactId>parquet-common</artifactId>
        </dependency>
        <dependency>
            <groupId>com.alibaba</groupId>
            <artifactId>fastjson</artifactId>
        </dependency>
        <!-- Other -->
        <dependency>
            <groupId>org.javatuples</groupId>
            <artifactId>javatuples</artifactId>
        </dependency>
        <dependency>
            <groupId>net.jpountz.lz4</groupId>
            <artifactId>lz4</artifactId>
        </dependency>
        <dependency>
            <groupId>org.jfree</groupId>
            <artifactId>jfreechart</artifactId>
        </dependency>
        <!-- kudu -->
        <dependency>
            <groupId>org.apache.kudu</groupId>
            <artifactId>kudu-client</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.kudu</groupId>
            <artifactId>kudu-spark2_2.11</artifactId>
        </dependency>
        <dependency>
            <groupId>commons-httpclient</groupId>
            <artifactId>commons-httpclient</artifactId>
            <version>3.0.1</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <configuration>
                    <source>8</source>
                    <target>8</target>
                </configuration>
            </plugin>
            <plugin>
                <groupId>net.alchim31.maven</groupId>
                <artifactId>scala-maven-plugin</artifactId>
                <version>3.2.0</version>
                <executions>
                    <execution>
                        <goals>
                            <goal>compile</goal>
                            <goal>testCompile</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
```



- 4）、离线指标计算模块【`logistics-offline`】依赖

```xml
   <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <maven.compiler.source>1.8</maven.compiler.source>
        <maven.compiler.target>1.8</maven.compiler.target>
    </properties>

    <dependencies>
        <dependency>
            <groupId>cn.itcast.logistics</groupId>
            <artifactId>logistics-common</artifactId>
            <version>1.0.0</version>
        </dependency>
        <!-- Scala -->
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
        </dependency>
        <!-- Structured Streaming -->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_${scala.version}</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.parquet</groupId>
            <artifactId>parquet-common</artifactId>
        </dependency>
        <dependency>
            <groupId>net.jpountz.lz4</groupId>
            <artifactId>lz4</artifactId>
        </dependency>
        <dependency>
            <groupId>org.jfree</groupId>
            <artifactId>jfreechart</artifactId>
        </dependency>
        <dependency>
            <groupId>com.alibaba</groupId>
            <artifactId>fastjson</artifactId>
        </dependency>
        <!-- kudu -->
        <dependency>
            <groupId>org.apache.kudu</groupId>
            <artifactId>kudu-client</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.kudu</groupId>
            <artifactId>kudu-spark2_2.11</artifactId>
        </dependency>
        <!-- Other -->
        <dependency>
            <groupId>org.javatuples</groupId>
            <artifactId>javatuples</artifactId>
        </dependency>
        <dependency>
            <groupId>commons-httpclient</groupId>
            <artifactId>commons-httpclient</artifactId>
            <version>3.0.1</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <configuration>
                    <source>8</source>
                    <target>8</target>
                </configuration>
            </plugin>
            <plugin>
                <groupId>net.alchim31.maven</groupId>
                <artifactId>scala-maven-plugin</artifactId>
                <version>3.2.0</version>
                <executions>
                    <execution>
                        <goals>
                            <goal>compile</goal>
                            <goal>testCompile</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
```



## 06-[掌握]-项目初始化之导入数据生成器模块 

> ​			在整个项目中，实时ETL程序，需要模拟产生实时业务数据，所以模块：【`logistics-generate`】，提供给大家，只需要将该模块导入至Maven Project即可。

- 1）、解压【`logistics-generate.zip`】到Maven Project目录【`D:\Logistics_New\workspace\itcast-logistics-parent`】

![1612489688102](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612489688102.png)



- 2）、在Maven Project中显示导入模块【`logistics-generate`】

![1612489757009](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612489757009.png)

选择解压后目录【`logistics-generate`】，截图如下所示：

![1612489811987](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612489811987.png)



- 3）、需要在父工程【`itcast-logistics-parent`】的POM文件中，手动添加该模块

![1612489908735](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612489908735.png)

> 经过上述三个步骤，导入Maven Module至Maven Project工程就OK啦。。。



- 4）、将数据生成器模块中目录【`table-data`】设置为资源目录，截图如下所示：

![1612490002901](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612490002901.png)

> 查看模块下代码，仅仅只有生成数据代码，其中`MockDataApp`为模拟CRM系统和物流系统业务数据代码。
>
> - 1）、`MockCrmDataApp`：模拟产生CRM系统业务数据（先清空CRM系统表数据，再插入数据）
> - 2）、`MockLogisticsDataApp`：模拟产生物流系统业务数据（先清空系统表数据，再插入数据）

![1612490105005](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612490105005.png)



## 07-[掌握]-项目初始化之构建公共模块 

> 每个项目初始化的时候，都有一些功能类和配置，需要进行补充。

- 1）、公共模块创建包，在公共模块【`logistics-common`】的`java`目录下，创建如下程序包：

![1612490645041](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612490645041.png)

创建包完成以后，截图如下所示：

![1612490658663](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612490658663.png)



- 2）、在公共模块【`logistics-common`】的`scala`目录下，创建如下程序包

![1612490699374](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612490699374.png)



- 3）、导入 JavaBean 对象，将：`资料\公共模块\beans`目录下文件导入到`common`包

![1612490727650](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612490727650.png)



- 4）、导入公共处理类，将：`资料\公共模块\utils`目录下文件导入到`common`包

![1612490751940](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612490751940.png)

> 至此，构建整个项目公共模块基本上配置完成。



## 08-[理解]-实时ETL开发之加载配置文件 

> 在进行实时ETL模块开发之前，依然需要进行基本配置，比如创建包和属性配置文件设置和加载。

- 1）、在实时ETL模块【`logistics-etl`】，创建包结构，由于使用Scala语言开发，所有在`scala`目录下创建包

![1612490911750](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612490911750.png)

创建完成以后，工程结构：

![1612490953924](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612490953924.png)



- 2）、针对每个项目来说，基本都有属性配置文件：`config.properties`，主要配置数据库连接信息，在不同环境测试，仅仅修改属性文件内容即可，不需要修改代码。

  > 在公共模块【`logistics-common`】的`resources`目录创建配置文件：`config.properties`。
  >
  > [有时候，属性配置文件放在conf文件中，此时该文件中value值，必须使用双引号引起来，否则解析时会有错误。]())
  >
  > ```ini
  > bigdata.host=“node2.itcast.cn”
  > ```
  >
  > 

  ==项目属性配置文件：`config.properties=`==

```properties

# CDH-6.2.1
bigdata.host=node2.itcast.cn

# HDFS
dfs.uri=hdfs://node2.itcast.cn:8020
# Local FS
local.fs.uri=file://

# Kafka
kafka.broker.host=node2.itcast.cn
kafka.broker.port=9092
kafka.init.topic=kafka-topics --zookeeper node2.itcast.cn:2181/kafka --create --replication-factor 1 --partitions 1 --topic logistics
kafka.logistics.topic=logistics
kafka.crm.topic=crm

# ZooKeeper
zookeeper.host=node2.itcast.cn
zookeeper.port=2181

# Kudu
kudu.rpc.host=node2.itcast.cn
kudu.rpc.port=7051
kudu.http.host=node2.itcast.cn
kudu.http.port=8051

# ClickHouse
clickhouse.driver=ru.yandex.clickhouse.ClickHouseDriver
clickhouse.url=jdbc:clickhouse://node2.itcast.cn:8123/logistics
clickhouse.user=root
clickhouse.password=123456

# ElasticSearch
elasticsearch.host=node2.itcast.cn
elasticsearch.rpc.port=9300
elasticsearch.http.port=9200

# Azkaban
app.first.runnable=true

# Oracle JDBC
db.oracle.url="jdbc:oracle:thin:@//192.168.88.10:1521/ORCL"
db.oracle.user=itcast
db.oracle.password=itcast

# MySQL JDBC
db.mysql.driver=com.mysql.jdbc.Driver
db.mysql.url=jdbc:mysql://192.168.88.10:3306/crm?useUnicode=true&characterEncoding=utf8&autoReconnect=true&failOverReadOnly=false
db.mysql.user=root
db.mysql.password=123456

## Data path of ETL program output ##
# Run in the yarn mode in Linux
spark.app.dfs.checkpoint.dir=/apps/logistics/dat-hdfs/spark-checkpoint
spark.app.dfs.data.dir=/apps/logistics/dat-hdfs/warehouse
spark.app.dfs.jars.dir=/apps/logistics/jars

# Run in the local mode in Linux
spark.app.local.checkpoint.dir=/apps/logistics/dat-local/spark-checkpoint
spark.app.local.data.dir=/apps/logistics/dat-local/warehouse
spark.app.local.jars.dir=/apps/logistics/jars

# Running in the local Mode in Windows
spark.app.win.checkpoint.dir=D://apps/logistics/dat-local/spark-checkpoint
spark.app.win.data.dir=D://apps/logistics/dat-local/warehouse
spark.app.win.jars.dir=D://apps/logistics/jars
```

> 需要编写工具类，解析属性文件中内容，获取每个属性的值，便于后期使用，创建对象：`Configuration`

![1612491317873](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612491317873.png)

```scala
package cn.itcast.logistics.common

import java.util.{Locale, ResourceBundle}

/**
 * 读取配置文件的工具类
 */
object Configuration {
	/**
	 * 定义配置文件操作的对象
	 */
	private lazy val resourceBundle: ResourceBundle = ResourceBundle.getBundle(
		"config", new Locale("zh", "CN")
	)
	private lazy val SEP = ":"
	
	// CDH-6.2.1
	lazy val BIGDATA_HOST: String = resourceBundle.getString("bigdata.host")
	
	// HDFS
	lazy val DFS_URI: String = resourceBundle.getString("dfs.uri")
	
	// Local FS
	lazy val LOCAL_FS_URI: String = resourceBundle.getString("local.fs.uri")
	
	// Kafka
	lazy val KAFKA_BROKER_HOST: String = resourceBundle.getString("kafka.broker.host")
	lazy val KAFKA_BROKER_PORT: Integer = Integer.valueOf(resourceBundle.getString("kafka.broker.port"))
	lazy val KAFKA_INIT_TOPIC: String = resourceBundle.getString("kafka.init.topic")
	lazy val KAFKA_LOGISTICS_TOPIC: String = resourceBundle.getString("kafka.logistics.topic")
	lazy val KAFKA_CRM_TOPIC: String = resourceBundle.getString("kafka.crm.topic")
	lazy val KAFKA_ADDRESS: String = KAFKA_BROKER_HOST + SEP + KAFKA_BROKER_PORT
	
	// Spark
	lazy val LOG_OFF = "OFF"
	lazy val LOG_DEBUG = "DEBUG"
	lazy val LOG_INFO = "INFO"
	lazy val LOCAL_HADOOP_HOME = "D:/BigdataUser/hadoop-3.0.0"
	lazy val SPARK_KAFKA_FORMAT = "kafka"
	lazy val SPARK_KUDU_FORMAT = "kudu"
	lazy val SPARK_ES_FORMAT = "es"
	lazy val SPARK_CLICK_HOUSE_FORMAT = "clickhouse"
	
	// ZooKeeper
	lazy val ZOOKEEPER_HOST: String = resourceBundle.getString("zookeeper.host")
	lazy val ZOOKEEPER_PORT: Integer = Integer.valueOf(resourceBundle.getString("zookeeper.port"))
	
	// Kudu
	lazy val KUDU_RPC_HOST: String = resourceBundle.getString("kudu.rpc.host")
	lazy val KUDU_RPC_PORT: Integer = Integer.valueOf(resourceBundle.getString("kudu.rpc.port"))
	lazy val KUDU_HTTP_HOST: String = resourceBundle.getString("kudu.http.host")
	lazy val KUDU_HTTP_PORT: Integer = Integer.valueOf(resourceBundle.getString("kudu.http.port"))
	lazy val KUDU_RPC_ADDRESS: String = KUDU_RPC_HOST + SEP + KUDU_RPC_PORT
	
	// ClickHouse
	lazy val CLICK_HOUSE_DRIVER: String = resourceBundle.getString("clickhouse.driver")
	lazy val CLICK_HOUSE_URL: String = resourceBundle.getString("clickhouse.url")
	lazy val CLICK_HOUSE_USER: String = resourceBundle.getString("clickhouse.user")
	lazy val CLICK_HOUSE_PASSWORD: String = resourceBundle.getString("clickhouse.password")
	
	// ElasticSearch
	lazy val ELASTICSEARCH_HOST: String = resourceBundle.getString("elasticsearch.host")
	lazy val ELASTICSEARCH_RPC_PORT: Integer = Integer.valueOf(resourceBundle.getString("elasticsearch.rpc.port"))
	lazy val ELASTICSEARCH_HTTP_PORT: Integer = Integer.valueOf(resourceBundle.getString("elasticsearch.http.port"))
	lazy val ELASTICSEARCH_ADDRESS: String = ELASTICSEARCH_HOST + SEP + ELASTICSEARCH_HTTP_PORT
	
	// Azkaban
	lazy val IS_FIRST_RUNNABLE: java.lang.Boolean = java.lang.Boolean.valueOf(resourceBundle.getString("app.first.runnable"))
	
	// ## Data path of ETL program output ##
	// # Run in the yarn mode in Linux
	lazy val SPARK_APP_DFS_CHECKPOINT_DIR: String = resourceBundle.getString("spark.app.dfs.checkpoint.dir") // /apps/logistics/dat-hdfs/spark-checkpoint
	lazy val SPARK_APP_DFS_DATA_DIR: String = resourceBundle.getString("spark.app.dfs.data.dir") // /apps/logistics/dat-hdfs/warehouse
	lazy val SPARK_APP_DFS_JARS_DIR: String = resourceBundle.getString("spark.app.dfs.jars.dir") // /apps/logistics/jars
	
	// # Run in the local mode in Linux
	lazy val SPARK_APP_LOCAL_CHECKPOINT_DIR: String = resourceBundle.getString("spark.app.local.checkpoint.dir") // /apps/logistics/dat-local/spark-checkpoint
	lazy val SPARK_APP_LOCAL_DATA_DIR: String = resourceBundle.getString("spark.app.local.data.dir") // /apps/logistics/dat-local/warehouse
	lazy val SPARK_APP_LOCAL_JARS_DIR: String = resourceBundle.getString("spark.app.local.jars.dir") // /apps/logistics/jars
	
	// # Running in the local Mode in Windows
	lazy val SPARK_APP_WIN_CHECKPOINT_DIR: String = resourceBundle.getString("spark.app.win.checkpoint.dir") // D://apps/logistics/dat-local/spark-checkpoint
	lazy val SPARK_APP_WIN_DATA_DIR: String = resourceBundle.getString("spark.app.win.data.dir") // D://apps/logistics/dat-local/warehouse
	lazy val SPARK_APP_WIN_JARS_DIR: String = resourceBundle.getString("spark.app.win.jars.dir") // D://apps/logistics/jars
	
	// # Oracle JDBC & # MySQL JDBC
	lazy val DB_ORACLE_URL: String = resourceBundle.getString("db.oracle.url")
	lazy val DB_ORACLE_USER: String = resourceBundle.getString("db.oracle.user")
	lazy val DB_ORACLE_PASSWORD: String = resourceBundle.getString("db.oracle.password")
	
	lazy val DB_MYSQL_DRIVER: String = resourceBundle.getString("db.mysql.driver")
	lazy val DB_MYSQL_URL: String = resourceBundle.getString("db.mysql.url")
	lazy val DB_MYSQL_USER: String = resourceBundle.getString("db.mysql.user")
	lazy val DB_MYSQL_PASSWORD: String = resourceBundle.getString("db.mysql.password")
	
	def main(args: Array[String]): Unit = {
		println(s"DFS URI = ${DFS_URI}")
		println(s"DB_MYSQL_URL = ${DB_MYSQL_URL}")
	}
}
```



## 09-[掌握]-实时ETL开发之流计算程序【模板】

> 首先编写流式计算程序，实时从Kafka消费业务数据，打印到控制台，具体操作说明如下：

![1612492723809](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612492723809.png)

![1612492729514](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612492729514.png)



**测试程序**：实时从Kafka消费数据（物流系统和CRM系统业务数据），将数据打印在控制台，没有任何逻辑

```ini
1. 初始化设置Spark Application配置
2. 判断Spark Application运行模式进行设置
3. 构建SparkSession实例对象

4. 初始化消费物流Topic数据参数
5. 消费物流Topic数据，打印控制台
6. 初始化消费CRM Topic数据参数
7. 消费CRM Topic数据，打印控制台

8. 启动流式应用，等待终止
```

```scala
package cn.itcast.logistics.etl

import org.apache.spark.sql.SparkSession

/**
 * 测试程序：实时从Kafka消费数据（物流系统和CRM系统业务数据），将数据打印在控制台，没有任何逻辑
 */
object LogisticsEtlApp {
	
	def main(args: Array[String]): Unit = {
		// step1. 构建SparkSession实例对象，设置相关属性
		// 1. 初始化设置Spark Application配置
		// 2. 判断Spark Application运行模式进行设置
		// 3. 构建SparkSession实例对象
		val spark: SparkSession = null
		
		// step2. 从Kafka数据源加载业务数据（CRM系统：crm和物流系统：logistics）
		// step3. 实时数据ETL转换操作
		// step4. 将数据打印到控制台Console
		// 4. 初始化消费物流Topic数据参数
		// 5. 消费物流Topic数据，打印控制台
		
		// 6. 初始化消费CRM Topic数据参数
		// 7. 消费CRM Topic数据，打印控制台
		

		// step5. 启动流式应用，需要等待终止
		// 8. 启动流式应用，等待终止
		spark.streams.awaitAnyTermination()
	}
	
}

```



## 10-[掌握]-实时ETL开发之流计算程序【编程】

> ​		将流计算程序模块构建完成，接下来需要分别实现每个步骤代码：创建SparkSession对象，加载数据和数据输出，具体代码如下所示：

```scala
package cn.itcast.logistics.etl

import cn.itcast.logistics.common.Configuration
import org.apache.commons.lang3.SystemUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 测试程序：实时从Kafka消费数据（物流系统和CRM系统业务数据），将数据打印在控制台，没有任何逻辑
 */
object LogisticsEtlApp {
	
	def main(args: Array[String]): Unit = {
		// step1. 构建SparkSession实例对象，设置相关属性
		// 1. 初始化设置Spark Application配置
		val sparkConf: SparkConf = new SparkConf()
			.set("spark.sql.session.timeZone", "Asia/Shanghai")
			.set("spark.sql.files.maxPartitionBytes", "134217728")
			.set("spark.sql.files.openCostInBytes", "134217728")
			.set("spark.sql.autoBroadcastJoinThreshold", "67108864")
		
		// 2. 判断Spark Application运行模式进行设置
		if (SystemUtils.IS_OS_WINDOWS || SystemUtils.IS_OS_MAC) {
			//本地环境LOCAL_HADOOP_HOME
			System.setProperty("hadoop.home.dir", Configuration.LOCAL_HADOOP_HOME)
			//设置运行环境和checkpoint路径
			sparkConf
				.set("spark.master", "local[*]")
				.set("spark.sql.shuffle.partitions", "4")
				.set("spark.sql.streaming.checkpointLocation", Configuration.SPARK_APP_WIN_CHECKPOINT_DIR)
		} else {
			//生产环境，提交应用：spark-submit --deploy-mode cluster --conf spark.sql.shuffle.partitions=10 xx.jar ...
			sparkConf
				.set("spark.master", "yarn")
				.set("spark.sql.streaming.checkpointLocation", Configuration.SPARK_APP_DFS_CHECKPOINT_DIR)
		}
		// 3. 构建SparkSession实例对象
		val spark: SparkSession = SparkSession.builder()
    		.appName(this.getClass.getSimpleName.stripSuffix("$"))
    		.config(sparkConf)
			.getOrCreate()
		import spark.implicits._
		spark.sparkContext.setLogLevel(Configuration.LOG_OFF)
		
		// step2. 从Kafka数据源加载业务数据（CRM系统：crm和物流系统：logistics）
		// step3. 实时数据ETL转换操作
		// step4. 将数据打印到控制台Console
		// 4. 初始化消费物流Topic数据参数
		val logisticsStreamDF: DataFrame = spark
			.readStream
			.format("kafka")
			.option("kafka.bootstrap.servers", Configuration.KAFKA_ADDRESS)
			.option("subscribe", Configuration.KAFKA_LOGISTICS_TOPIC)
			.load()
		// 5. 消费物流Topic数据，打印控制台
		val logisticsQuery = logisticsStreamDF
			.selectExpr("CAST(value AS STRING)")
			.writeStream
			.outputMode(OutputMode.Append())
			.queryName("query-logistics")
			.format("console")
			.option("numRows", "10")
			.option("truncate", "false")
			.start()
		
		// 6. 初始化消费CRM Topic数据参数
		val crmStreamDF: DataFrame = spark
			.readStream
			.format("kafka")
			.option("kafka.bootstrap.servers", Configuration.KAFKA_ADDRESS)
			.option("subscribe", Configuration.KAFKA_CRM_TOPIC)
			.load()
		// 7. 消费CRM Topic数据，打印控制台
		val crmQuery = crmStreamDF
			.selectExpr("CAST(value AS STRING)")
			.writeStream
			.outputMode(OutputMode.Append())
			.queryName("query-crm")
			.format("console")
			.option("numRows", "10")
			.option("truncate", "false")
			.start()

		// step5. 启动流式应用，需要等待终止
		// 8. 启动流式应用，等待终止
		spark.streams.awaitAnyTermination()
	}
	
}

```

> SparkSQL 参数调优设置：
>
> - 1）、设置会话时区：`set("spark.sql.session.timeZone", "Asia/Shanghai")`
>
> - 2）、设置读取文件时单个分区可容纳的最大字节数
>
>   `set("spark.sql.files.maxPartitionBytes", "134217728")`
>
> - 3）、设置合并小文件的阈值：`set("spark.sql.files.openCostInBytes", "134217728")`
>
> - 4）、设置 shuffle 分区数：`set("spark.sql.shuffle.partitions", "4")`
>
> - 5）、设置执行 join 操作时能够广播给所有 worker 节点的最大字节大小
>
>   `set("spark.sql.autoBroadcastJoinThreshold", "67108864")`



## 11-[掌握]-实时ETL开发之流计算程序【测试】

> ​			前面已经编写完成流式计算程序，启动MySQL数据库和Canal或Oracle数据库和OGG，实时产生业务数据，运行流式计算程序是否运行正常。

- 1）、启动MySQL数据库和Canal采集CRM系统业务数据

```ini
使用VMWare 启动node1.itcast.cn虚拟机，使用root用户（密码123456）登录
1) 启动MySQL数据库
	# 查看容器
	[root@node1 ~]# docker ps -a
	8b5cd2152ed9        mysql:5.7     0.0.0.0:3306->3306/tcp   mysql	
	
	# 启动容器
	[root@node1 ~]# docker start mysql
	myoracle
	
	# 容器状态
	[root@node1 ~]# docker ps
	8b5cd2152ed9        mysql:5.7   Up 6 minutes        0.0.0.0:3306->3306/tcp   mysql	

2) 启动CanalServer服务
	# 查看容器
	[root@node1 ~]# docker ps -a
	28888fad98c9        canal/canal-server:v1.1.2        0.0.0.0:11111->11111/tcp   canal-server
	
	# 启动容器
	[root@node1 ~]# docker start canal-server
	myoracle
	
	# 容器状态
	[root@node1 ~]# docker ps
	28888fad98c9        canal/canal-server:v1.1.2       Up 2 minutes  0.0.0.0:11111->11111/tcp   canal-server	
	
	# 进入容器
	[root@node1 ~]# docker exec -it canal-server /bin/bash
	[root@28888fad98c9 admin]# 
	
	# 进入CanalServer启动脚本目录
	[root@28888fad98c9 admin]# cd canal-server/bin/
	
	# 重启CanalServer服务
	[root@28888fad98c9 bin]# ./restart.sh 
	
	# 退出容器
	[root@28888fad98c9 bin]# exit

	

```



- 2）、使用CM启动Zookeeper和Kafka服务

![1612494540331](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612494540331.png)



- 3）、运行流式程序

> 由于流式应用程序，运行时检查点目录在D盘下，运行之前，如果目录存在，先删除。

![1612494769161](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612494769161.png)



> 可以继续启动Oracle数据库和OGG服务，实时产生数据，查看流式程序消费情况，具体步骤与上述类似省略。



## 12-[掌握]-实时ETL开发之实时业务数据测试

> 运行模拟数据生成器程序，实时产生业务数据，插入到数据库中（CRM数据库和物流数据库）。

- 1）、首先启动流式应用程式（启动之前，删除以前Checkpoint检查点目录）
- 2）、运行CRM系统数据生成器程序：`MockCrmDataApp`

> 数据生成器程序逻辑：先清空truncate数据库中所有表的数据，然后在插入数据。

![1612495374673](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612495374673.png)

> 运行MockCrmDataApp程序，查看KafkaTopic中是否有数据以及流式计算程序是否消费到数据。

- 3）、运行Logistics物流系统数据生成器程序：`MockLogisticsDataApp`

> 由于物流系统有48张表，为了测试选择2表数据清空和生成即可。

![1612495659099](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612495659099.png)

> 此外，需要设置先清空数据，如下图所示：

![1612495746517](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612495746517.png)



## 13-[掌握]-实时ETL开发之封装流计算公共接口

> 由于项目实时ETL需要编写三个流式计算程序，分别将消费数据进行过ETL转换后存储到不同Sink中。
>
> - 1）、SparkSession对象：[提取工具类创建，传递参数]()
> - 2）、加载Kafka数据：[都是相同，消费2个Topic数据]()
> - 3）、数据ETL转换：**每个应用不同**
> - 4）、输出到终端Sink：**每个应用不同**
>
> ==编写流式计算公共接口：Trait特质，有具体方法（共有功能，相同代码）和抽象方法（每个子类独有功能，代码不一样）==

![1612496987519](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612496987519.png)

> ​		Structured Streaming 流处理程序消费Kafka数据以后，会将数据分别存储到Kudu、ES、ClickHouse中，因此可以根据存储介质不同，封装其公共接口，每个流处理程序继承自该接口。
>
> 编写特质Trait：`BasicStreamApp`

![1612497353454](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612497353454.png)

```scala
package cn.itcast.logistics.etl.realtime

import cn.itcast.logistics.common.Configuration
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 所有ETL流式处理的基类，实时增量ETL至：Kudu、Elasticsearch和ClickHouse都要实现此基类，定义三个方法
	 * - 1. 加载数据：load
	 * - 2. 处理数据：process
	 * - 3. 保存数据：save
 */
trait BasicStreamApp {

	/**
	 * 读取数据的方法
	 *
	 * @param spark SparkSession
	 * @param topic 指定消费的主题
	 * @param selectExpr 默认值：CAST(value AS STRING)
	 * @return 从Kafka消费流式Stream DataFrame，默认情况下，其中仅仅包含value字段，类型String
	 */
	def load(spark: SparkSession, topic: String,
	         selectExpr: String = "CAST(value AS STRING)"): DataFrame = {
		spark
			.readStream
			.format("kafka")
			.option("kafka.bootstrap.servers", Configuration.KAFKA_ADDRESS)
			.option("subscribe", topic)
			.load()
			.selectExpr(selectExpr)
	}
	
	/**
	 * 数据的处理
	 *
	 * @param streamDF 流式数据集StreamingDataFrame
	 * @param category 业务数据类型，比如物流系统业务数据，CRM系统业务数据等
	 * @return 流式数据集StreamingDataFrame
	 */
	def process(streamDF: DataFrame, category: String): DataFrame
	
	/**
	 * 数据的保存
	 *
	 * @param streamDF 保存数据集DataFrame
	 * @param tableName 保存表的名称
	 * @param isAutoCreateTable 是否自动创建表，默认创建表
	 */
	def save(streamDF: DataFrame, tableName: String, isAutoCreateTable: Boolean = true): Unit
	
}

```



> 此外将创建SparkSession实例对象代码封装到工具类：SparkUtils中，具体进行编码实现即可。



## 14-[掌握]-实时ETL开发之SparkUtils工具类

> ​		在实际项目中，往往就一些连接对象，放在工具类进行创建，比如SparkSession实例对象构建，创建`SparkUtils`工具类：
>
> ```ini
> // 1. 初始化设置Spark Application配置
> 	sparkConf
> 
> // 2. 判断Spark Application运行模式进行设置
> 	autoSettingEnv
> 
> // 3. 构建SparkSession实例对象
> 	createSparkSession
> ```
>
> 公共模块【logistics-commo】的scala目录的common程序包下创建 SparkUtils 单例对象

![1612498215155](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612498215155.png)

> 截图代码如下所示：

```scala
package cn.itcast.logistics.common

import org.apache.commons.lang3.SystemUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Spark 操作的工具类
 */
object SparkUtils {

	// 将匿名函数赋值变量
	/**
	 * 创建sparkConf对象
	 */
	lazy val sparkConf = () => {
		val conf: SparkConf = new SparkConf()
			//设置时区
			.set("spark.sql.session.timeZone", "Asia/Shanghai")
			//设置单个分区可容纳的最大字节数，默认是128M， 等同于block块的大小
			.set("spark.sql.files.maxPartitionBytes", "134217728")
			//设置合并小文件的阈值，避免每个小文件占用一个分区的情况
			.set("spark.sql.files.openCostInBytes", "134217728")
			//设置join操作时可以广播到worker节点的最大字节大小，可以避免shuffle操作
			.set("spark.sql.autoBroadcastJoinThreshold", "67108864")
		//返回sparkConf对象
		conf
	}
	
	/**
	 * 预定义可用于window和linux中的运行模式
	 */
	lazy val autoSettingEnv = (sparkConf: SparkConf) => {
		// 依据应用程序运行环境，设置运行模式
		if (SystemUtils.IS_OS_WINDOWS || SystemUtils.IS_OS_MAC) {
			//本地环境LOCAL_HADOOP_HOME
			System.setProperty("hadoop.home.dir", Configuration.LOCAL_HADOOP_HOME)
			//设置运行环境和checkpoint路径
			sparkConf
				.set("spark.master", "local[*]")
				.set("spark.sql.shuffle.partitions", "4")
				.set("spark.sql.streaming.checkpointLocation", Configuration.SPARK_APP_WIN_CHECKPOINT_DIR)
		} else {
			//生产环境，提交应用：spark-submit --deploy-mode cluster --conf spark.sql.shuffle.partitions=10 xx.jar ...
			sparkConf
				.set("spark.master", "yarn")
				.set("spark.sql.streaming.checkpointLocation", Configuration.SPARK_APP_DFS_CHECKPOINT_DIR)
		}
		// 返回SparkConf对象
		sparkConf
	}
	
	/**
	 * 创建sparkSession对象
	 */
	def createSparkSession(sparkConf: SparkConf, clazz: Class[_]): SparkSession = {
		SparkSession.builder()
			.appName(clazz.getSimpleName.stripSuffix("$"))
			.config(sparkConf)
			.getOrCreate()
	}
	
}

```



## 15-[理解]-实时ETL开发之KuduStreamApp程序

> 编写第一个结构化流程序`KuduStreamApp`，==从Kafka消费数据，进行ETL转换处理，存储到Kudu表==。

![1612507205668](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612507205668.png)

> 创建对象：`KuduStreamApp`，在实时ETL模块下包：`cn.itcast.logistics.etl.realtime`。

实时Kudu ETL应用程序入口，数据处理逻辑步骤：

```ini
step1. 创建SparkSession实例对象，传递SparkConf
step2. 从Kafka数据源实时消费数据
step3. 对JSON格式字符串数据进行转换处理
step4. 获取消费每条数据字段信息
step5. 将解析过滤获取的数据写入同步至Kudu表
```

```scala
package cn.itcast.logistics.etl.realtime
import cn.itcast.logistics.common.{Configuration, SparkUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Kudu数据管道应用：实现Kudu数据库的实时ETL操作
 */
object KuduStreamApp extends BasicStreamApp {
	
	/**
	 * 数据的处理
	 *
	 * @param streamDF 流式数据集StreamingDataFrame
	 * @param category 业务数据类型，比如物流系统业务数据，CRM系统业务数据等
	 * @return 流式数据集StreamingDataFrame
	 */
	override def process(streamDF: DataFrame, category: String): DataFrame = ???
	
	/**
	 * 数据的保存
	 *
	 * @param streamDF          保存数据集DataFrame
	 * @param tableName         保存表的名称
	 * @param isAutoCreateTable 是否自动创建表，默认创建表
	 */
	override def save(streamDF: DataFrame, tableName: String, isAutoCreateTable: Boolean): Unit = ???
	
	
	/** MAIN 方法入口：Spark Application应用程序入口，必须创建SparkContext对象或SpakrSession对象 */
	def main(args: Array[String]): Unit = {
		// step1. 获取SparkSession对象
		val spark: SparkSession = SparkUtils.createSparkSession(
			SparkUtils.autoSettingEnv(SparkUtils.sparkConf()), this.getClass
		)
		import spark.implicits._
		spark.sparkContext.setLogLevel(Configuration.LOG_OFF)
		
		// step2. 从Kafka加载数据：物流系统业务数据logistics和CRM系统业务数据crm
		val logisticsStreamDF: DataFrame = load(spark, "logistics")
		val crmStreamDF: DataFrame = load(spark, "crm")
		
		// step3. 对流式数据进行实时ETL转换操作
		val logisticsEtlStreamDF = process(logisticsStreamDF, "logistics")
		val crmEtlStreamDF = process(crmStreamDF, "crm")
		
		// step4. 将转换后数据进行输出
		save(logisticsEtlStreamDF, "tbl_logistics")
		save(crmEtlStreamDF, "tbl_crm")
		
		// step5. 启动流式应用后，等待终止
		spark.streams.active.foreach(query => println(s"正在启动Query： ${query.name}"))
		spark.streams.awaitAnyTermination()
	}
}

```

> 按照编写StructuredStreaming程序逻辑，将MAIN方法代码编写完成，接下来，需要具体实现其中：
>
> - 1）、process：流式数据ETL方法，[此处先不做任何转换，进行输出而已，后续进行ETL转换]()
> - 2）、save：将数据保存外部系统方法，[此处目前仅仅将数据打印在控制台而已，后续再保存至Kudu表]()

```scala
package cn.itcast.logistics.etl.realtime

import cn.itcast.logistics.common.{Configuration, SparkUtils}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Kudu数据管道应用：实现Kudu数据库的实时ETL操作
 */
object KuduStreamApp extends BasicStreamApp {
	
	/**
	 * 数据的处理
	 *
	 * @param streamDF 流式数据集StreamingDataFrame
	 * @param category 业务数据类型，比如物流系统业务数据，CRM系统业务数据等
	 * @return 流式数据集StreamingDataFrame
	 */
	override def process(streamDF: DataFrame, category: String): DataFrame = {
		// 1. 依据不同业务系统数据进行不同的ETL转换
		val etlStreamDF: DataFrame = category match {
			// TODO: 物流系统业务数据ETL转换
			case "logistics" =>
				streamDF
			
			// TODO：CRM系统业务数据ETL转换
			case "crm" =>
				streamDF
			
			// TODO: 其他业务数据数据，直接返回即可
			case _ => streamDF
		}
		// 2. 返回转换后流数据
		etlStreamDF
	}
	
	/**
	 * 数据的保存
	 *
	 * @param streamDF          保存数据集DataFrame
	 * @param tableName         保存表的名称
	 * @param isAutoCreateTable 是否自动创建表，默认创建表
	 */
	override def save(streamDF: DataFrame, tableName: String, isAutoCreateTable: Boolean): Unit = {
		streamDF.writeStream
			.outputMode(OutputMode.Append())
			.queryName(s"query-${tableName}")
			.format("console")
			.option("numRows", "10")
			.option("truncate", "false")
			.start()
	}
	
	/** MAIN 方法入口：Spark Application应用程序入口，必须创建SparkContext对象或SpakrSession对象 */
	def main(args: Array[String]): Unit = {
		// step1. 获取SparkSession对象
		val spark: SparkSession = SparkUtils.createSparkSession(
			SparkUtils.autoSettingEnv(SparkUtils.sparkConf()), this.getClass
		)
		spark.sparkContext.setLogLevel(Configuration.LOG_OFF)
		
		// step2. 从Kafka加载数据：物流系统业务数据logistics和CRM系统业务数据crm
		val logisticsStreamDF: DataFrame = load(spark, "logistics")
		val crmStreamDF: DataFrame = load(spark, "crm")
		
		// step3. 对流式数据进行实时ETL转换操作
		val logisticsEtlStreamDF = process(logisticsStreamDF, "logistics")
		val crmEtlStreamDF = process(crmStreamDF, "crm")
		
		// step4. 将转换后数据进行输出
		save(logisticsEtlStreamDF, "tbl_logistics")
		save(crmEtlStreamDF, "tbl_crm")
		
		// step5. 启动流式应用后，等待终止
		spark.streams.active.foreach(query => println(s"正在启动Query： ${query.name}"))
		spark.streams.awaitAnyTermination()
	}
}

```

> 编码完成以后，可以测试一下，分别对CRM系统和Logistics系统数据库表的数据记性CUD操作，查看是否消费

## 16-[理解]-实时ETL开发之Kafka数据JSON格式 

> 当使用OGG或Canal实时采集数据时，发送到Kafka Topic中数据都是JSON格式数据，接下来看看数据格式。

- 1）、OGG 采集JSON格式数据，[INSERT插入、UPDATE更新和DELTE删除数据]()

![1612508510172](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612508510172.png)

具体查看三种类型（INSERT插入、UPDATE更新和DELTE删除）JSON数据：

![1612508739612](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612508739612.png)

> 所以，当解析OGG采集JSON数据时，将JSON解析到JavaBean对象中，字段有7个。



- 2）、Canal采集MySQL数据库表数据时，字段信息

![1612508870809](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612508870809.png)

具体查看三种类型（INSERT插入、UPDATE更新和DELTE删除）JSON数据：

![1612509102518](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612509102518.png)

> 使用Canal采集数据JSON中有12个字段，不同操作类型，有的有值，有的没有值。



## 17-[掌握]-实时ETL开发之定义数据Bean对象 

> ​		前面已经分析JSON格式数据结构，OGG采集和Canal采集数据结构有所不同，所有需要定义2个JavaBean，分别封装JSON格式数据，[但是发现2中JSON格式数据，都有操作类型字段，所以可以先编写一个基类，仅有一个字段：table。]()
>
> ==定义Bean对象，全部使用Java语言定义，并没有使用Scala中CaseClass样例类定义。==

- 1）、定义 Bean 对象基类：`MessageBean`

![1612509371572](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612509371572.png)

```scala
package cn.itcast.logistics.common.beans.parser;

import java.io.Serializable;

/**
 * 根据数据源定义抽象类，数据源：ogg 和 canal， 两者有共同的table属性
 */
public abstract class MessageBean implements Serializable {

	private static final long serialVersionUID = 373363837132138843L;

	private String table;

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	@Override
	public String toString() {
		return table;
	}

}
```

- 2）、定义 OGG 数据 Bean 对象，总共7个字段

![1612509561500](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612509561500.png)

```scala
package cn.itcast.logistics.common.beans.parser;

import java.util.Map;

/**
 * 定义消费OGG数据的JavaBean对象
 * {
 *     "table": "ITCAST.tbl_route",            //表名：库名.表名
 *     "op_type": "U",                         //操作类型：U表示修改
 *     "op_ts": "2020-10-08 09:10:54.000774",
 *     "current_ts": "2020-10-08T09:11:01.925000",
 *     "pos": "00000000200006645758",
 *     "before": {                            //操作前的字段集合
 *        "id": 104,
 *        "start_station": "东莞中心",
 *        "start_station_area_id": 441900,
 *        "start_warehouse_id": 1,
 *        "end_station": "蚌埠中转部",
 *        "end_station_area_id": 340300,
 *        "end_warehouse_id": 107,
 *        "mileage_m": 1369046,
 *        "time_consumer_minute": 56172,
 *        "state": 1,
 *        "cdt": "2020-02-02 18:51:39",
 *        "udt": "2020-02-02 18:51:39",
 *        "remark": null
 *        },
 *     "after": {                         //操作后的字段集合
 *        "id": 104,
 *        "start_station": "东莞中心",
 *        "start_station_area_id": 441900,
 *        "start_warehouse_id": 1,
 *        "end_station": "TBD",
 *        "end_station_area_id": 340300,
 *        "end_warehouse_id": 107,
 *        "mileage_m": 1369046,
 *        "time_consumer_minute": 56172,
 *        "state": 1,
 *        "cdt": "2020-02-02 18:51:39",
 *        "udt": "2020-02-02 18:51:39",
 *        "remark": null
 *    }
 * }
 */
public class OggMessageBean extends MessageBean {

	private static final long serialVersionUID = -9063091624956235851L;

	//定义操作类型
	private String op_type;

	@Override
	public void setTable(String table) {
		//如果表名不为空
		if (table != null && !table.equals("")) {
			table = table.replaceAll("[A-Z]+\\.", "");
		}
		super.setTable(table);
	}

	public String getOp_type() {
		return op_type;
	}

	public void setOp_type(String op_type) {
		this.op_type = op_type;
	}

	public String getOp_ts() {
		return op_ts;
	}

	public void setOp_ts(String op_ts) {
		this.op_ts = op_ts;
	}

	public String getCurrent_ts() {
		return current_ts;
	}

	public void setCurrent_ts(String current_ts) {
		this.current_ts = current_ts;
	}

	public String getPos() {
		return pos;
	}

	public void setPos(String pos) {
		this.pos = pos;
	}

	public Map<String, Object> getBefore() {
		return before;
	}

	public void setBefore(Map<String, Object> before) {
		this.before = before;
	}

	public Map<String, Object> getAfter() {
		return after;
	}

	public void setAfter(Map<String, Object> after) {
		this.after = after;
	}

	//操作时间
	private String op_ts;

	@Override
	public String toString() {
		return "OggMessageBean{" +
			"table='" + super.getTable() + '\'' +
			", op_type='" + op_type + '\'' +
			", op_ts='" + op_ts + '\'' +
			", current_ts='" + current_ts + '\'' +
			", pos='" + pos + '\'' +
			", before=" + before +
			", after=" + after +
			'}';
	}

	/**
	 * 返回需要处理的列的集合
	 * @return
	 */
	public Map<String, Object> getValue() {
		//如果执行的是删除操作，则返回before节点的列的集合，如果执行的是插入和更新操作，则返回after节点的列的集合
		if (after == null) {
			return before;
		} else {
			return after;
		}
	}

	//同步时间
	private String current_ts;
	//偏移量
	private String pos;
	//操作之前的数据
	private Map<String, Object> before;
	//操作之后的数据
	private Map<String, Object> after;
}

```



- 3）、定义 Canal 数据 Bean 对象，总共12个字段

![1612509747826](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612509747826.png)

```scala
package cn.itcast.logistics.common.beans.parser;

import java.util.List;
import java.util.Map;

/**
 * 定义消费canal数据对应的JavaBean对象
 * {
 *     "data": [{
 *        "id": "1",
 *        "name": "北京",
 *        "tel": "222",
 *        "mobile": "1111",
 *        "detail_addr": "北京",
 *        "area_id": "1",
 *        "gis_addr": "1",
 *        "cdt": "2020-10-08 17:20:12",
 *        "udt": "2020-11-05 17:20:16",
 *        "remark": null
 *        }],
 *     "database": "crm",
 *     "es": 1602148867000,
 *     "id": 15,
 *     "isDdl": false,
 *     "mysqlType": {
 *        "id": "bigint(20)",
 *        "name": "varchar(50)",
 *        "tel": "varchar(20)",
 *        "mobile": "varchar(20)",
 *        "detail_addr": "varchar(100)",
 *        "area_id": "bigint(20)",
 *        "gis_addr": "varchar(20)",
 *        "cdt": "datetime",
 *        "udt": "datetime",
 *        "remark": "varchar(100)"
 *    },
 *     "old": [{
 *        "tel": "111"
 *    }],
 *     "sql": "",
 *     "sqlType": {
 *        "id": -5,
 *        "name": 12,
 *        "tel": 12,
 *        "mobile": 12,
 *        "detail_addr": 12,
 *        "area_id": -5,
 *        "gis_addr": 12,
 *        "cdt": 93,
 *        "udt": 93,
 *        "remark": 12
 *    },
 *     "table": "crm_address",
 *     "ts": 1602148867311,
 *     "type": "UPDATE"               //修改数据
 * }
 */
public class CanalMessageBean extends MessageBean {

	private static final long serialVersionUID = -3147101694588578078L;

	//操作的数据集合
	private List<Map<String, Object>> data;

	public List<Map<String, Object>> getData() {
		return data;
	}

	public void setData(List<Map<String, Object>> data) {
		this.data = data;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public Long getEs() {
		return es;
	}

	public void setEs(Long es) {
		this.es = es;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public boolean isDdl() {
		return isDdl;
	}

	public void setDdl(boolean ddl) {
		isDdl = ddl;
	}

	public Map<String, Object> getMysqlType() {
		return mysqlType;
	}

	public void setMysqlType(Map<String, Object> mysqlType) {
		this.mysqlType = mysqlType;
	}

	public String getOld() {
		return old;
	}

	public void setOld(String old) {
		this.old = old;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public Map<String, Object> getSqlType() {
		return sqlType;
	}

	public void setSqlType(Map<String, Object> sqlType) {
		this.sqlType = sqlType;
	}


	public Long getTs() {
		return ts;
	}

	public void setTs(Long ts) {
		this.ts = ts;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	//数据库名称
	private String database;
	private Long es;
	private Long id;
	private boolean isDdl;
	private Map<String, Object> mysqlType;
	private String old;
	private String sql;
	private Map<String, Object> sqlType;
	private Long ts;
	private String type;

	/**
	 * 重写父类的settable方法，将表名修改成统一的前缀
	 * @param table
	 */
	@Override
	public void setTable(String table) {
		if(table!=null && !table.equals("")){
			if(table.startsWith("crm_")) {
				table = table.replace("crm_", "tbl_");
			}
		}
		super.setTable(table);
	}
}

```



## 18-[掌握]-实时ETL开发之数据转换Bean及测试 

> 前面已经将OGG和Canal采集数据封装的JavaBean类创建完成，接下来：==解析JSON字符串为JavaBean对象。==
>
> - 1）、如何解析JSON字符串
>
>   [使用阿里巴巴开源JSON库：FastJson库，官网：https://github.com/alibaba/fastjson]()
>
>   添加MAVEN依赖：
>
>   ```xml
>   <dependency>
>       <groupId>com.alibaba</groupId>
>       <artifactId>fastjson</artifactId>
>       <version>1.2.73</version>
>   </dependency>
>   ```
>
>   [以后，只要数据使用Canal采集，就要想到使用FastJson解析]()
>
> - 2）、为什么使用FastJson呢？不适用其他类库呢？？？
>
>   [FastJson类库，使用比较简单：==JSON -> JavaBean对象和JavaBean对象 -> JSON字符串==]()
>
>   ```java
>   		val jsonStr: String = ""
>   		JSON.parseObject(jsonStr, classOf[BeanTest])
>   		
>   		val bean = new BeanTest(101, "zhangsan")
>   		val jsonBean: AnyRef = JSON.toJSONString(bean, true)
>   ```
>
> - 3）、**当使用FastJson解析字符串为Bean对象时，最好是Java语言定义JavaBean类**，不要使用Scala语言。原因由于FastJson对Scala语言中CaseClass支持不友好（构造方法导致）。

解析编写代码，使用FastJSON类库，解析JSON字符串为Bean对象，核心代码如下：

```scala
	/**
	 * 数据的处理
	 *
	 * @param streamDF 流式数据集StreamingDataFrame
	 * @param category 业务数据类型，比如物流系统业务数据，CRM系统业务数据等
	 * @return 流式数据集StreamingDataFrame
	 */
	override def process(streamDF: DataFrame, category: String): DataFrame = {
		// 导入隐式转换
		import streamDF.sparkSession.implicits._
		
		// 1. 依据不同业务系统数据进行不同的ETL转换
		val etlStreamDF: DataFrame = category match {
			// TODO: 物流系统业务数据ETL转换
			case "logistics" =>
				// a. 转换JSON为Bean对象
				val logisticsBeanStreamDS: Dataset[OggMessageBean] = streamDF
					.as[String] // 将DataFrame转换为Dataset
					.filter(json => null != json && json.trim.length > 0) // 过滤数据
					// 由于OggMessageBean是使用Java语言自定义类，所以需要自己指定编码器Encoder
					.mapPartitions { iter =>
						iter.map { json => JSON.parseObject(json, classOf[OggMessageBean]) }
					}(Encoders.bean(classOf[OggMessageBean]))
				// b. 返回转换后数据
				logisticsBeanStreamDS.toDF()
				
			// TODO：CRM系统业务数据ETL转换
			case "crm" =>
				// a. 转换JSON为Bean对象
				implicit val canalEncoder: Encoder[CanalMessageBean] = Encoders.bean(classOf[CanalMessageBean])
				val crmBeanStreamDS: Dataset[CanalMessageBean] = streamDF
					.filter(row => ! row.isNullAt(0))
					.mapPartitions{iter =>
						iter.map{row =>
							val json = row.getAs[String](0)
							JSON.parseObject(json, classOf[CanalMessageBean])
						}
					}
				// b. 返回转换后数据
				crmBeanStreamDS.toDF()
			
			// TODO: 其他业务数据数据，直接返回即可
			case _ => streamDF
		}
		// 2. 返回转换后流数据
		etlStreamDF
	}
```

> ​		在将JSON转换为Bean对象时，需要指定编码器Encoder，原因在于DataSet属于强类型，数据类型必须有Encoder对象进行编码，在SparkSQL中默认听编码器仅仅针对基本数据类型、元组类型和CaseClass类型，如果是其他类对，需要自己创建编码器对象Encoders。
>
> - 1）、方式一：
>
> ![1612512396973](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612512396973.png)
>
> - 2）、方式二：指定隐式参数，自动传递值
>
> ![1612512436245](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612512436245.png)

完整KuduStreamApp应用程序代码如下：

```scala
package cn.itcast.logistics.etl.realtime

import cn.itcast.logistics.common.beans.parser.{CanalMessageBean, OggMessageBean}
import cn.itcast.logistics.common.{Configuration, SparkUtils}
import com.alibaba.fastjson.JSON
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}

/**
 * Kudu数据管道应用：实现Kudu数据库的实时ETL操作
 */
object KuduStreamApp extends BasicStreamApp {
	
	/**
	 * 数据的处理
	 *
	 * @param streamDF 流式数据集StreamingDataFrame
	 * @param category 业务数据类型，比如物流系统业务数据，CRM系统业务数据等
	 * @return 流式数据集StreamingDataFrame
	 */
	override def process(streamDF: DataFrame, category: String): DataFrame = {
		// 导入隐式转换
		import streamDF.sparkSession.implicits._
		
		// 1. 依据不同业务系统数据进行不同的ETL转换
		val etlStreamDF: DataFrame = category match {
			// TODO: 物流系统业务数据ETL转换
			case "logistics" =>
				// a. 转换JSON为Bean对象
				val logisticsBeanStreamDS: Dataset[OggMessageBean] = streamDF
					.as[String] // 将DataFrame转换为Dataset
					.filter(json => null != json && json.trim.length > 0) // 过滤数据
					// 由于OggMessageBean是使用Java语言自定义类，所以需要自己指定编码器Encoder
					.mapPartitions { iter =>
						iter.map { json => JSON.parseObject(json, classOf[OggMessageBean]) }
					}(Encoders.bean(classOf[OggMessageBean]))
				// b. 返回转换后数据
				logisticsBeanStreamDS.toDF()
				
			// TODO：CRM系统业务数据ETL转换
			case "crm" =>
				// a. 转换JSON为Bean对象
				implicit val canalEncoder: Encoder[CanalMessageBean] = Encoders.bean(classOf[CanalMessageBean])
				val crmBeanStreamDS: Dataset[CanalMessageBean] = streamDF
					.filter(row => ! row.isNullAt(0))
					.mapPartitions{iter =>
						iter.map{row =>
							val json = row.getAs[String](0)
							JSON.parseObject(json, classOf[CanalMessageBean])
						}
					}
				// b. 返回转换后数据
				crmBeanStreamDS.toDF()
			
			// TODO: 其他业务数据数据，直接返回即可
			case _ => streamDF
		}
		// 2. 返回转换后流数据
		etlStreamDF
	}
	
	/**
	 * 数据的保存
	 *
	 * @param streamDF          保存数据集DataFrame
	 * @param tableName         保存表的名称
	 * @param isAutoCreateTable 是否自动创建表，默认创建表
	 */
	override def save(streamDF: DataFrame, tableName: String, isAutoCreateTable: Boolean): Unit = {
		streamDF.writeStream
			.outputMode(OutputMode.Append())
			.queryName(s"query-${tableName}")
			.format("console")
			.option("numRows", "10")
			.option("truncate", "false")
			.start()
	}
	
	/** MAIN 方法入口：Spark Application应用程序入口，必须创建SparkContext对象或SpakrSession对象 */
	def main(args: Array[String]): Unit = {
		// step1. 获取SparkSession对象
		val spark: SparkSession = SparkUtils.createSparkSession(
			SparkUtils.autoSettingEnv(SparkUtils.sparkConf()), this.getClass
		)
		spark.sparkContext.setLogLevel(Configuration.LOG_OFF)
		
		// step2. 从Kafka加载数据：物流系统业务数据logistics和CRM系统业务数据crm
		val logisticsStreamDF: DataFrame = load(spark, "logistics")
		val crmStreamDF: DataFrame = load(spark, "crm")
		
		// step3. 对流式数据进行实时ETL转换操作
		val logisticsEtlStreamDF = process(logisticsStreamDF, "logistics")
		val crmEtlStreamDF = process(crmStreamDF, "crm")
		
		// step4. 将转换后数据进行输出
		save(logisticsEtlStreamDF, "tbl_logistics")
		save(crmEtlStreamDF, "tbl_crm")
		
		// step5. 启动流式应用后，等待终止
		spark.streams.active.foreach(query => println(s"正在启动Query： ${query.name}"))
		spark.streams.awaitAnyTermination()
	}
}

```



## 19-[复习]-今日课程内容回顾 

> 主要讲解：开发环境和项目环境初始化，ETL模块基本开发（解析JSOn字符串为Bean对象）

![1612513467906](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612513467906.png)




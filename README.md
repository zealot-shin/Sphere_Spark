# Sphere_Spark
用spark 进行数据的etl
数据源目前有mongodb 和mysql db
目前已经实现了MySQL和mongo 数据库的连接设置，写入pq文件的简单设置。

以下有待实现：
1，连接和查询优化 部分。
2，mongodb的schema等参数化
3，spark conf 的设置
4，hdfs 的pq存储设置
5，集群环境的发布调整
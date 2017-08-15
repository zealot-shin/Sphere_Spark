# Sphere_Spark
用spark 进行数据的etl
数据源目前有mongodb 和mysql db
目前已经实现了MySQL和mongo 数据库的连接设置，写入pq文件的简单设置。

以下有待实现：
1，连接和查询优化部分
2，使用此框架进行数据分析，使用udf进行复杂的数据处理。预计已经可以实现，需要实际写几个类进行测试。
3，实现从db到其他存储方式的模板。比如到es，或者dbtodb。
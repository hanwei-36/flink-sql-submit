# Flink SQL Submit
该项目改自 [flink-sql-submit](https://github.com/wuchong/flink-sql-submit)
Flink版本:1.14.0

变动：
- 区分SQL类型从正则改为calcite的SqlParser
- 默认所有的插入语句在一组StatementSet中执行
- 支持set语句和udf
- 支持yarn-per-job模式

## How to use

1. Set your Flink install path and SQL_DIR in `env.sh`
2. Add your SQL scripts under `SQL_DIR` with `.sql` suffix, e.g, `q1.sql`
3. Start all the service your job needed, including Flink, Kafka, and DataBases.
3. Run your SQL via `./run.sh <sql-file-name>`, e.g. `./run.sh q1`
4. If the terminal returns the following output, it means the job is submitted successfully.

```
Starting execution of program
Job has been submitted with JobID d01b04d7c8f8a90798d6400462718743
```




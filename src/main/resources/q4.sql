create function OrderedSplitFunction as 'com.guren.sqlsubmit.function.OrderedSplitFunction';
SET 'OrderedSplitFunction.order'='desc';
CREATE TABLE user_log
(
    user_id   String,
    user_name String
) WITH (
      'connector' = 'kafka',
      'topic' = 'user_log',
      'properties.bootstrap.servers' = 'localhost:9092',
      'properties.group.id' = 'testGroup',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'csv'
);

CREATE TABLE sink_print
(
    user_id   String,
    user_name String,
    word      String,
    length    Int
) WITH (
      'connector' = 'print'
      );

insert into sink_print
select *
from user_log
         join LATERAL TABLE(OrderedSplitFunction(user_name,' ')) t(word,length)
on true;

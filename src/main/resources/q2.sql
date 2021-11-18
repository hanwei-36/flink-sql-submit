-- 开启 mini-batch
--SET 'table.exec.mini-batch.enabled'=true;
-- -- mini-batch的时间间隔，即作业需要额外忍受的延迟
--SET 'table.exec.mini-batch.allow-latency'='1s';
-- 一个 mini-batch 中允许最多缓存的数据
--SET 'table.exec.mini-batch.size'=1000;
-- 开启 local-global 优化
--SET 'table.optimizer.agg-phase-strategy'='TWO_PHASE';
-- 开启 distinct agg 切分
--SET 'table.optimizer.distinct-agg.split.enabled'=true;
SET 'parallelism.default'='1';
SET 'execution.checkpointing.mode'='AT_LEAST_ONCE';
SET 'execution.checkpointing.interval' = '180000';
SET 'execution.checkpointing.timeout' = '180000';
SET 'restart-strategy' = 'failure-rate';
SET 'restart-strategy.failure-rate.delay' = '2 s';
SET 'restart-strategy.failure-rate.failure-rate-interval' = '2 min';
SET 'restart-strategy.failure-rate.max-failures-per-interval' = '2';
CREATE TABLE user_log (
    user_id String,
    item_id String
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '10',
    'fields.user_id.length'='15',
    'fields.item_id.length'='15'
);

CREATE TABLE sink_print (
    user_id String,
    item_id String
) WITH (
      'connector' = 'print'
);


insert into sink_print select CONCAT('first',user_id),item_id from user_log;
insert into sink_print select CONCAT('second',user_id),item_id from user_log;


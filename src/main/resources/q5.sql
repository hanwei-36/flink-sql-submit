SET 'execution.checkpointing.interval' = '180000';
create function splitFunction as 'com.guren.sqlsubmit.function.SplitFunction';
create view stu AS
SELECT
    *
FROM
    (
        VALUES
        ('数学', '张三', '2021-01-01'),
        ('数学', '李四', '2021-01-02')
    ) AS t (subject, name, tt);


CREATE TABLE sink_print (
                            subject String,
                             user_name String,
                             word Date
) PARTITIONED BY (subject) WITH (
  'connector' = 'filesystem',
  'path' = 'file:////tmp/print',
  'format' = 'csv'
);

insert into sink_print select subject, name, TO_DATE(tt) from stu join LATERAL TABLE(splitfunction(name,' ')) t(word,length) on true;

package com.flink.demo.cases.case26;


import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.core.fs.Path;

import java.io.Serializable;

/**
 * Bucket关闭消息
 * @param <BucketId>
 */
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class BucketEvent<BucketId> implements Serializable {

    public BucketId bucketId;
    public int taskId;
    public int numberOfTasks;
    public long records;
    public Path path;
}
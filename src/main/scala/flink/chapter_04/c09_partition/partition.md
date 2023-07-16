# 分区策略

## 分区策略

- 数据在算子之间流动需要依靠分区策略（分区器），Flink目前内置了8种已实现的分区策略和1种自定义分区策略。已实现的分区策略对应的API为：

```shell
     BinaryHashPartitioner
     BroadcastPartitioner
     ForwardPartitioner
     GlobalPartitioner
     KeyGroupStreamPartitioner
     RebalancePartitioner
     RescalePartitioner
     ShufflePartitioner
```
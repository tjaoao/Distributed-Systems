# Distributed-Systems

### 分布式系统课程第一次作业-MapReducer

---------

#### 什么是 MapReduce？

MapReduce 是一个**编程模型**和**软件框架**，用于处理和生成大数据集。它由 Google 在 2004 年提出，后来被 Apache Hadoop 等项目开源实现。

#### 核心思想

"分而治之"（Divide and Conquer） - 将复杂的大规模数据处理任务分解为多个小任务，并行处理，最后合并结果

#### MapReduce 执行流程

```
输入数据 → 分片(Split) → Map任务 → Shuffle → Reduce任务 → 输出结果
```

#### MapReduce 编程模型特点

##### 优点

1. **简单性**：开发者只需关注Map和Reduce函数逻辑
2. **可扩展性**：可轻松扩展到数千个节点
3. **容错性**：自动处理节点故障
4. **数据本地性**：尽量在数据所在节点进行计算

##### 局限性

1. **不适合实时处理**：批处理模式，延迟较高
2. **迭代计算效率低**：多轮MapReduce作业IO开销大
3. **编程模型固定**：不适合所有计算场景

#### MapReduce 应用场景

##### 典型应用

- **词频统计**（WordCount）
- **日志分析**
- **网页爬取和索引**
- **数据清洗和ETL**
- **机器学习特征提取**
- **图算法**（如PageRank）

##### 不适用场景

- 实时数据处理
- 迭代密集型计算
- 流式处理
- 低延迟查询

------

### MapReduce 示例详解

#### WordCount 完整数据流

假设输入数据：

```
文件1: "hello world"
文件2: "hello hadoop"
```

**执行过程：**

1. **输入分片**：创建2个分片（每个文件一个）
2. **Map阶段**：
   - Map任务1：输入("0", "hello world") → 输出("hello",1), ("world",1)
   - Map任务2：输入("0", "hello hadoop") → 输出("hello",1), ("hadoop",1)
3. **Shuffle阶段**：
   - 分区：所有键分配到对应Reduce任务
   - 排序：按键字典序排序
   - 分组：相同键的值合并
4. **Reduce阶段**：
   - Reduce任务：输入("hadoop",[1]), ("hello",[1,1]), ("world",[1])
   - 输出：("hadoop",1), ("hello",2), ("world",1)

### MapReduce 性能优化

#### 优化策略

1. **合理设置Map和Reduce任务数**
2. **使用Combiner减少网络传输**
3. **数据压缩**
4. **选择合适的InputFormat和OutputFormat**
5. **数据本地化优化**

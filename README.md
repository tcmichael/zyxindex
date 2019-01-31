
# 描述
xxx

# 流程图

pic

# 交互细节

    1. 顺序遍历文件doc 得到（key，offset）.
    2. 计算key的hash，hash为64位。
    3. 按照高8位进行分shard，分成256个shard。
    4. 1TB的文件的offset是40位，所以每个key写入（64-8+40）= 96 位。
    5. 把shard文件写成HashTable


```
preLoad() {
    foreach key,offset in files {
       hash = Hash64(key);
       shardId = carcShardId(hash);
       shards[shardId].put(hash, offset);   
    }
    
    foreach shard in shards {
        HashTable[i] = generate(shard[i])
    }
}
```

```
Get(key) {
    hash = Hash64(key);
    shardId = calcShardId(hash);
    offset = HashTable[i].get(key);
    readKv(file, offset);
}
```

# 优化
    1. 空间优化，hashtable slot: (key: 24+32, offset:40)
    2. 工程性优化，每次打开不需要重复建立索引
    3. 并发生成hashTable
    4. 临时shard文件，使用bufio，减少随机写

# 待执行优化
    1. keycount比较少的话，shard直接落成hashTable，可以减少一次写磁盘io
    2. key冲突 返回多个结果
 
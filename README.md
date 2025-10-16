```mark
bitcask-kv-go/
├── data/                 # 数据存储核心模块
│   ├── log_record.go     ➤ 日志记录编码/解码（CRC校验、变长编码）
│   └── data_file.go      ➤ 数据文件管理（追加写、MMap读）
├── index/                # 索引系统
│   ├── btree.go          ➤ B+树索引实现（Put/Get/Delete）
│   └── iterator.go       ➤ 索引迭代器（正向/反向遍历）
├── fio/                  # IO 抽象层
│   ├── mmap.go           ➤ 内存映射 IO 实现 
│   └── file_io.go        ➤ 标准文件 IO 实现
├── doc/                  # 设计文档
│   ├── bitcaskDb.mdx     ➤ 数据库架构设计（含 Merge 机制流程图）
│   └── bitcaskWriteBatch.mdx ➤ 事务实现原理
└── utils/                # 工具模块
    ├── rand_kv.go        ➤ 测试数据生成工具
    └── file.go           ➤ 磁盘空间检测实现
```

### **关键技术特性**
1. **日志结构存储**：通过 <mcfile name="log_record.go" path="d:\kv\kv-project\data\log_record.go"></mcfile> 实现 CRC 校验头和变长编码
2. **混合 IO 模式**：在 <mcfile name="mmap.go" path="d:\kv\kv-project\fio\mmap.go"></mcfile> 中支持标准文件 IO 和内存映射的运行时切换
3. **事务支持**：<mcfile name="bitcaskWriteBatch.mdx" path="d:\kv\kv-project\doc\bitcaskWriteBatch.mdx"></mcfile> 两阶段提交和序列号机制

### **运行时生成的状态文件**
- `raft/raft_state.bin`：记录 Raft HardState、快照元信息与日志条目，用于节点崩溃恢复。
- `cluster/members.json`：持久化当前集群成员（nodeID → address），保证重启后成员列表与传输路由自动恢复。

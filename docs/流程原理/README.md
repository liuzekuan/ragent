# 流程原理

这个目录用于记录 `ragent` 项目中具体类、方法、调用链和底层机制的运行原理。

与 `docs/项目经验/` 的区别：

- `docs/流程原理/` 重点回答“当前项目中的代码是怎样运行的”。
- `docs/项目经验/` 重点回答“从当前实现中能总结出什么，以及怎样迁移到新项目”。

## 已有文章

- [KnowledgeDocumentScheduleJob scan 方法的数据库租约锁与扫描流程](./KnowledgeDocumentScheduleJob-scan方法的数据库租约锁与扫描流程.md)
  - 详细分析候选任务查询、数据库原子抢锁、租约 token、TTL、心跳续约、文档状态 CAS、锁丢失保护、完整刷新流程和当前竞争窗口

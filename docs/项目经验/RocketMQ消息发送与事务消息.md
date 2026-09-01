# RocketMQ 消息发送与事务消息

## 1. 这篇文章解决什么问题

本文把 RocketMQ 在 Spring Boot 项目中的两条常用链路完整串起来：

1. 普通消息：生产者发送消息，Broker 持久化，消费者按 Topic 和 Consumer Group 接收并处理。
2. 事务消息：生产者先发送 half 消息，再执行本地数据库事务；本地事务成功后提交消息，失败则回滚消息；如果结果不明确，Broker 发起回查。

内容分为两层：

- 先用通用示例说明一个新项目如何从 0 接入 RocketMQ。
- 再对照本项目的 `KnowledgeDocumentServiceImpl.startChunk()`、`RocketMQProducerAdapter` 和 `DelegatingTransactionListener`，解释每一步为什么这样设计。

本文以 Spring Boot + `rocketmq-spring-boot-starter` 为例。代码使用 Java 17 风格，包名、数据库表和业务字段可以替换成新项目自己的实现。

---

## 2. 先建立几个核心概念

### 2.1 NameServer、Broker、Producer、Consumer

```text
Producer  --发送-->  NameServer（发现 Broker 地址）  -->  Broker（存储消息）
Consumer  --订阅-->  NameServer（发现 Broker 地址）  -->  Broker（拉取/推送消息）
```

- **NameServer**：保存 Broker 的路由信息。客户端先向它查询 Topic 在哪个 Broker 上，不负责存储业务消息。
- **Broker**：真正存储消息、维护消费进度、处理投递和事务状态。
- **Producer**：业务服务中的消息生产者，负责发送普通消息或事务消息。
- **Consumer**：业务服务中的消息消费者，按 Topic 和 Consumer Group 接收消息。
- **Topic**：消息的逻辑分类，例如 `order-created`、`knowledge-document-chunk`。
- **Tag**：Topic 下的细粒度标签，可用于消费者过滤；本项目主要使用 Topic。
- **Message Key**：业务幂等、检索和排查时使用的业务键，不等于 RocketMQ 的消息 ID。
- **Consumer Group**：消费者实例的逻辑组。同一组内通常是一条消息只由一个实例处理；不同组会分别收到同一条消息。

### 2.2 普通消息和事务消息的区别

普通消息只关心“消息能否发送成功”：

```text
业务代码 -> 发送消息 -> Broker 返回发送结果
```

事务消息多了一个本地事务协调过程：

```text
发送 half 消息 -> 执行本地事务 -> COMMIT / ROLLBACK / UNKNOWN
                         |
                         +-- UNKNOWN 时，Broker 后续回查
```

事务消息解决的是“数据库状态和消息状态需要保持业务一致”的问题。例如：订单创建成功后必须发送“订单已创建”事件；知识文档状态改成 `RUNNING` 后才允许投递分块任务。

它不是分布式数据库事务，也不是严格意义上的跨系统 ACID。它依赖本地数据库事务、Broker 的 half 消息和状态回查，把不一致窗口压缩到可恢复范围内。

### 2.3 `executeLocalTransaction` 与消费者监听的区别

这两个名字都带“监听”，但处于不同阶段：

| 回调/监听器 | 触发者 | 触发时机 | 作用 |
|---|---|---|---|
| `executeLocalTransaction` | RocketMQ Producer 客户端 | half 消息发送成功后 | 执行当前消息对应的本地事务 |
| `checkLocalTransaction` | RocketMQ Broker 通过 Producer 发起回查 | 本地事务结果未知、超时或 Producer 异常 | 查询数据库确认事务最终状态 |
| `@RocketMQMessageListener` 的 `onMessage` | RocketMQ Consumer 客户端 | 消息被 Broker 提交并投递后 | 执行业务消费逻辑 |

`@RocketMQTransactionListener` 绑定的是事务回调，不是 Topic 消费者；`@RocketMQMessageListener` 绑定的才是 Topic 消费者。

`checkLocalTransaction()` 的执行时机可以按两条路径理解：

正常路径：

```text
1. Producer 发送 half 消息
2. RocketMQ 客户端调用 executeLocalTransaction()
3. 返回 COMMIT 或 ROLLBACK
   -> 事务结束，不需要 checkLocalTransaction()
```

状态未知路径：

```text
1. Producer 执行本地事务时宕机、超时或发生网络异常
2. Broker 不知道本地事务最终成功还是失败
3. Broker 后续发起事务回查
4. Producer 调用 checkLocalTransaction(message)
5. 回查器查询数据库，返回 COMMIT、ROLLBACK 或 UNKNOWN
```

例如：

```text
half 消息已写入 Broker
Producer 执行数据库事务时进程宕机
Broker 发现消息长时间没有明确结果
Broker -> Producer：请确认这条事务消息的状态
Producer -> checkLocalTransaction(message)
```

因此，`checkLocalTransaction()` 是“事务状态不明确”时的补偿回查方法，不是正常发送流程中每条消息都会执行的方法。

还要区分“监听器绑定”和“事务回调执行”：`@RocketMQTransactionListener` 不是在每次 `sendMessageInTransaction()` 调用时临时绑定的。Spring/RocketMQ 在应用启动阶段扫描这个注解对应的 Bean，把它转换并设置到底层 `TransactionMQProducer` 的事务监听器上；应用启动完成后，后续每次事务消息发送都复用这个已经绑定好的监听器。

---

## 3. 从 0 启动 RocketMQ

### 3.1 使用 Docker Compose 启动本地 Broker

本仓库已经提供了可参考的 Compose 文件：

```text
resources/docker/rocketmq-stack-5.2.0.compose.yaml
```

最小化的本地环境也可以写成：

```yaml
services:
  namesrv:
    image: apache/rocketmq:5.2.0
    command: sh mqnamesrv
    ports:
      - "9876:9876"

  broker:
    image: apache/rocketmq:5.2.0
    depends_on:
      - namesrv
    environment:
      NAMESRV_ADDR: namesrv:9876
    command: sh mqbroker -n namesrv:9876
    ports:
      - "10911:10911"
      - "10912:10912"
```

启动：

```bash
docker compose -f resources/docker/rocketmq-stack-5.2.0.compose.yaml up -d
docker compose -f resources/docker/rocketmq-stack-5.2.0.compose.yaml ps
```

应用在宿主机运行时，NameServer 地址一般是：

```text
127.0.0.1:9876
```

应用也在 Docker 网络中运行时，应使用容器服务名，例如：

```text
rmqnamesrv:9876
```

不要混用这两个地址。应用容器访问 `127.0.0.1:9876` 时，访问的是应用容器自己，而不是 NameServer 容器。

### 3.2 添加 Maven 依赖

在新项目的业务模块或基础设施模块添加：

```xml
<dependency>
    <groupId>org.apache.rocketmq</groupId>
    <artifactId>rocketmq-spring-boot-starter</artifactId>
    <version>2.3.6</version>
</dependency>
```

当前仓库在根 `pom.xml` 中统一管理版本，在 `framework/pom.xml` 中引入 starter：

```xml
<dependency>
    <groupId>org.apache.rocketmq</groupId>
    <artifactId>rocketmq-spring-boot-starter</artifactId>
</dependency>
```

版本选择建议：让 `rocketmq-spring-boot-starter` 与项目的 Spring Boot 主版本、JDK 版本和 RocketMQ Broker 版本经过兼容性验证；不要只复制一个旧项目的版本号。

### 3.3 添加基础配置

```yaml
rocketmq:
  name-server: 127.0.0.1:9876
  producer:
    group: demo-producer-group
    send-message-timeout: 3000
```

本项目的配置位于：

```text
bootstrap/src/main/resources/application.yaml
```

内容是：

```yaml
rocketmq:
  name-server: 127.0.0.1:9876
  producer:
    group: ragent-producer${unique-name:}_pg
    send-message-timeout: 2000
```

`producer.group` 是 Producer Group，不是消费者组；消费者要在 `@RocketMQMessageListener` 中单独配置 `consumerGroup`。

### 3.4 验证基础设施

启动应用后应重点检查：

1. NameServer 地址可达。
2. Producer 能成功连接 Broker。
3. Topic 对应消费者启动成功。
4. 事务消息使用的 Producer 已配置事务监听器。

本地排查时，可以先看应用日志，再通过 RocketMQ Dashboard 或命令行查看 Topic、Consumer Group 和消费进度。

---

## 4. 普通消息：从 0 到发送和消费

普通消息适合“数据库状态不需要和消息在同一个本地事务中完成”的场景，例如日志通知、统计事件、非关键缓存刷新。

### 4.1 设计消息事件和统一包装器

本项目不把数据库 DO 直接作为消息体，而是定义独立的事件对象。普通消息的真实示例是“消息反馈”事件：

```text
rag/src/main/java/com/nageoffer/ai/ragent/rag/mq/event/MessageFeedbackEvent.java
```

```java
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MessageFeedbackEvent implements Serializable {

    private String messageId;
    private String userId;
    private Integer vote;
    private String reason;
    private String comment;
    private boolean cancelled;
    private long submitTime;
}
```

所有普通消息和事务消息都使用框架里的统一包装器，实际消费者类型是：

```java
MessageWrapper<MessageFeedbackEvent> message;
```

本项目的对应类是：

```text
framework/src/main/java/com/nageoffer/ai/ragent/framework/mq/MessageWrapper.java
```

### 4.2 通过 `MessageQueueProducer` 发送

业务代码只依赖 `MessageQueueProducer`。本项目的真实普通消息生产者是 `MessageFeedbackServiceImpl`：

```java
MessageFeedbackEvent event = MessageFeedbackEvent.builder()
        .messageId(messageId)
        .userId(userId)
        .vote(request.getVote())
        .reason(request.getReason())
        .comment(request.getComment())
        .submitTime(System.currentTimeMillis())
        .build();

messageQueueProducer.send(
        feedbackTopic,
        userId + ":" + messageId,
        "消息反馈",
        event
);
```

代码位置：[MessageFeedbackServiceImpl.java](/Users/liuzekuan/devTools/idea_workspace/ragent/rag/src/main/java/com/nageoffer/ai/ragent/rag/service/impl/MessageFeedbackServiceImpl.java:60)。取消反馈时复用同一个 Topic，只把 `cancelled` 设置为 `true`，并使用同样的业务 key。

`RocketMQProducerAdapter.send` 会负责：

1. 为空的业务 key 生成 UUID。
2. 用 `MessageWrapper` 包装 body。
3. 设置 RocketMQ 的 keys Header。
4. 调用 `rocketMQTemplate.syncSend`。
5. 统一记录 SendStatus、msgId 和业务 key。

### 4.3 创建 Topic 消费者

本项目的 Topic 消费者是：

```text
rag/src/main/java/com/nageoffer/ai/ragent/rag/mq/MessageFeedbackConsumer.java
```

完整实现：

```java
@Component
@RequiredArgsConstructor
@RocketMQMessageListener(
        topic = "message-feedback_topic${unique-name:}",
        consumerGroup = "message-feedback_cg${unique-name:}"
)
public class MessageFeedbackConsumer
        implements RocketMQListener<
        MessageWrapper<MessageFeedbackEvent>> {

    private final MessageFeedbackService feedbackService;

    @Override
    public void onMessage(
            MessageWrapper<MessageFeedbackEvent> message) {
        MessageFeedbackEvent event = message.getBody();
        feedbackService.submitFeedbackByEvent(event);
    }
}
```

### 4.4 普通消息完整时序

```text
MessageFeedbackServiceImpl.submitFeedbackAsync
    |
    | 1. 构造 MessageFeedbackEvent
    | 2. messageQueueProducer.send(feedbackTopic, ...)
    v
RocketMQTemplate / Producer
    |
    | 3. 向 NameServer 查询路由
    | 4. 将消息发送给 Broker
    v
Broker
    |
    | 5. 持久化消息，返回 SendResult
    | 6. 按 Consumer Group 分配消息
    v
MessageFeedbackConsumer
    |
    | 7. onMessage(message)
    | 8. feedbackService.submitFeedbackByEvent(event)
    v
业务结果
```

普通消息中，发送成功只说明 Broker 接收了消息，不说明消费者已经处理成功。消费者处理失败时，RocketMQ 会根据重试配置重新投递；因此消费逻辑必须幂等。

### 4.5 普通消息的异常和重试

RocketMQ 只有在消费者方法抛出异常时，才会把这次消费判定为失败并进入重试。普通反馈消息的消费者调用链是：

```java
@Override
public void onMessage(
        MessageWrapper<MessageFeedbackEvent> message) {
    MessageFeedbackEvent event = message.getBody();
    feedbackService.submitFeedbackByEvent(event);
}
```

`submitFeedbackByEvent` 校验失败或数据库操作失败时会抛出异常，消费者容器会把本次消费判定为失败并触发重试。与之相对，文档分块消费者调用的 `executeChunk` 会在内部捕获分块异常、把文档标记为失败并记录日志，因此分块业务失败不会触发 MQ 重试，而是由文档状态和分块日志承载失败结果。

如果某个新消费者确实需要 RocketMQ 重试，应在消费者边界记录日志后重新抛出异常：

```java
try {
    feedbackService.submitFeedbackByEvent(event);
} catch (Exception e) {
    log.error("反馈消息消费失败, messageId={}",
            event.getMessageId(), e);
    throw e;
}
```

不要只记录日志然后正常返回，否则 RocketMQ 会认为消息消费成功。

更完整的消费者配置可以显式声明消费模式和线程数：

```java
@RocketMQMessageListener(
        topic = "knowledge-document-chunk_topic${unique-name:}",
        consumerGroup = "knowledge-document-chunk_cg${unique-name:}",
        consumeMode = ConsumeMode.CONCURRENTLY,
        messageModel = MessageModel.CLUSTERING,
        consumeThreadMax = 20,
        maxReconsumeTimes = 5
)
```

具体属性名和可用值要以当前 `rocketmq-spring-boot-starter` 版本的注解定义为准。不要盲目复制旧版本配置。

### 4.6 普通消息的幂等

RocketMQ 常见的是至少一次投递。重复原因包括：

- 消费者执行业务成功，但 ACK 返回前进程宕机。
- 网络抖动导致客户端和 Broker 对结果认识不同。
- 消费者主动抛异常触发重试。
- 消费者扩缩容、负载重平衡。

本项目的反馈消息没有单独的 MQ 消费记录表，而是使用业务 key 和提交时间实现幂等、乱序保护。生产者使用：

```java
userId + ":" + messageId
```

事件中还携带 `submitTime`。`MessageFeedbackServiceImpl.doUpsertFeedback` 更新已有记录时，只有本次 `submitTime` 晚于数据库的 `updateTime` 才覆盖旧值，避免多节点消费乱序导致旧反馈覆盖新反馈。

文档分块是另一套状态机幂等策略：事务消息的本地事务中使用条件更新：

```java
.eq(KnowledgeDocumentDO::getId, docId)
.ne(KnowledgeDocumentDO::getStatus,
        DocumentStatus.RUNNING.getCode())
```

这样同一文档重复点击“开始分块”时，只有第一次能把状态从非 `RUNNING` 更新成功。消费者侧则先检查文档是否存在：

```java
public void executeChunk(String docId) {
    KnowledgeDocumentDO documentDO = documentMapper.selectById(docId);
    if (documentDO == null) {
        log.warn("文档不存在，跳过分块任务, docId={}", docId);
        return;
    }

    runChunkTask(documentDO);
}
```

如果新业务不能依靠状态机保证幂等，应在项目中增加消费记录表或唯一索引，并把“检查重复、业务更新、写消费记录”放在同一个 Spring 事务中。不要假设 RocketMQ 只投递一次。

---

## 5. 事务消息：要解决什么一致性问题

### 5.1 典型反例

假设订单服务需要同时完成：

1. 数据库插入订单。
2. 发送 `order-created` 消息。

直接写成：

```java
orderMapper.insert(order);
rocketMQTemplate.syncSend("order-created", event);
```

可能出现：

```text
数据库提交成功
进程在发送消息前宕机
结果：订单存在，但没有订单消息
```

反过来先发消息也会出现：

```text
消息发送成功
数据库插入失败
结果：消费者收到了一条实际上不存在的订单事件
```

### 5.2 RocketMQ 事务消息能保证什么

RocketMQ 事务消息采用“half 消息 + 本地事务 + 事务状态确认”的模式：

- half 消息先写入 Broker，但暂时对普通消费者不可见。
- Producer 客户端调用本地事务回调。
- 本地事务成功，返回 `COMMIT`，Broker 才让消息可见。
- 本地事务失败，返回 `ROLLBACK`，Broker 丢弃或删除消息。
- Producer 宕机、超时或状态未知时，Broker 之后发起 `checkLocalTransaction` 回查。

它不能替代消费者幂等，也不能让外部 HTTP 调用、远程服务调用和数据库天然成为一个 ACID 事务。事务回调中应优先只做当前数据库连接可管理的本地事务。

---

## 6. 从 0 搭建事务消息（项目具体代码见第 8 节）

本节先按 RocketMQ 的生命周期拆解搭建步骤；其中的抽象接口形状对应本项目的真实组件。新功能直接按第 8 节的 `MessageQueueProducer`、`RocketMQProducerAdapter`、`DelegatingTransactionListener` 和 `TransactionChecker` 结构添加业务事件、消费者和回查器。

### 6.1 第一步：定义本地事务边界

先明确“什么数据库状态代表本地事务提交成功”。例如订单事务可以定义为：订单表已经插入且状态为 `CREATED`；文档分块事务可以定义为：文档状态已经更新为 `RUNNING`，对应调度记录也已写入。

事务消息的本地事务必须满足：

1. 可以在一个数据库事务中完成。
2. 可以通过消息内容查询数据库判断是否提交。
3. 重复执行时能安全失败或幂等。
4. 不依赖当前 JVM 内存中的 Lambda、线程变量或临时对象来做回查。

### 6.2 第二步：注册一个事务监听器（概念伪代码）

本项目的实际实现文件是：

```text
framework/src/main/java/com/nageoffer/ai/ragent/framework/mq/producer/DelegatingTransactionListener.java
```

启动阶段的绑定过程是：

```text
Spring 创建 DelegatingTransactionListener Bean
        ↓
RocketMQ Spring 扫描 @RocketMQTransactionListener
        ↓
找到对应的 rocketMQTemplate
        ↓
取得底层 TransactionMQProducer
        ↓
设置 TransactionListener
        ↓
应用后续调用 sendMessageInTransaction 时复用该监听器
```

因此，`@RocketMQTransactionListener` 的作用是启动时完成 Producer 事务回调注册；它不是在每次 `sendMessageInTransaction()` 调用时临时绑定的，也不是 Topic 消费注解。

### 6.3 一个 `RocketMQTemplate` 只能绑定一个事务监听器

如果多个类都使用：

```java
@RocketMQTransactionListener
```

并且它们都绑定默认的 `rocketMQTemplate`，RocketMQ Spring 启动注册第二个监听器时会发现底层 Producer 已经存在事务监听器，通常直接抛异常，应用启动失败。不会出现“同一条消息依次执行多个 `executeLocalTransaction`”的情况。

如果确实需要多个 Producer，可以创建多个 `RocketMQTemplate`，分别指定：

```java
@RocketMQTransactionListener(
        rocketMQTemplateBeanName = "anotherRocketMQTemplate"
)
```

更常见的做法是像本项目一样只保留一个统一监听器，再在监听器内部按 `txId` 分发本地事务、按 Topic 分发回查器。

### 6.4 第三步：实现事务消息发送适配器（概念伪代码）

本项目不再单独创建一个业务事务 Producer，而是由以下适配器统一实现：

```text
framework/src/main/java/com/nageoffer/ai/ragent/framework/mq/producer/RocketMQProducerAdapter.java
```

生产者适配器要注意两件事：

1. `txId` 必须放进消息 Header，这样 `executeLocalTransaction` 才能找到对应的 Lambda。
2. 发送异常或发送状态非 `SEND_OK` 时，要清理内存 Map，避免本地事务回调泄漏。

实际项目中可把 `unregister` 设为包可见，并由同包适配器调用；示例为了突出流程省略了部分访问修饰符。

### 6.5 第四步：在业务 Service 中调用（概念伪代码）

本项目的真实调用在：

```text
rag/src/main/java/com/nageoffer/ai/ragent/knowledge/service/impl/KnowledgeDocumentServiceImpl.java
```

`startChunk` 传入的 Lambda 不是消息消费者，而是文档状态更新和调度写入的本地数据库事务逻辑。

### 6.6 第五步：实现事务回查（概念伪代码）

回查不能依赖 `localTransactions` 这类内存 Map，因为：

- Broker 可能把回查请求发到另一台服务实例。
- 原实例可能已经重启，内存数据已经丢失。
- 回查应该根据持久化状态判断本地事务最终结果。

本项目的回查接口和实现分别是：

```text
framework/src/main/java/com/nageoffer/ai/ragent/framework/mq/producer/TransactionChecker.java
rag/src/main/java/com/nageoffer/ai/ragent/knowledge/mq/KnowledgeDocumentChunkTransactionChecker.java
```

初始化时按 Topic 注册：

```java
transactionListener.registerChecker(chunkTopic, this);
```

回查时从 `MessageWrapper<KnowledgeDocumentChunkEvent>` 中取出 `docId`，查询 `KnowledgeDocumentDO`，只有状态为 `RUNNING` 才返回已提交。判断必须使用可持久化、可重复查询的业务事实，例如：

- 订单是否存在且状态为 `CREATED`。
- 文档是否为 `RUNNING`。
- 知识库是否已经逻辑删除。
- 事务表中的状态是否为 `COMMITTED`。

不要用“内存中是否存在 txId”判断是否提交；这无法支持重启和多实例回查。统一回调的具体 `checkLocalTransaction` 实现在 `DelegatingTransactionListener`，完整代码见第 8.5.4 节。

### 6.7 第六步：配置事务回查参数

事务回查间隔、最大回查次数和 Producer 线程池等配置会受 RocketMQ Client 与 starter 版本影响。建议：

1. 先查看当前依赖版本的 `@RocketMQTransactionListener` 和 Producer 配置类。
2. 在测试环境模拟本地事务抛异常、进程宕机和网络中断。
3. 根据业务最长事务时间设置回查间隔和保留时间。

不要只通过“发送接口返回成功”判断事务链路完成，必须同时观察 Broker 中消息是否最终可消费，以及消费者是否成功处理。

---

## 7. 事务消息的完整时序

### 7.1 成功路径

```text
业务 Service
    |
    | 1. 生成 txId，注册 txId -> localTransaction
    | 2. 组装带 txId / topic Header 的消息
    v
RocketMQTemplate.sendMessageInTransaction
    |
    | 3. 发送 half 消息
    v
Broker
    |
    | 4. 暂存消息，普通消费者不可见
    v
Producer 客户端
    |
    | 5. 回调 executeLocalTransaction(message, arg)
    | 6. 根据 txId 找到 Lambda
    | 7. TransactionTemplate 开启数据库事务
    | 8. mapper.update / insert 等数据库操作
    | 9. Lambda 正常结束，数据库事务 commit
    | 10. 返回 RocketMQLocalTransactionState.COMMIT
    v
Broker
    |
    | 11. 提交 half 消息，使其对消费者可见
    v
Consumer
    |
    | 12. 按 Topic 和 Consumer Group 收到消息
    | 13. onMessage 执行业务消费
```

### 7.2 本地事务失败路径

```text
half 消息已发送
    ↓
executeLocalTransaction 执行数据库操作
    ↓
数据库异常 / 业务校验失败
    ↓
TransactionTemplate 回滚
    ↓
监听器返回 ROLLBACK
    ↓
Broker 丢弃消息，消费者不可见
```

### 7.3 Producer 宕机或结果未知路径

回查是Broker 主动发起的

```text
half 消息已发送
    ↓
Producer 执行本地事务期间宕机
    ↓
Broker 长时间没有收到明确 COMMIT / ROLLBACK
    ↓
Broker 发起事务回查
    ↓
Producer 调用 checkLocalTransaction(message)
    ↓
按 Topic 找 checker，按消息内容查询数据库
    ├─ 已提交：返回 COMMIT，消息可见
    ├─ 已回滚：返回 ROLLBACK，消息丢弃
    └─ 无法判断：返回 UNKNOWN，稍后继续回查
```

### 7.4 `COMMIT` 后是否立即调用消费者

`COMMIT` 的含义是“通知 Broker 可以提交这条 half 消息”，不是直接调用消费者方法。Broker 提交后，消息才进入普通投递流程；之后消费者可能立即收到，也可能因为网络、线程池、消费堆积等原因稍后收到。

因此：

- Producer 的 `COMMIT` 不代表消费者已经执行成功。
- 消费者仍然需要重试和幂等。
- 消费者异常不会自动回滚已经提交的本地事务；它属于另一条消费处理链路。

---

## 8. 本项目完整实现：以后新功能照这个结构搭建

前面的通用概念用于理解 RocketMQ；真正落地到这个项目时，不建议每个业务类直接操作 `RocketMQTemplate`。本项目把 MQ 分成四层：

下面的代码片段与仓库当前实现保持一致；为便于阅读，个别片段省略了许可证头和不影响职责理解的 import，真正复制时优先直接打开对应源文件。

```text
业务 Service
    ↓ 注入 MessageQueueProducer
RocketMQProducerAdapter
    ↓ 统一包装 MessageWrapper、设置 Header、记录日志
RocketMQTemplate
    ↓ 发送到 Broker
DelegatingTransactionListener
    ├─ executeLocalTransaction：按 txId 找本地事务 Lambda
    └─ checkLocalTransaction：按 topic 找 TransactionChecker
消费者
    └─ @RocketMQMessageListener(topic, consumerGroup)
```

新功能建议遵循下面的目录和职责：

```text
framework/mq/
├── MessageWrapper.java                         # 通用消息包装
└── producer/
    ├── MessageQueueProducer.java               # 业务侧统一发送接口
    ├── RocketMQProducerAdapter.java             # RocketMQTemplate 适配实现
    ├── DelegatingTransactionListener.java       # 全局事务回调
    └── TransactionChecker.java                  # 事务回查接口

rag/knowledge/mq/
├── event/KnowledgeDocumentChunkEvent.java      # 业务事件
├── KnowledgeDocumentChunkConsumer.java         # Topic 消费者
└── KnowledgeDocumentChunkTransactionChecker.java # 事务回查器
```

### 8.1 基础配置与自动装配

配置文件是：

```text
bootstrap/src/main/resources/application.yaml
```

```yaml
rocketmq:
  name-server: 127.0.0.1:9876
  producer:
    group: ragent-producer${unique-name:}_pg
    send-message-timeout: 2000
```

框架模块通过配置类把统一事务监听器和业务侧 Producer 接口注册为 Bean：

```java
package com.nageoffer.ai.ragent.framework.config;

@Configuration
public class RocketMQAutoConfiguration {

    @Bean
    public DelegatingTransactionListener delegatingTransactionListener() {
        return new DelegatingTransactionListener();
    }

    @Bean
    public MessageQueueProducer messageQueueProducer(
            RocketMQTemplate rocketMQTemplate,
            DelegatingTransactionListener transactionListener) {
        return new RocketMQProducerAdapter(
                rocketMQTemplate, transactionListener);
    }
}
```

这一步的意义是：业务模块只依赖 `MessageQueueProducer`，不会把 `RocketMQTemplate`、事务 Header 和回调 Map 泄漏到业务层。

### 8.2 通用消息包装器：`MessageWrapper`

文件：

```text
framework/src/main/java/com/nageoffer/ai/ragent/framework/mq/MessageWrapper.java
```

```java
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MessageWrapper<T> implements Serializable {

    private String keys;
    private T body;

    @Builder.Default
    private String uuid = UUID.randomUUID().toString();

    @Builder.Default
    private Long timestamp = System.currentTimeMillis();
}
```

业务事件放在 `body`，`keys` 用于业务主键或幂等判断，`uuid` 用于消息级幂等，`timestamp` 用于日志和延迟排查。不要把数据库 DO 直接作为消息契约；应使用单独的事件类。

### 8.3 业务侧统一接口：`MessageQueueProducer`

文件：

```text
framework/src/main/java/com/nageoffer/ai/ragent/framework/mq/producer/MessageQueueProducer.java
```

完整接口：

```java
public interface MessageQueueProducer {

    SendResult send(String topic, String keys, String bizDesc, Object body);

    void sendInTransaction(
            String topic,
            String keys,
            String bizDesc,
            Object body,
            Consumer<Object> localTransaction);
}
```

普通消息统一调用 `send`，事务消息统一调用 `sendInTransaction`。这样以后如果替换 RocketMQ、增加统一监控、增加消息 Trace 或调整重试策略，只需要修改适配层。

### 8.4 普通消息完整示例：使用 `MessageQueueProducer.send`

本项目的普通消息生产者是 `MessageFeedbackServiceImpl`，业务 Service 不需要直接构造 Spring `Message`：

```java
MessageFeedbackEvent event = MessageFeedbackEvent.builder()
        .messageId(messageId)
        .userId(userId)
        .vote(request.getVote())
        .reason(request.getReason())
        .comment(request.getComment())
        .submitTime(System.currentTimeMillis())
        .build();

messageQueueProducer.send(
        feedbackTopic,
        userId + ":" + messageId,
        "消息反馈",
        event
);
```

这里的 `send` 最终进入 `RocketMQProducerAdapter.send`：

```java
@Override
public SendResult send(String topic, String keys,
                       String bizDesc, Object body) {
    keys = StrUtil.isEmpty(keys)
            ? UUID.randomUUID().toString()
            : keys;

    Message<MessageWrapper<Object>> message = MessageBuilder
            .withPayload(MessageWrapper.builder()
                    .keys(keys)
                    .body(body)
                    .build())
            .setHeader(MessageConst.PROPERTY_KEYS, keys)
            .build();

    SendResult sendResult = rocketMQTemplate.syncSend(topic, message);
    log.info("[生产者] {} - 发送结果: {}, 消息ID: {}, Keys: {}",
            bizDesc, sendResult.getSendStatus(),
            sendResult.getMsgId(), keys);
    return sendResult;
}
```

普通消息消费者使用真实的 `MessageFeedbackConsumer`：

```java
@Slf4j
@Component
@RequiredArgsConstructor
@RocketMQMessageListener(
        topic = "message-feedback_topic${unique-name:}",
        consumerGroup = "message-feedback_cg${unique-name:}"
)
public class MessageFeedbackConsumer implements RocketMQListener<
        MessageWrapper<MessageFeedbackEvent>> {

    private final MessageFeedbackService feedbackService;

    @Override
    public void onMessage(MessageWrapper<MessageFeedbackEvent> message) {
        MessageFeedbackEvent event = message.getBody();
        log.info("收到反馈事件，messageId={}, userId={}, keys={}",
                event.getMessageId(), event.getUserId(), message.getKeys());
        feedbackService.submitFeedbackByEvent(event);
    }
}
```

普通消息的固定链路就是：

```text
MessageFeedbackServiceImpl.submitFeedbackAsync
  → MessageQueueProducer.send
  → RocketMQProducerAdapter.send
  → RocketMQTemplate.syncSend
  → message-feedback_topic
  → MessageFeedbackConsumer.onMessage
  → MessageFeedbackService.submitFeedbackByEvent
```

### 8.5 事务消息完整示例：`KnowledgeDocumentChunkEvent`

#### 8.5.1 业务事件

文件：

```text
rag/src/main/java/com/nageoffer/ai/ragent/knowledge/mq/event/KnowledgeDocumentChunkEvent.java
```

```java
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KnowledgeDocumentChunkEvent implements Serializable {

    private String docId;
    private String kbId;
    private String operator;
}
```

发送前 `kbId` 可能还没有补齐；本地事务中查询文档后会执行 `event.setKbId(...)`，但不要把这个修改当成可靠的消息字段更新。RocketMQ 已经把消息转换成 Broker 使用的消息体，事务回调中的对象修改不应作为跨进程契约；消费者和回查器需要的字段必须在发送前就放进事件，或者在消费者侧根据 `docId` 查询数据库。本项目的回查只依赖 `docId` 和数据库状态。

#### 8.5.2 业务 Service 发起事务消息

文件：

```text
rag/src/main/java/com/nageoffer/ai/ragent/knowledge/service/impl/KnowledgeDocumentServiceImpl.java
```

完整调用结构：

```java
public void startChunk(String docId) {
    KnowledgeDocumentDO beforeDO = documentMapper.selectById(docId);
    Assert.notNull(beforeDO, () -> new ClientException("文档不存在"));

    KnowledgeDocumentChunkEvent event = KnowledgeDocumentChunkEvent.builder()
            .docId(docId)
            .operator(UserContext.getUsername())
            .build();

    messageQueueProducer.sendInTransaction(
            chunkTopic,
            docId,
            "文档分块",
            event,
            arg -> {
                int updated = documentMapper.update(
                        new LambdaUpdateWrapper<KnowledgeDocumentDO>()
                                .set(KnowledgeDocumentDO::getStatus,
                                        DocumentStatus.RUNNING.getCode())
                                .set(KnowledgeDocumentDO::getUpdatedBy,
                                        event.getOperator())
                                .set(KnowledgeDocumentDO::getUpdateTime,
                                        new Date())
                                .eq(KnowledgeDocumentDO::getId, docId)
                                .ne(KnowledgeDocumentDO::getStatus,
                                        DocumentStatus.RUNNING.getCode())
                );

                if (updated == 0) {
                    throw new ClientException(
                            "文档分块操作正在进行中，请稍后再试");
                }

                KnowledgeDocumentDO documentDO =
                        documentMapper.selectById(docId);
                event.setKbId(documentDO.getKbId());
                scheduleService.upsertSchedule(documentDO);
            }
    );
}
```

这里的 Lambda 是本地事务逻辑，不是消费者逻辑。它只负责把文档状态改成 `RUNNING`、写入调度信息；耗时的解析、分块、向量化在后面的消费者中执行。

#### 8.5.3 适配器如何把 Lambda 交给 RocketMQ

文件：

```text
framework/src/main/java/com/nageoffer/ai/ragent/framework/mq/producer/RocketMQProducerAdapter.java
```

事务方法的完整关键代码：

```java
@Override
public void sendInTransaction(String topic, String keys,
                              String bizDesc, Object body,
                              Consumer<Object> localTransaction) {
    keys = StrUtil.isEmpty(keys)
            ? UUID.randomUUID().toString()
            : keys;
    String txId = UUID.randomUUID().toString();

    transactionListener.registerLocalTransaction(
            txId, localTransaction);

    Message<MessageWrapper<Object>> message = MessageBuilder
            .withPayload(MessageWrapper.builder()
                    .keys(keys)
                    .body(body)
                    .build())
            .setHeader(MessageConst.PROPERTY_KEYS, keys)
            .setHeader(DelegatingTransactionListener.HEADER_TX_ID, txId)
            .setHeader(DelegatingTransactionListener.HEADER_TOPIC, topic)
            .build();

    try {
        TransactionSendResult sendResult =
                rocketMQTemplate.sendMessageInTransaction(
                        topic, message, null);

        if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
            transactionListener.unregisterLocalTransaction(txId);
        }

        log.info("[生产者] {} - 事务消息发送结果: {}, 本地事务状态: {}, "
                        + "消息ID: {}, Keys: {}",
                bizDesc,
                sendResult.getSendStatus(),
                sendResult.getLocalTransactionState(),
                sendResult.getMsgId(),
                keys);
    } catch (Throwable ex) {
        transactionListener.unregisterLocalTransaction(txId);
        throw ex;
    }
}
```

这里必须先 `registerLocalTransaction`，再调用 `sendMessageInTransaction`。因为 half 消息发送成功后，RocketMQ 客户端可能立即同步回调 `executeLocalTransaction`。

#### 8.5.4 统一事务监听器

文件：

```text
framework/src/main/java/com/nageoffer/ai/ragent/framework/mq/producer/DelegatingTransactionListener.java
```

```java
@Slf4j
@RocketMQTransactionListener
public class DelegatingTransactionListener
        implements RocketMQLocalTransactionListener {

    static final String HEADER_TX_ID = "TRANSACTION_CONTEXT_ID";
    static final String HEADER_TOPIC = "TRANSACTION_TOPIC";

    private final ConcurrentMap<String, Consumer<Object>>
            localTransactionMap = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, TransactionChecker>
            checkerMap = new ConcurrentHashMap<>();

    @Autowired
    private PlatformTransactionManager transactionManager;

    public void registerLocalTransaction(
            String txId, Consumer<Object> localTransaction) {
        localTransactionMap.put(txId, localTransaction);
    }

    public void registerChecker(String topic,
                                TransactionChecker checker) {
        checkerMap.put(topic, checker);
    }

    @Override
    public RocketMQLocalTransactionState executeLocalTransaction(
            Message message, Object arg) {
        String txId = (String) message.getHeaders()
                .get(HEADER_TX_ID);
        Consumer<Object> localTransaction = txId == null
                ? null
                : localTransactionMap.remove(txId);

        if (localTransaction == null) {
            return RocketMQLocalTransactionState.ROLLBACK;
        }

        try {
            new TransactionTemplate(transactionManager)
                    .executeWithoutResult(status ->
                            localTransaction.accept(arg));
            return RocketMQLocalTransactionState.COMMIT;
        } catch (Exception e) {
            return RocketMQLocalTransactionState.ROLLBACK;
        }
    }

    @Override
    public RocketMQLocalTransactionState checkLocalTransaction(
            Message message) {
        String topic = (String) message.getHeaders()
                .get(HEADER_TOPIC);
        TransactionChecker checker = topic == null
                ? null
                : checkerMap.get(topic);
        if (checker == null) {
            return RocketMQLocalTransactionState.ROLLBACK;
        }

        try {
            MessageWrapper<?> wrapper =
                    (MessageWrapper<?>) message.getPayload();
            return checker.check(wrapper)
                    ? RocketMQLocalTransactionState.COMMIT
                    : RocketMQLocalTransactionState.ROLLBACK;
        } catch (Exception e) {
            return RocketMQLocalTransactionState.UNKNOWN;
        }
    }
}
```

`TransactionTemplate` 会在 Lambda 正常结束后提交数据库事务；Lambda 抛出异常时回滚。只有这个回调返回 `COMMIT`，Broker 才会让消息对消费者可见。

#### 8.5.5 Topic 消费者

文件：

```text
rag/src/main/java/com/nageoffer/ai/ragent/knowledge/mq/KnowledgeDocumentChunkConsumer.java
```

完整消费者：

```java
@Slf4j
@Component
@RequiredArgsConstructor
@RocketMQMessageListener(
        topic = "knowledge-document-chunk_topic${unique-name:}",
        consumerGroup = "knowledge-document-chunk_cg${unique-name:}"
)
public class KnowledgeDocumentChunkConsumer
        implements RocketMQListener<
        MessageWrapper<KnowledgeDocumentChunkEvent>> {

    private final KnowledgeDocumentService documentService;

    @Override
    public void onMessage(
            MessageWrapper<KnowledgeDocumentChunkEvent> message) {
        KnowledgeDocumentChunkEvent event = message.getBody();

        log.info("[消费者] 开始消费文档分块任务，docId={}, keys={}",
                event.getDocId(), message.getKeys());

        UserContext.set(LoginUser.builder()
                .username(event.getOperator())
                .build());
        try {
            documentService.executeChunk(event.getDocId());
        } finally {
            UserContext.clear();
        }
    }
}
```

注意：这个类上的 `@RocketMQMessageListener` 才是 Topic 消费监听。它和 `DelegatingTransactionListener` 上的 `@RocketMQTransactionListener` 完全是两种机制。

#### 8.5.6 事务回查器

文件：

```text
rag/src/main/java/com/nageoffer/ai/ragent/knowledge/mq/KnowledgeDocumentChunkTransactionChecker.java
```

```java
@Slf4j
@Component
@RequiredArgsConstructor
public class KnowledgeDocumentChunkTransactionChecker
        implements TransactionChecker {

    private final KnowledgeDocumentMapper documentMapper;
    private final DelegatingTransactionListener transactionListener;

    @Value("knowledge-document-chunk_topic${unique-name:}")
    private String chunkTopic;

    @PostConstruct
    public void init() {
        transactionListener.registerChecker(chunkTopic, this);
    }

    @Override
    public boolean check(MessageWrapper<?> message) {
        KnowledgeDocumentChunkEvent event =
                (KnowledgeDocumentChunkEvent) message.getBody();
        KnowledgeDocumentDO documentDO =
                documentMapper.selectById(event.getDocId());

        return documentDO != null
                && DocumentStatus.RUNNING.getCode()
                        .equals(documentDO.getStatus());
    }
}
```

回查按 Topic 注册，但判断依据是数据库中的文档状态，不是内存中的 `txId`。Broker 回查可能落到任意服务实例，因此这一步必须使用共享持久化状态。

### 8.7 `startChunk` 做了什么

位置：

```text
rag/src/main/java/com/nageoffer/ai/ragent/knowledge/service/impl/KnowledgeDocumentServiceImpl.java
```

核心代码：

```java
messageQueueProducer.sendInTransaction(
        chunkTopic,
        docId,
        "文档分块",
        event,
        arg -> {
            int updated = documentMapper.update(
                    new LambdaUpdateWrapper<KnowledgeDocumentDO>()
                            .set(KnowledgeDocumentDO::getStatus,
                                    DocumentStatus.RUNNING.getCode())
                            .set(KnowledgeDocumentDO::getUpdatedBy,
                                    event.getOperator())
                            .set(KnowledgeDocumentDO::getUpdateTime, new Date())
                            .eq(KnowledgeDocumentDO::getId, docId)
                            .ne(KnowledgeDocumentDO::getStatus,
                                    DocumentStatus.RUNNING.getCode())
            );

            if (updated == 0) {
                throw new ClientException("文档分块操作正在进行中，请稍后再试");
            }

            KnowledgeDocumentDO documentDO = documentMapper.selectById(docId);
            event.setKbId(documentDO.getKbId());
            scheduleService.upsertSchedule(documentDO);
        }
);
```

它的业务含义是：只有文档成功进入 `RUNNING` 状态并完成调度记录更新，分块任务消息才允许对消费者可见。

### 8.8 `registerLocalTransaction` 为什么必须在发送前执行

位置：

```text
framework/src/main/java/com/nageoffer/ai/ragent/framework/mq/producer/RocketMQProducerAdapter.java
```

关键流程：

```java
String txId = UUID.randomUUID().toString();
transactionListener.registerLocalTransaction(txId, localTransaction);

Message<MessageWrapper<Object>> message = MessageBuilder
        .withPayload(...)
        .setHeader(DelegatingTransactionListener.HEADER_TX_ID, txId)
        .setHeader(DelegatingTransactionListener.HEADER_TOPIC, topic)
        .build();

rocketMQTemplate.sendMessageInTransaction(topic, message, null);
```

`sendMessageInTransaction` 在 half 消息成功后可能很快同步回调 `executeLocalTransaction`。如果先发送、后注册，回调可能已经发生，Map 中找不到 Lambda，结果就会被判定为 `ROLLBACK`。

### 8.9 `DelegatingTransactionListener` 为什么不标注 Topic

本项目的统一监听器：

```java
@RocketMQTransactionListener
public class DelegatingTransactionListener
        implements RocketMQLocalTransactionListener {
```

它不标注 Topic，是因为它监听的是 Producer 的事务回调，而不是普通消息消费。

它内部维护两张表：

```java
private final ConcurrentMap<String, Consumer<Object>> localTransactionMap;
private final ConcurrentMap<String, TransactionChecker> checkerMap;
```

- `localTransactionMap`：按 `txId` 找“当前这条消息”的本地事务 Lambda，仅在当前实例发送期间有效。
- `checkerMap`：按 `topic` 找回查器，回查逻辑必须基于数据库，支持任意实例执行。

对应代码：

```java
String txId = (String) message.getHeaders().get(HEADER_TX_ID);
Consumer<Object> localTransaction = localTransactionMap.remove(txId);
```

以及：

```java
String topic = (String) message.getHeaders().get(HEADER_TOPIC);
TransactionChecker checker = checkerMap.get(topic);
```

可以记成：

```text
executeLocalTransaction：txId 路由
checkLocalTransaction：topic 路由
onMessage：@RocketMQMessageListener 的 topic 路由
```

### 8.10 事务模板是否自动提交数据库

这一行：

```java
new TransactionTemplate(transactionManager)
        .executeWithoutResult(status -> localTransaction.accept(arg));
```

正常情况下会：

1. 通过 `PlatformTransactionManager` 开启事务。
2. 执行 Lambda 中的 `mapper.update`、`mapper.insert` 等操作。
3. Lambda 正常结束后自动 `commit`。
4. Lambda 抛出运行时异常或错误时自动 `rollback`。

因此 `executeLocalTransaction` 返回 `COMMIT` 时，通常意味着本地数据库事务已经提交成功；但它不代表消费者已经消费成功。

如果普通方法没有 `@Transactional`：

```java
public void updateData() {
    mapper.update(...);
}
```

在没有外层 Spring 事务的前提下，MyBatis-Spring 通常使用自动提交连接，单条 SQL 执行完成后就提交。若这个普通方法是从一个已有 `@Transactional` 方法调用的，它会加入外层事务，最终要等外层方法结束时才提交。不能简单地认为每次 `mapper.update` 都立即提交。

### 8.11 消费者如何接收分块消息

位置：

```text
rag/src/main/java/com/nageoffer/ai/ragent/knowledge/mq/KnowledgeDocumentChunkConsumer.java
```

```java
@RocketMQMessageListener(
        topic = "knowledge-document-chunk_topic${unique-name:}",
        consumerGroup = "knowledge-document-chunk_cg${unique-name:}"
)
public class KnowledgeDocumentChunkConsumer
        implements RocketMQListener<MessageWrapper<KnowledgeDocumentChunkEvent>> {

    @Override
    public void onMessage(MessageWrapper<KnowledgeDocumentChunkEvent> message) {
        KnowledgeDocumentChunkEvent event = message.getBody();
        documentService.executeChunk(event.getDocId());
    }
}
```

当 `startChunk` 的本地事务返回 `COMMIT` 后，Broker 才会把 `knowledge-document-chunk_topic...` 消息投递到这个消费者。消费者随后执行耗时的解析、分块、向量嵌入和入库流程。

### 8.12 本项目的事务回查器

位置：

```text
rag/src/main/java/com/nageoffer/ai/ragent/knowledge/mq/KnowledgeDocumentChunkTransactionChecker.java
```

初始化时按 Topic 注册：

```java
@PostConstruct
public void init() {
    transactionListener.registerChecker(chunkTopic, this);
}
```

回查时按数据库状态判断：

```java
KnowledgeDocumentDO documentDO = documentMapper.selectById(docId);

return documentDO != null
        && DocumentStatus.RUNNING.getCode().equals(documentDO.getStatus());
```

这比查询内存中的 `txId` 可靠，因为 Broker 回查可能落到任意实例，原发送实例也可能已经重启。

---

## 9. 新项目推荐的抽象方式

如果项目中只有一个 Topic，可以直接在业务服务中使用 `RocketMQTemplate`。当 Topic 增多、事务消息增多时，建议抽象成两层：

### 9.1 Producer 统一适配层

统一负责：

- 生成业务 key 和事务 ID。
- 统一包装消息。
- 设置消息 Header。
- 记录发送日志。
- 发送异常时清理本地事务回调。
- 屏蔽 RocketMQ API 与业务代码的耦合。

接口可以是：

```java
public interface MessageQueueProducer {

    SendResult send(String topic, String keys, String bizDesc, Object body);

    void sendInTransaction(
            String topic,
            String keys,
            String bizDesc,
            Object body,
            Consumer<Object> localTransaction);
}
```

本项目已经采用这一接口：

```text
framework/src/main/java/com/nageoffer/ai/ragent/framework/mq/producer/MessageQueueProducer.java
```

### 9.2 一个统一事务监听器 + 多个回查器

统一监听器负责 RocketMQ 的生命周期回调，多种业务通过接口注册：

```java
public interface TransactionChecker {

    boolean check(MessageWrapper<?> message);
}
```

业务模块只负责：

1. 定义自己的事件对象。
2. 定义自己的 Topic。
3. 实现自己的 `TransactionChecker`。
4. 在 `@PostConstruct` 中按 Topic 注册。

这样可以避免每个业务都重复创建一个 `@RocketMQTransactionListener`，也避免多个监听器争抢同一个底层 Producer。

---

## 10. 普通消息和事务消息的选择

### 10.1 选择普通消息

适合：

- 日志、埋点、统计等允许少量延迟或丢失的场景。
- 消息发送失败可以由业务补偿或定时任务补发的场景。
- 数据库操作和消息不要求同一时间成功的场景。

### 10.2 选择事务消息

适合：

- 数据库本地状态成功后消息才能被消费。
- 数据库失败时消息必须不可见。
- 可以设计可靠回查字段的场景。

不适合：

- 本地事务中包含长时间远程调用。
- 事务操作无法通过数据库状态判断。
- 需要跨多个数据库、多个 MQ 或多个外部系统做强一致 ACID 事务。

对于更复杂的跨系统一致性，可以评估 Outbox、本地消息表、可靠事件表、工作流编排或专门的分布式事务方案。不要为了“用了 MQ”就把所有消息都改成事务消息。

---

## 11. 常见误区和排查清单

### 11.1 误区：给 `executeLocalTransaction` 加 Topic 注解

错误理解：它像 `@RocketMQMessageListener` 一样监听某个 Topic。

正确理解：它绑定 Producer 的事务回调；同一个 `RocketMQTemplate` 通常只能有一个事务监听器。

### 11.2 误区：`COMMIT` 等于消费者处理成功

`COMMIT` 只表示 Broker 提交 half 消息。消费者还可能：

- 尚未收到消息。
- 处理失败并重试。
- 因重复投递执行多次。

### 11.3 误区：回查可以读本地 Map

回查请求可能发送到其他实例，必须查询数据库、事务表或其他共享持久化存储。

### 11.4 误区：本地事务 Lambda 抛异常后仍会提交

只要异常从 Lambda 传播到 `TransactionTemplate`，事务管理器会回滚；监听器捕获异常后返回 `ROLLBACK`。如果业务代码自己吞掉异常，事务模板会认为执行成功并提交，所以不要无意中吞异常。

### 11.5 误区：消息发送成功就不需要幂等

Producer 发送成功和 Consumer 处理成功是两个独立结果。消费者仍应使用唯一键、消费记录表或状态机保证幂等。

### 11.6 排查顺序

```text
1. NameServer 地址是否正确、网络是否可达？
2. Producer Group 是否配置？
3. Topic 字符串是否完全一致？
4. Consumer Group 是否正确、消费者 Bean 是否被 Spring 扫描？
5. 普通消息是否收到 SendResult，事务消息是否收到 TransactionSendResult？
6. 事务监听器是否成功绑定到对应 RocketMQTemplate？
7. executeLocalTransaction 是否找到 txId？
8. TransactionTemplate 是否提交或回滚？
9. Broker 是否最终收到 COMMIT / ROLLBACK？
10. checkLocalTransaction 是否能从数据库判断最终状态？
11. 消费者是否因异常反复重试？
12. 是否发生重复消费而被误认为消息重复发送？
```

建议日志至少包含：Topic、业务 key、消息 UUID、事务 ID、Producer Group、Consumer Group、Broker 返回状态和业务主键。日志不要打印密码、Token、完整文件内容等敏感数据。

---

## 12. 最小可运行检查清单

### 普通消息

- [ ] Docker 中 NameServer 和 Broker 已启动。
- [ ] Spring Boot 引入 `rocketmq-spring-boot-starter`。
- [ ] 配置 `rocketmq.name-server` 和 Producer Group。
- [ ] Producer 使用正确 Topic 发送消息。
- [ ] Consumer 使用相同 Topic、稳定 Consumer Group。
- [ ] `onMessage` 处理异常时抛出异常，让 RocketMQ 重试。
- [ ] 消费逻辑具备幂等能力。

### 事务消息

- [ ] 一个 `RocketMQTemplate` 只绑定一个 `@RocketMQTransactionListener`。
- [ ] 发送前先注册 `txId -> localTransaction`。
- [ ] 消息 Header 携带 txId 和 Topic。
- [ ] 本地事务由 `TransactionTemplate` 或 `@Transactional` 管理。
- [ ] 本地事务成功才返回 `COMMIT`。
- [ ] 本地事务失败返回 `ROLLBACK`。
- [ ] 发送异常和非成功状态时清理本地回调。
- [ ] 为每个事务 Topic 注册可查询数据库的 `TransactionChecker`。
- [ ] `checkLocalTransaction` 遇到暂时无法判断时返回 `UNKNOWN`。
- [ ] 消费者仍然实现幂等和失败重试。

---

## 13. 一句话总结

普通消息是“发送成功后异步消费”；事务消息是“先把消息隐藏起来，执行本地事务，确认提交后再让消费者看到”。

本项目的关键设计可以压缩成三条：

```text
1. @RocketMQTransactionListener：绑定 Producer 事务回调，不绑定 Topic 消费。
2. txId：定位当前消息对应的本地事务 Lambda。
3. topic：定位事务回查器；回查必须依赖数据库等共享持久化状态。
```

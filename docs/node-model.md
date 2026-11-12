# 节点内多模块模型：使用与验证

实现规格见 [node-model-implementation-spec.md](node-model-implementation-spec.md)。这是事务级时序模型：一个 rank 对应一颗加速器，计算单元、显式存储、DMA、mesh 和集合通信在同一仿真时间线上推进；不执行真实数值计算。

## 启用与构建

未设置 `node-model-config` 时保持 legacy 工作流。设置后启用 detailed 模式，目前验证的外部后端为 analytical congestion-aware；其他后端会明确拒绝 detailed 模式。

system 配置示例为 [NodeMesh.json](../examples/system/native_collectives/NodeMesh.json)，它引用 [mesh-node.json](../examples/node/mesh-node.json) 和 [mesh-node-profiles.json](../examples/node/mesh-node-profiles.json)。所有配置引用相对其所在文件解析，可以在其他工作目录启动。

在仓库根目录构建；先按现有项目说明安装编译器、CMake、Boost、C++/Python Protobuf 并初始化 analytical 相关子模块。在本云环境可先运行：

```bash
source /workspace/.astra-env/activate.sh
```

然后运行：

```bash
cmake -S build/astra_analytical -B build/astra_analytical/build -DBUILD_TESTING=ON
cmake --build build/astra_analytical/build -j4
ctest --test-dir build/astra_analytical/build --output-on-failure
```

不需要运行 ns-3/HTSim 或启动后台服务。Python 测试使用项目 Chakra 的 `encodeMessage` 与生成的 protobuf 定义。

## 运行示例

生成四个 rank 的真实 Chakra ET：

```bash
python3 tests/node_model/generate_fixture.py /tmp/astra-node-demo --scenario compute
build/astra_analytical/build/bin/AstraSim_Analytical_Congestion_Aware \
  --workload-configuration=/tmp/astra-node-demo/workload \
  --system-configuration=/tmp/astra-node-demo/system.json \
  --network-configuration=examples/network/analytical/Ring_4npus.yml \
  --remote-memory-configuration=examples/remote_memory/analytical/no_memory_expansion.json
```

该 compute 示例先并行执行两个 12 ns 的算子，再执行依赖二者的第三个算子，每 rank 在 24 ns 完成。

将 `--scenario` 改为 `memory`、`all_reduce`、`reduce_scatter`、`all_gather` 或 `all_to_all`，重新生成同一路径后使用相同命令运行：

- `memory`：分配 CU 内部存储，HBM→CU 搬入，读取/计算/写入，CU→节点共享存储搬出，显式释放。
- 四种集合通信：复用已有 `Sys::generate_collective`、Ring、stream 和分块机制；实际读写存储、使用 NoC 和专用归约单元，再向原后端发送或接收。

## 如何配置存储和互连

`storages` 是可变长实例数组，没有固定两级或固定实例数限制。每个实例分别设置 `capacity_bytes`、`bytes_per_ns`、`access_latency_ns`、`queue_entries`：

- `cu_private` 的 clients 为所属 CU。
- `group_local` 的 clients 为共享该实例的 CU 列表；同一节点可有多组。
- `node_shared` 的 clients 为 `["*"]`；集合通信和直接网络收发使用此范围的存储。
- `sram/hbm/ddr` 的技术类型不会触发自动数据搬移。

CU 内部存储通过 `local_storage` 和 `direct_links` 绑定。访问其他存储时走 CU.fabric；DMA、存储、集合通信及 NIC 的 fabric 端口通过 `noc.attachments` 挂到 mesh router。`router` 坐标为 `[x,y]`，XY 先 x 后 y。端口只能连接一次，多个端点挂在同一 router 上各有自己的注入/弹出链路。

请求、数据及返回响应都消耗路径资源。相反方向链路独立；共享有向链路竞争，互不相交路径可以并行。有限缓冲、注入 credit 和存储服务队列共同提供背压；存储响应无法注入时保留服务队列项。

拓扑连接保存在节点 JSON 的 `direct_links` 和 `noc.attachments`，由 `NodeTopology` 构造成端口与有向链路图；运行中的队列、占用和仲裁保存在 `MeshNoC`。每个 `Sys` 持有一个 `NodeSystem`，其中的 `NodeModel` 管理 CU、存储、DMA、集合通信和 NIC；这些内部模块不增加 rank。

对外仍沿用 `AstraNetworkAPI` 的 send/recv、调度及完成回调。内部资源竞争会改变实际发包时间，外部到达时间也会改变内部写入、归约和 ET 后继的启动时间，因此不是预先计算一个固定节点耗时。`Workload` 只在操作真正完成时解除 ET 依赖。

## 数据位置与容量

这不是缓存模型。每个 buffer 指定 `storage + buffer_id + generation`，读写范围由 offset/bytes 指定；不会在“读不到”时自动到其他层取数。

`initial_buffers` 在启动时预留容量；`allocate`、`free`、`dma_copy` 和 read/write 构成操作阶段图。目的 buffer 必须先分配，copy 默认保留源。工作区和在途目的分配都计入对应存储容量。

容量不足抛出 `CapacityExceeded`，报告容量、占用/预留、请求量和活跃分配；碎片导致无法放入连续区域也报错。不会驱逐、溢出到 HBM/DDR 或等待将来释放。队列暂满才属于可以恢复的背压。

合法覆盖写不重复占用容量。`free` 不能回收在途使用中的 buffer；释放后重用 ID 要递增 generation。跨存储副本不会自动同步，通过显式 copy 和阶段依赖管理可见性。

## 集合通信契约

- profile 的 operation、input 长度及 participants 必须与 ET/communicator 一致。
- detailed 模式选择覆盖该 communicator 的**扁平逻辑 Ring**，保留 profile 的精确字节数，不使用 legacy 的小块数据量扩张。它不模拟 legacy 的分维层次 collective 策略。
- 当前 analytical congestion-aware 后端本身仅支持一维外部网络，二维外部网络配置会被后端拒绝；这不限制节点内部的二维 mesh。逻辑 Ring 的每条消息仍经真实的外部后端路由。
- 沿用 `preferred-dataset-splits` 分块；首版要求非空等长块并满足元素对齐。Tree、自定义 collective 和非 Ring 配置明确拒绝。
- All-Reduce/All-to-All 输入输出等长；All-Gather 输出为输入×组大小；Reduce-Scatter 输出为输入÷组大小。
- All-Reduce/All-Gather 默认将 output 用作工作区，也可指定独立 workspace，结束后会回拷结果。
- Reduce-Scatter 要求显式 workspace，至少能容纳完整输入；All-to-All workspace 至少为输入÷组大小，用于逐跳转发。它们也占实际存储容量。
- 同一 communicator 的实例按一致顺序提交；跨 rank 会检查 instance/op/大小签名。不同 communicator 的 stream/tag 使用现有分区，并对溢出报错。
- 本地收写/归约完成后才推进算法；发送完成不表示远端数据已经可见。
- NIC 预留约束本地提交/消费，不等于对外部网络的物理 credit 背压。

## 统计和追踪

每 rank 的完成日志含一行 `node-model-summary` JSON，包含有效配置、逐操作时间、逐存储容量与吞吐、DMA、NoC 链路、NIC 以及未完成请求诊断。

`memory_stall_ns`、`dma_wait_ns`、`noc_wait_ns`、`storage_queue_wait_ns`、`external_network_wait_ns` 是按请求/packet 累计的等待量，可能相互重叠或超过 wall time，不能相加作为总时间分解。`compute_busy_ns` 是实际 compute 阶段服务时间，旧 exposed-communication 减法不用于 detailed 模式。

在节点 JSON 增加以下字段可启用有界追踪：

```json
"trace": {"enabled": true, "max_events": 10000}
```

trace 记录 rank、op、txn、parent、stage、module、tick；存储事件记录 buffer/generation/offset，链路事件记录 packet/port。到达 max_events 后明确标记 truncated，不继续增加日志内存。

## 验证范围和限制

2026-11-11 本云环境中的验证结果：

| 检查 | 结果 |
| --- | --- |
| 常规构建与 CTest | 两个测试目标全部通过：18 项 C++ 接口测试、28 项 Python 端到端测试 |
| Debug + ASan/UBSan/LeakSanitizer | 18 项 C++ 测试全部通过，开启泄漏检测 |
| Debug + ASan/UBSan frontend | 28 项端到端测试全部通过；关闭泄漏检测以区分下述既有 frontend 泄漏 |
| 文档示例 | compute、memory 及四种 collective 全部在四 rank 上完成；compute 为 24 ns |
| legacy 回归 | 参考 stdout 完全一致；aware 16-rank All-Gather、unaware 4-rank Reduce-Scatter 原有示例均完成 |

- C++ 接口测试覆盖多 CU、admission 恢复、容量与并发预留、释放/generation、DMA、读往返、全双工/独立 mesh 路径和有限缓冲。
- Python 集成测试经真实 ET reader 和 analytical 可执行程序运行，覆盖四种 Ring、独立 communicator、工作区回拷、singleton、点对点、空/metadata trace、容量错误、trace，以及 legacy 参考输出。
- 集成测试还验证降低本地 NoC 带宽会推迟对外发送和算子完成；不是仅给 collective 附加固定延迟。
- 模型不执行 tensor 数值运算；输入 profile 决定访问序列和所需同步，不验证算子数值正确性。
- 不实现硬件缓存、DRAM 行/bank 精确时序、wormhole、能耗和对外硬件 credit。首版一个算子固定一个 CU，未实现算子自动跨 CU 切分。
- 节点内每操作最多 8 个阶段在途，每内存访问最多 8 个子事务；这些首版固定窗口会随有效统计配置一起导出。
- 独立节点模块使用 ASan/UBSan/LeakSanitizer 检查。完整 frontend 的 legacy 路径存在 `CollectiveImplLookup` 既有泄漏；集成 ASan/UBSan 检查需与该基线泄漏区分，不能将其报告为新节点模块已通过全量泄漏检查。

示例参数用于验证功能，尚未用真实硬件测量校准。用于性能预测前，应分别通过纯计算、单存储连续读写、单路径传输与共享路径争用微基准校准启动时延、持续吞吐和竞争行为，再组合运行计算/DMA/通信重叠场景。

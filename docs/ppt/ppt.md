<!-- Slide number: 1 -->
Part Ⅰ 工作介绍

### Notes:
可参考工作：www.vldb.org/pvldb/vol12/p516-zeuch.pdf Why it matters？Figure 1很好的展示了效果

<!-- Slide number: 2 -->
What is the problem?
背景：现代算法和神经网络模型用高维向量（embedding）表示一个实体，语义相近的实体在向量空间中的距离也更接近。

连接（Join）是处理传统结构化数据的经典操作。当面临无限的数据流时，为保证计算的有效性，通常需要采用窗口化机制将处理范围限定于近期数据。为了满足流处理对高吞吐和低延迟的要求，利用多核处理器的并行计算能力对其进行加速，已成为主流的技术路径。

流式向量相似性连接 (Streaming vector similarity join) 开始成为现代流应用（例如，数据聚合、数据清理、推荐系统）的核心部分。

![이미지 단색으로 채워진](GoogleShape383p84.jpg)
Query Image Embedding
| 0.43 | -0.2 | 1.54 | 8.21 | -4.2 | 1.49 |
| --- | --- | --- | --- | --- | --- |
| 0.43 | -0.2 | 1.54 | 8.21 | -4.2 | 1.49 |
| --- | --- | --- | --- | --- | --- |
What is the most similar
image embedding to the query image?
| 0.43 | -0.2 | 1.54 | 8.21 | -4.2 | 1.49 |
| --- | --- | --- | --- | --- | --- |
| 0.43 | -0.2 | 1.54 | 8.21 | -4.2 | 1.49 |
| --- | --- | --- | --- | --- | --- |
| 0.43 | -0.2 | 1.54 | 8.21 | -4.2 | 1.49 |
| --- | --- | --- | --- | --- | --- |
| 0.43 | -0.2 | 1.54 | 8.21 | -4.2 | 1.49 |
| --- | --- | --- | --- | --- | --- |
| 0.43 | -0.2 | 1.54 | 8.21 | -4.2 | 1.49 |
| --- | --- | --- | --- | --- | --- |
| 0.43 | -0.2 | 1.54 | 8.21 | -4.2 | 1.49 |
| --- | --- | --- | --- | --- | --- |
| 0.12 | -0.1 | 4.12 | 1.43 | -2.0 | 1.5 |
| --- | --- | --- | --- | --- | --- |
Image Embeddings

![](图形18.jpg)

![](图片1.jpg)

### Notes:
图片补充join在多核处理器上进行

<!-- Slide number: 3 -->
What is the problem?
新兴的流式连接技术复用了向量算子，但引入了滑动窗口语义，这在共享内存架构上带来了动态状态维护、数据过期与细粒度并发的第二重复杂度。

简单移植现有连接技术面临着根本性的结构限制：除了“维度灾难”带来的计算压力外，最核心的问题在于向量缺乏严格的全序关系。这导致标准的流式分区机制（如keyBy）失效，无法有效地将相似度计算负载进行局部化处理。

### Notes:
图片补充join在多核处理器上进行

<!-- Slide number: 4 -->
Why it matters?
多数据源信息聚合
跨监控目标重识别

![](图片4.jpg)

![](GoogleShape391p85.jpg)

![](图片109.jpg)
核心需求：大语言模型（LLMs）依赖外部知识，以生成具备上下文感知的实时回复。
面临挑战：长期动态任务要求系统具备实时知识更新与持续处理高维向量流的能力。

<!-- Slide number: 5 -->
Why existing work fail?
| 分类 | 方法名 | 核心特点 (Feature) | 主要缺陷/局限性 (Limitation) |
| --- | --- | --- | --- |
| 第一类：直接相关工作(加速 VSJ) | ADSSJ | 采用聚类+分布式架构，将高维向量流映射到不同节点以减少计算量。 | 分布式架构带来了较高的网络通信开销和负载均衡挑战，且聚类维护成本高。 |
|  | VectraFlow（Cluster） | 采用聚类方法，将向量操作（V-Join）作为原生算子集成到流处理引擎中。 | 作为早期原型系统，其查询优化器支持有限，且聚类模型的实时更新可能引起抖动。 |
|  | 索引加速(HDR-Tree, ANNS等) | 利用高维索引（如树结构或图索引）剪枝，显著降低候选集数量。 | 难以平衡索引的实时更新（构建成本高）与查询效率，在流式高频插入下性能下降明显。 |
| 第二类：相关工作(部分重叠) | Low-latency Handshake Join | 多核流连接（无向量）：采用双流握手（Bi-flow）模型，通过元组复制和“快进”机制降低延迟。 | 数据复制机制增加了内存带宽压力，且不具备处理高维向量计算密集型任务的能力。 |
|  | SplitJoin | 多核流连接（无向量）：将连接操作拆分为分割（Split）和连接（Join）原语以流水线化执行。 | 在结构化数据上的吞吐量不如 Scale-OIJ 等基于优化数据结构的方法，且未针对向量距离计算优化。 |
|  | Scale-OIJ | 基于键值结构：使用并发\*双层跳表（Skip-List）管理窗口状态，适合大窗口场景。 | 跳表结构的内存占用较高，且依赖键值（Key）进行分区，无法直接解决向量相似度连接中的无Key路由问题。 |
|  | PIM-Tree | 基于键值结构：基于改造的B树（或内存处理 PIM 索引）进行流数据管理。 | B树结构在高并发写入下的锁竞争较为严重，难以适应流式场景下的极致低延迟需求。 |
|  | EDBT22 (LSH+Dist) | 向量连接（无流/并行）：利用局部敏感哈希（LSH）将相似向量映射到同一节点进行分布式连接。 | LSH 存在精度损失（近似解），且跨节点的 Shuffle 操作导致大量网络通信开销。 |
| 第三类：其他 | SimJoin（也可放第二类） | 使用静态索引Join检索，对查询集通过MST优化Join顺序来提高窗口结果的复用率 | 设计为面向静态数据的批处理算法，缺乏对动态数据流和高并发实时更新的支持。（论文中提到动态和并发改造） |
|  | FGF | 使用 FGF (Fast General Form) Hilbert 空间填充曲线对数据排序以提升缓存局部性。 | 需要对数据进行复杂的预排序和空间转换，难以在动态流数据上实时维护这种全局有序性。 |
|  | VBase | 统一了向量搜索与关系查询，利用松弛单调性（Relaxed Monotonicity）优化查询。 | 系统设计侧重于数据库的复杂查询（如 TopK+Filter），而非纯粹的高吞吐量流式连接，架构较为厚重。 |
|  | DiskJoin | 针对单机磁盘环境，通过分桶（Bucket-wise）和访问批处理优化 SSD I/O。 | 依赖磁盘 I/O，延迟远高于内存算法，不适用于实时性要求极高的流式处理场景。 |
第一类直接相关工作：ADSSJ（聚类+分布式）、VectraFlow（聚类）、使用各类索引（HDR-Tree、ANNS索引等）加速VSJ
第二类相关工作：
做了多核流连接没做向量：
Low-latency Handshake join 、SplitJoin（结构化数据上打不过Scale-OIJ）
用了基于键值的数据结构：Scale-OIJ（使用跳表）、PIM-Tree（使用B树改造）
做了向量连接，没考虑多核并行和流：EDBT22（LSH+分布式）
第三类：DiskJoin、FGF、VBase、SimJoin（SimJoin文中也提到了动态数据和并发的优化方向，也可放在第二类）
传统流式架构分区失效：高维向量天然缺乏全序关系，致使传统流计算依赖的键值分区机制失效，无法在多核间实现有效的数据局部化与负载均衡。
向量技术迁移受阻：现有索引、聚类与LSH方案多面向静态或分布式设计，直接移植存在状态维护开销与同步瓶颈，没有办法充分发挥多核处理器的性能优势。

### Notes:
整理不同工作features之间的对比表格

<!-- Slide number: 6 -->
Why existing work fail?
| 分类 | 方法名 | 核心思路 | 优点 | 缺点 |
| --- | --- | --- | --- | --- |
| 传统等值流连接 | SplitJoin、Scale-OIJ | 基于Key的分区，维护确定性的状态桶或跳表等数据结构 | 聚焦于通用流处理架构，延迟控制与吞吐量优化和成熟，支持负载均衡 | 向量缺乏全序关系，键值分区无法实现相似度感知的路由 |
|  | Low-latency Handshake Join |  |  |  |
| 共享索引 | HDR-Tree | 所有线程并发维护/查询一个全局共享的高维索引 | 通过索引剪枝向量查询路径，降低查询延迟 | 索引结构进行流式动态更新有难度，全局共享索引在多核多线程下存在锁竞争 |
|  | IVF |  |  |  |
|  | HNSW |  |  |  |
| 聚类/哈希分区 | ADSSJ | 利用聚类质心或哈希函数将空间切分，数据流按内容路由到各分区 | 计算局部性强，锁竞争开销少 | 分区策略本身存在开销：聚类维护成本高、LSH参数敏感等。不天然支持负载均衡，需要额外机制支持（ADSSJ）。分区边界存在重复计算，需要结果去重 |
|  | VectraFlow (Cluster) |  |  |  |
|  | EDBT22 (LSH+Dist) |  |  |  |
| 比较维度 | 共享索引 | 聚类/哈希分区 | VSJoin(Ours) |
| --- | --- | --- | --- |
| 无锁/低锁更新 | × | √ | √ |
| 负载均衡 | √ | ○ | √ (○) |
| 多核拓展性 | × | √ | √ |
| 读写解耦 | × | × | √ |
| 窗口快速查询&更新 | √ | × | √ |

### Notes:
整理不同工作features之间的对比表格

<!-- Slide number: 7 -->
Why existing work fail?
第一类直接相关工作：ADSSJ（聚类+分布式）、VectraFlow（聚类）、使用各类索引（HDR-Tree、ANNS索引等）加速VSJ
第二类相关工作：
做了多核流连接没做向量：
Low-latency Handshake join 、SplitJoin（结构化数据上打不过Scale-OIJ）
用了基于键值的数据结构：Scale-OIJ（使用跳表）、PIM-Tree（使用B树改造）
做了向量连接，没考虑多核并行和流：EDBT22（LSH+分布式）
第三类：DiskJoin、FGF、VBase、SimJoin（SimJoin文中也提到了动态数据和并发的优化方向，也可放在第二类）

### Notes:
整理不同工作features之间的对比表格

<!-- Slide number: 8 -->
| 论文 | 处理模型 | 数据类型 | 核心技术 | 并行模型 | 核心策略 | 算法目标 |
| --- | --- | --- | --- | --- | --- | --- |
| Low-latency Handshake join [VLDB’14] | 流式 (Streaming) | 结构化数据 | 基于分区 | 单机多核 | 上下文不敏感分区 NUMA感知 流水线并行 | 精确Join |
| SplitJoin [ATC’16] | 流式 (Streaming) | 结构化数据 | 基于分区 | 单机多核 | 上下文不敏感分区 广播转发 | 精确Join |
| PIM-Tree [SIGMOD’20] | 流式 (Streaming) | 结构化数据 | 基于索引 | 单机多核 | 不可变共享索引 可变分区索引 | 精确Join |
| EDBT '22 | 批处理 (Batch) | 高维向量 | 基于哈希（LSH）+ 分区 | 分布式 | 使用两级LSH进行数据分区和解决节点内子问题 | 近似 Distance-based join |
| FGF-Hilbert [SIGMOD’19] | 批处理 (Batch) | 高维向量 | 基于分区 | 单机多核 | 采用FGF-Hilbert遍历，使遍历顺序对cache不敏感，根据工作负载划分chunk后使用omp并行 | 精确 ϵ-join |
| VBase [OSDI’23] | 批处理 (Batch) | 高维向量 | 基于索引 | 单机 | 使用静态索引加速Join检索 将knn检索改造为ϵ相似度检索 | 近似 ϵ-join |
| SimJoin [SIGMOD’25] | 批处理 (Batch) | 高维向量 | 基于索引 | 单机 | 使用静态索引Join检索，对查询集通过MST优化Join顺序来提高窗口结果的复用率 | 近似 ϵ-join |
| ADSSJ [DEBS ’23] | 流式 (Streaming) | 高维向量 | 基于聚类 | 分布式 | 采用空间分区对Worker进行划分，Worker内部的多个workset通过三角不等式确定的阈值进行内外分区 | 精确 ϵ-join |

### Notes:
整理不同工作features之间的对比表格

<!-- Slide number: 9 -->
| 论文 | 处理模型 | 数据类型 | 核心技术 | 并行模型 | 核心策略 | 算法目标 |
| --- | --- | --- | --- | --- | --- | --- |
| HDR-Tree [ADC’22] | 流式 (Streaming) | 高维向量 | 基于索引 | 单机 | 针对高维数据设计HDR-Tree | 精确+近似 K-NN Join |
| …还有一些Knn-Join |  |  |  |  |  |  |
| DiskJoin [SIGMOD’26] | 批处理 (Batch) | 高维向量 |  | 单机 |  |  |
| Streaming Similarity Self-Join [VLDB’16] | 流式 (Streaming) | 高维向量 | 基于索引 | 单机 | 以MiniBatch的形式对join进行流水线处理，采用L2索引（文中的一种倒排索引）对向量进行过滤检索 | Self ϵ-join |
| Scale-OIJ [ICDE'23] | 流式 | 结构化数据 | 基于索引 | 单机多核 | 设计了一种双层跳表结构，以及无锁的并发控制框架，还有动态调度算法处理负载不均衡 |  |
|  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |

### Notes:
整理不同工作features之间的对比表格

<!-- Slide number: 10 -->
Why existing work fail?
| 流式数据连接 | Index-based Join： [VLDB’14] LLHS/[ATC’16]SplitJoin/[SIGMOD’15] BiStream ：基于内容不敏感的随机分区，分区内使用局部索引来加速Join计算。改为向量索引可支持向量连接，缺点是需要所有线程都可用才能正确生成结果。 [SIGMOD’20]: 基于B+-Tree设计索引（PIM-Tree），较难改造为支持向量流的连接 Hash-based/Sort-based： [SIGMOD’21]: IaWJ算法的Benchmark。展示了多核环境中实现数据并行的基本模式：共享内存 ：如 NPJ。无共享分区：如 PRJ, MPass, JB。复制/广播：如 JM。 |
| --- | --- |
| 向量数据连接 | [SIGMOD’19] FGF-Hilbert: 精确相似度Join [EDBT’22]: 使用数据分区+LSH来处理分布式的Similarity Join问题 [OSDI’23]: 仅基于静态索引（IVF、HNSW）实现了流-表向量相似度连接 [arXiv’24]Xling:采用基于学习的技术来预测某个数据点是否具有足够数量的连接结果 [SIGMOD’25] : 提出使用k-ANN的邻近图（Vamana、HNSW、NSG等）作为索引来辅助进行相似性Join。同时对查询集通过最小生成树（MST）优化Join顺序来提高窗口结果的复用率。文中讨论了其基于MST的并行方案，以及邻近图动态修改的方案。因此可以支持流-表的相似度连接。若要改为流-流的相似度连接，则需要探索MST的动态更新。 |
| 流式向量处理系统 | [CIDR’25]: 使用聚类，以类哈希的方式实现了V-Join作为滑动窗口内向量流的连接，但未进行并行优化，性能有较大提升空间 |
经典的并行滑动窗口流式Join方法依赖于特定的数据分区策略和传统的数据结构，直接往向量数据迁移效果并不理想。
而现有的向量处理技术要么缺乏对多核并行能力的有效利用，要么是基于批处理的方法，应用于本课题时仍存在前述提出的挑战。

### Notes:
整理不同工作features之间的对比表格

<!-- Slide number: 11 -->
Why existing work fail?

SIGMOD’20 : Parallel Index-based Stream Join on a Multicore CPU
方案：对于基于索引的Window Join操作，其设计了一个用于流连接的高效索引PIM-Tree，其由一个可变组件（TI）和一个不可变组件（TS）组成 。新的元组最初被插入到插入高效的可变组件中。当该组件达到阈值时，它将合并到搜索高效的不可变组件中，在合并的同时丢弃过期的元组。可变组件（TI）进一步划分为多个不相交的范围Bi，每个范围与一个B+树和一个锁相关联，这种多分区的允许多个线程同时对不同的值范围执行并发操作。文章还对并发环境下进行数据流Join中面临的并发挑战、数据乱序到达做了流程的设计来保证结果的正确性。
结果：PIM-Tree相比其他索引显著降低了延迟，在多核CPU上能实现比单线程方法5倍以上的吞吐量。
局限：该索引仅适用于结构化数据的连接操作，难以直接拓展到向量数据的连接。

![](GoogleShape416p88.jpg)

### Notes:

<!-- Slide number: 12 -->
Why existing work fail?

OSDI’23 : Unifying Online Vector Similarity Search and Relational Queries via Relaxed Monotonicity(VBase)
方案：由于高维向量索引通常不具备严格单调性，VBase引入了“松弛单调性”的概念 。在进行范围筛选时，VBase不仅仅检查当前遍历到的向量是否超出了距离范围R，它还需要同时满足“松弛单调性”的检查。针对stream-to-table的join场景，VBase对现有的ANNS方案（HNSW，IVFFlat）进行改造，使其原本用于TopK的查询接口改为适合VBase的通用查询接口，从而能支持向量范围筛选的单调性检查。
结果：VBase比Baseline（执行嵌套循环连接和全表扫描）快7900倍，并且召回率达到0.9992 。
局限：文章基于静态索引了实现了Join操作，因此不支持流式数据的Join。

![](GoogleShape430p90.jpg)

### Notes:

<!-- Slide number: 13 -->
Why existing work fail?

arXiv’21 :A Fast and Accurate Graph-Based ANN Index for Streaming Similarity Search (FreshDiskANN)
方案：其基于DiskANN的分片构图以及量化压缩技术进行索引的构建和存储。关于动态更新，其通过α-RNG的性质指导边剪枝过程，保证图在动态更新下的可导航性。其在主存中维护了一个可读写的RW临时索引和多个只读的RO快照索引，以及在SSD中的长期索引。查询会对全部类型的索引进行检索并合并结果；更新会被首先写入RW索引中，并定期生成RO索引快照，在插入一定量后触发StreamingMerge操作在后台进行全局索引的合并。合并过程先检索RO索引中新点的可能近邻并暂存在主存的临时数据结构中，最后分块对SSD中的全局索引进行更新和剪枝。
结果：能在十亿级数据的流式更新场景中实现较高的召回率和较低的延迟。

### Notes:

<!-- Slide number: 14 -->
Why existing work fail?

SOSP’23 :Incremental In-Place Update for Billion-Scale Vector Search (SPFresh)
方案：基于SPANN的聚类索引方案实现（通过K-Means将数据划分为多个小的聚类，并对聚类边缘的数据进行复制提高其被搜索到的概率以提高召回率，并通过图结构组织聚类中心以加速中心点扫描），其通过LIRE算法，在每次向量更新时分割或合并聚类分区，并重新分配附近分区的向量来适应数据的变化。
结果：以较低开销在磁盘上实现了原地增量更新的向量索引，相比现有定期全局重建的方法有着更高的准确率和更低的延迟。

局限：基于硬盘存储设计，对于完全在内存中进行的滑动窗口计算而言，其架构显得较为“笨重”；其次，这些算法并未针对滑动窗口“先进先出”的数据周期性过期模式进行优化；最后，它们的核心目标是Top-K相似性搜索，而非本课题所关注的、找出所有满足阈值的向量对的相似性连接查询。

### Notes:

<!-- Slide number: 15 -->
Why existing work fail?

CIDR’25 : VectraFlow: Integrating Vectors into Stream Processing
方案：提出了一个面向流的数据流引擎，将向量处理直接集成到流数据系统中，从而支持流式数据引擎中的原生向量处理能力。用于支持涉及向量数据的可扩展监控应用 。该系统扩展了关系模型以支持向量数据类型 ，并引入了针对流式环境的向量查询算子（如 iV-Filter, iV-TopK, V-Join）。为高效处理向量流，采用了聚类 、新的内存索引结构（如 Centroid OPList ）等优化技术，针对向量流连接（V-Join），其提出了使用聚类优化连接操作，通过学习输入向量的聚类分布，可以采用类似传统哈希的连接方式。
结果：针对流式查询算子，论文所提出的方法（如 Centroid OPList、聚类优化）在吞吐量和延迟方面相比基线（如暴力计算、HNSW）有显著提升，同时保持了可接受的结果召回率，对于V-Join操作，相比暴力算法在固定窗口大小和阈值的情况下可有2到10倍的加速比。
局限：未考虑采用多核并行策略或采用更高效的索引结构来优化Join操作。

### Notes:

<!-- Slide number: 16 -->
Why existing work fail?
Index-based Methods
Partition-based Methods

![Image](Picture4.jpg)

![](图片42.jpg)
In Progress
图1 随线程数增加锁占比变化图
图2 随线程数增吞吐量变化图
图3 并行度（分区数量）与召回率
分区方案：随着并行度增大，空间切割可能导致大量“边界向量”漏算，召回率降低
共享索引方案：随着线程数分配的增多，算子效率提升不明显甚至会降低，因为其未针对多核处理器进行优化，无法充分发挥并行性能

### Notes:
整理不同工作features之间的对比表格

<!-- Slide number: 17 -->
Why existing work fail?
Clustered-Join

![](图片52.jpg)
In Progress

![](图片53.jpg)

![](图片54.jpg)

### Notes:
整理不同工作features之间的对比表格

<!-- Slide number: 18 -->
What is your key idea?

![图示 AI 生成的内容可能不正确。](图片6.jpg)
采用一定的分区策略（例如LSH + Space Filling Curve）[1]，保证每个并行实例中得到的向量分区不重叠。每个并行实例中有一个Local的带锁可变索引（或者通过研究一种轻量的按相似阈值进行多播的规则，局部也做到无锁），同时所有实例共享一个Global的无锁不可变索引。
每个实例中的向量查询时，首先去对侧窗口的全局索引中查找，然后使用共享锁在对侧同分区以及临近的分区（查询范围可调）的局部索引中查找。
查询结束后，使用本分区的互斥锁在当前窗口的可变索引中插入当前查询的向量，过期的向量打上Lazy标记。
定期后台重建不可变索引，同时更新分区状态（重建阈值？更新时机？）
可变索引使用___，不可变索引使用___【需要再做实验测试，备选 IVF(+PQ）、HNSW、Vamana、Bruteforce】
负载均衡仍会受到分区策略影响，需要采用哪些机制进行负载均衡？

### Notes:
[1] VStream: A Distributed Streaming Vector Search System（VLDB’25）

<!-- Slide number: 19 -->
What is your key idea?
1. 采用一定的分区策略（如LSH）并行处理Join任务，通过多播控制分区边界召回
写入阶段：对边界向量复制到k个邻近逻辑分区
查询阶段：只查本分区 Local Index + 全局 Global Index，两路候选合并
2. 写入与查询解耦（Local mutable + Global immutable）
在线写入只进 Local Index（实时、轻量、分区独占）
Global 不在线写，而是后台批量重建，保证查询路径稳定、减少写锁干扰，后台线程同时完成多播去重和索引重建任务
优点：查询延迟低、并发友好（避免跨分区锁/同步）
3. 分区负载均衡
P个分区进一步细化为P*V个逻辑分区(Logical Partition），构建逻辑分区到物理节点的映射表，周期给空闲节点分配更多的逻辑分区
。。。？

### Notes:
[1] VStream: A Distributed Streaming Vector Search System（VLDB’25）

<!-- Slide number: 20 -->
What is your design?

![post_object_image_1945431538](图片1.jpg)

<!-- Slide number: 21 -->
What is the experiment plan?
数据集：

![](图片2.jpg)
+真实世界数据（一段时间内的新闻或者微博等平台热点）
将数据集随机分为两部分作为两条流的数据源

baseline：ADSSJ（聚类+分布式）改造为单机多线程、各类索引加速方案（采用PIM-Tree的策略管理索引）、VectraFlow（朴素的ClusterJoin）、EDBT22（LSH+分布式）改造为单机多线程
实验关键指标：吞吐量，时间延迟，执行耗时breakdown
实验设置：
多核扩展性：系统能随着核数增加实验接近线性的性能增长
数据偏度与负载均衡：使用不同分布的数据集模拟不同偏度的数据，统计吞吐量变化与各cpu利用率
recall和吞吐量权衡：调整索引关键参数（如探索队列大小，重建时间等），绘制recall与吞吐量变化曲线
敏感度分析：调整窗口大小W，相似度阈值等参数，查看系统性能变化

<!-- Slide number: 22 -->
What is the takeaway?
“你还想告诉别人什么？”
	在文章中，一般会在Conclusions和Limitations展开。
	这里可以讲一些自己对于该方向的见解【宣传】，
		           一些基于本方向的扩展，
		           以及一些没有做的遗憾（让读者去探索边界）~
+ window 的实现方法 ？
    + intra-window 考虑单个window？ 双倍长度再考虑 ？
+ 数据流合并的时候涉及到合并两个向量， 保留平均值， 还是设计数据结构进行合并?
+ Time-Tester Where ?

+ Vectordatabase
    + how to use
    + where to storage

+ 分为 Eager / Lazy
    + Eager
        + ANN
        + 嵌套
        + No-Index
    + Lazy
        + ANN
        + 嵌套
        + No-Index


+ Eager
    + 对两边分别动态维护长度为窗口的索引

+ Lazy
    + 窗口长度限制 ？
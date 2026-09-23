# 分布式任务工作流DAG编排与执行引擎

基于Vue 3 + FastAPI的任务编排平台，DAG拓扑排序、任务状态机、多Worker并发池、执行甘特图。

## 目标用户
数据工程师、ETL/ML Pipeline开发者、技术架构师

## 技术栈
- 前端: Vue 3 + TypeScript + Vite + Pinia + Element Plus + ECharts
- 后端: Python FastAPI + NumPy + SQLite + WebSocket

## 核心功能
1. DAG工作流编辑器：拖拽添加任务节点、连线建立依赖关系、BFS拓扑排序验证环检测
2. Spring StateMachine风格任务状态机：PENDING→RUNNING→SUCCESS/FAILED/TIMEOUT
3. 多Worker并发池模拟：可配置Worker数量、任务执行耗时模拟(指数分布)
4. 任务编排策略：FIFO/优先级/最大并发三种调度策略
5. 重试机制：可配置最大重试次数、指数退避延迟
6. 执行监控：ECharts甘特图时间线渲染、实时WebSocket推送任务状态
7. 熔断保护：连续失败阈值触发熔断，冷却时间后自动恢复
8. 编排校验：创建时校验名称非空且不重复（字段级错误提示）；Tarjan环检测识别互相等待的环节，创建后提示无法排定先后，未受影响环节照常执行
9. 工作流状态机：排队待执行(PENDING) → 执行中(RUNNING) → 结束(FINISHED)；中途打断回到排队待执行并保留已完成环节，可断点续跑
10. 配置绑定与恢复：并发上限/优先级策略在创建时绑定，多次执行保持一致；页面重开自动恢复最后查看的工作流与进展位置（含日志滚动位置）

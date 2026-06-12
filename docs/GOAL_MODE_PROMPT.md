# TurboBus Goal Mode Prompt

Use this prompt for each new TurboBus goal-mode round.

每轮开始必须先读取并严格遵守：

1. `AGENTS.md`
2. `docs/TURBOBUS_ROADMAP.md`
3. `docs/NEXT_STEPS.md`
4. `docs/PROGRESS.md`

目标规则：

- 不在提示词里硬编码当前核心目标。
- 每轮必须根据 `docs/NEXT_STEPS.md` 的 `Current Main Target`、`Current Code Work`、`Next Entry`，以及 `docs/PROGRESS.md` 的 `Current State`、`Remaining Risk`、`Next Main Target`，判断当前唯一主目标。
- 若冲突：`docs/NEXT_STEPS.md` 优先，其次 `docs/PROGRESS.md`，再次 `AGENTS.md`，最后 `docs/TURBOBUS_ROADMAP.md`。
- 当前主目标完成后停止，不自动进入下一个目标。
- 不提前做下一个目标，除非它是完成当前主目标所必需的最小阻塞项。

当前阶段原则：

- 只推进系统本体，不推进 benchmark、example、paper validation、server validation、新测试、替代验证 CLI、fake receipt、synthetic evidence、dry-run deliverable。
- adapter migration 只有在直接阻塞当前主路径时才允许最小范围推进。
- 不因本地缺 CUDA、vLLM、多 GPU 或服务器环境而新增 mock gate、假执行入口、本地替代框架。

硬性架构约束：

- daemon / scheduler 是唯一生产 transfer plan 的来源。
- 应用、benchmark、adapter 只能提交 `TransferIntent`、消费 `TransferReceipt`。
- worker / data plane / CUDA executor 只能执行 daemon-issued `ExecutionTicket` 或 exact daemon-issued plan。
- 不让应用、benchmark、adapter 选择 direct / relay / pool / target GPU / relay GPU。
- 不恢复旧 Runtime / planner 兼容 API。
- 不恢复单进程、单作业、手动 relay 路线。
- 不把 synthetic topology、fake receipt、JSON artifact、dry-run 输出当复现证据。
- 不让 benchmark 或 example 反向定义核心架构。

每轮推进要求：

- 每轮必须完成一个完整系统功能子目标，让系统新增一个真实代码能力。
- 每轮成果必须是一个可以独立描述的系统能力闭环，不能只是一个局部 bug 修复、等待语义调整、字段改名、helper 移动、导入清理、文档同步或边界收紧。
- 如果本轮涉及重构或删除旧入口，必须继续推进到同一系统边界下的真实功能闭环，不能停在半路。
- 不要把一个系统子目标拆成很多轮小点推进。
- 优先收敛唯一生产入口；如果发现 `TurboBusRuntimeSession` 与其他 production-looking 路径重复，优先收紧或消除重复职责。
- 不要把“修一个 bug”当成当前阶段主要推进方式。
- 只有当一个改动让系统多出一块可独立表述的真实能力时，才算本轮完成。

优先闭环类型：

- PCIe shared-fabric bandwidth-pool 闭环。
- block-level scheduling 和动态 path allocation 闭环。
- daemon-issued ticket / lease / progress / receipt 闭环。
- worker / backend / CUDA 执行闭环。
- buffer registration 到真实执行路径闭环。
- runtime session 到 daemon / worker / socket 的生产启动与执行闭环。
- workload adapter 到真实 transfer receipt 的闭环。

每轮优先交付的成果粒度示例：

- 一整条 PCIe fabric graph -> bandwidth pool -> scheduler scoring 闭环。
- 一整条 block plan -> execution ticket -> worker execution -> receipt 闭环。
- 一整条 buffer registration -> execution -> cleanup -> receipt 生命周期闭环。
- 一整条 TurboBusRuntimeSession -> daemon / worker / socket 的生产启动与执行闭环。

不应单独作为一轮成果的内容示例：

- 单独改一个 wait 语义。
- 单独修一个 receipt 解析分支。
- 单独收紧一个兼容 API。
- 单独移动 helper 或调整字段。
- 单独更新文档或计划文件。
- 单独为了“看起来更严谨”做边界修补。

工作方式：

- 每轮开始先执行 `git status`，识别已有脏改动。
- 不覆盖、不回滚、不混入与当前子目标无关的已有改动。
- 当前代码入口与优先文件以 `docs/NEXT_STEPS.md` 的 `Current Code Work` 和 `Next Entry` 为准。
- 优先修改生产主路径，不先改 benchmark、example、测试来倒逼系统实现。
- 可以协调 sub-agent 并行处理不重叠的调研、实现或检查任务。
- sub-agent 不能决定当前主目标，不能扩大阶段范围，不能让 benchmark、example 或测试反向定义架构。
- 主 agent 必须负责集成、审查、验证、提交、push 和最终完成判定。

文档要求：

- 只要本轮完成了真实系统子目标，就同步更新 `docs/NEXT_STEPS.md` 和 `docs/PROGRESS.md`。
- 这两份文件只保留当前状态、当前主目标、当前代码入口、下一步计划、当前剩余风险。
- 不要累计历史完成记录，不要把文件写成长流水账。
- `docs/NEXT_STEPS.md` 始终只保留一个当前唯一主目标。
- `docs/NEXT_STEPS.md` 和 `docs/PROGRESS.md` 中也要体现“按完整系统能力闭环推进，而不是按小 bug / 小点推进”的当前规则。
- `AGENTS.md` 和 `docs/TURBOBUS_ROADMAP.md` 只在长期方向或全局顺序真正变化时才最小更新。

验证要求：

- 当前阶段不新增测试、实验、benchmark、paper validation、服务器验证。
- 只运行与本轮主目标直接相关的最小现有检查。
- 文档-only 改动运行 `git diff --check`。
- Python 改动优先运行 `python -m py_compile` 针对相关文件。
- 不创建新的验证入口替代真实系统实现。

完成判定：

- `docs/NEXT_STEPS.md` 和 `docs/PROGRESS.md` 对当前状态、当前主目标、下一入口没有冲突。
- 本轮确实新增一个可以独立描述的系统能力闭环。
- daemon / scheduler 仍是生产 transfer plan 的唯一来源。
- 应用、benchmark、adapter、worker、CUDA executor 没有获得 route / relay / pool 选择权。
- staged diff 只包含本轮相关文件。
- 检查命令覆盖本轮改动的直接风险。

提交要求：

- 提交前确认 `git diff --cached` 只包含本轮相关文件。
- 不提交与本轮无关的已有改动。
- 完成本轮子目标后，commit 并 push 当前分支。
- 如果因外部原因无法 push，要明确说明原因，但不能因此跳过本轮系统实现。

最终回复必须包含：

1. 本轮开始时判断出的当前主目标。
2. 本次完成的完整系统子目标。
3. 修改的关键文件及其职责变化。
4. 本次运行的检查命令和结果。
5. 当前未完成但属于后续统一验证的风险。
6. commit id 和 push 结果。

// ==================== Application Initialization ====================
// 默认落在「首页」：左侧 catalog 目录树 + 右侧 SQL 工作台。
// 任务管理页的轮询/任务列表/DAG 由 dashboard.js 在切 tab 时接管。
initTheme();           // 深/浅主题：落 <html data-theme> + 同步 UI / 图表色板 + 顶栏按钮文案
loadJobPerPagePref();  // 任务列表每页条数（localStorage 记忆，默认 10）
loadJobStateFilterPref();  // 任务列表状态筛选（localStorage 记忆，默认「全部」）
switchTab('home');
loadCatalogTree();
bindSqlCatalogPicker();   // 标签栏右上角的 catalog 自绘下拉（原生 select 仅作数据源）
initSqlConsoles();    // 多查询控制台：装载标签模型 + 回填编辑器（须在 bindSqlEditor 之前）
bindSqlEditor();      // 草稿记忆（刷新不丢）+ Ctrl+Z / Ctrl+Shift+Z 撤销重做
bindSqlHighlight();   // 高亮垫层（注释灰 / 关键字蓝），须排在 initSqlConsoles（回填正文）之后
bindSqlDropZone();
bindSqlCaretMemory();
bindSqlAutocomplete();   // 字段/表补全：边打字边弹 + Ctrl+Space，元数据首次触发时预取缓存
bindSqlResultContextMenu();   // 结果表右键：复制列名 / 复制当前数据 / 复制全部数据
bindSqlSplitter();
bindAiInputAutoGrow();
bindAiChat();           // AI 页：发送按钮 + Ctrl+Enter → /api/ai/chat
bindRefreshGuard();     // 整页刷新（F5 / Ctrl+R / 工具栏刷新）拦截：提示改用顶栏刷新按钮

# 大型站点地图·代际遍历

面向合作资料接收人员的 Python 采集组件。解析 XML 站点地图或索引，支持 gzip 输入并保留来源声明时间。
输入样本都是本地合成数据，example 域名只表示来源身份，不需联网访问。

## 代际遍历与撤下纪律

资料站把 sitemap 拆成多层索引后会滚动替换文件，旧索引会短暂指向新子文件。
本组件按以下纪律工作：

* 每次遍历保存索引/子图的实体标识（URL + ETag + 正文摘要）、父子引用边和每次获取结果；
  gzip 按响应头与魔数识别，嵌套索引递归展开，循环引用只记录不再展开，
  同一 URL 多处出现保留每条引用并标记重复。
* `lastmod` 只是来源声称的修改时间：原样存档，绝不替代 ETag，也不作为扫描水位；
  未来时间会在汇总中单独列出。
* 任何限额触发、解析错误或网络失败只会让本次视图“不完整”，缺席条目不产生缺失证据。
* 只有连续两次可证明完整且相互独立（不同 run、各自重新完成遍历）的视图都缺少某条目，
  才确认撤下；一次缺席只是候选，仍然保留。
* 中断后按持久化边表只重取未完成节点；子图完成状态用版本号 CAS 提交，
  旧请求无法覆盖并发期间已换代的子图；同一 run 有进程级互斥锁。

```bash
python3 -m collector crawl-demo --fixture fixtures/sitemaps.json
```

演示用本地模拟站点（全部地址映射到回环服务）滚动完成 g1→g2 换代，
输出每个视图的完整性判定、视图差异、子树重读计划，以及哪些条目被保留、
哪些条目在两次独立完整视图缺席后才最终撤下。

## 安装与运行

使用 Python 3.11，依赖仅为标准库，SQLite 随解释器提供，无需安装第三方包。
从项目根目录执行；演示启动真实的回环 HTTP 来源、处理响应、事务入库后读取实际结果。
每次演示使用独立临时目录，结束后清理；通常两秒内结束，不回放历史输出。

```bash
python3 -m compileall -q collector
python3 -m unittest discover -s tests -v
python3 -m collector demo
python3 -m collector crawl-demo
```

常规导入保留在本地数据库中，可重复运行；默认路径已忽略，不会进入版本库。

```bash
python3 -m collector import --input fixtures/sample.json --db var/records.sqlite3
python3 -m collector show --db var/records.sqlite3
python3 -m collector serve --db var/records.sqlite3 --port 8080
python3 -m collector crawl --root-url http://127.0.0.1:8080/sitemap.xml --run-id nightly-1
```

查询服务监听回环地址，`GET /records` 返回数据库中的记录；按 Ctrl-C 停止。

## 组件与数据

`collector/domain.py` 负责来源格式和领域校验；`storage.py` 保留原始对象、导入凭据和当前记录。
单次导入在同一事务内提交，异常时整体回滚；相同来源和原文只登记一次凭据。
当前记录以领域标识为键，同键再次导入替换当前观察值，不代表保留了完整修订历史。
`transport.py` 提供请求客户端和本地响应服务；服务按路径依次消费响应脚本，并记录实际请求。
`api.py` 提供只读查询入口。`fixtures/sample.json` 是当前导入器支持的输入。

代际遍历新增四个模块：`crawlstore.py` 持久化遍历证据与子图完成状态；
`crawler.py` 执行可恢复的完整遍历；`mocksite.py` 由 `fixtures/sitemaps.json`
驱动本地模拟站点（代际切换、事件覆盖、gzip、条件请求）；
`rollover.py` 执行视图差异、撤下确认与可解释说明；`scenario.py` 编排滚动换代演示。
其他附件描述同领域的合成页面、协议或事件，可以由调用方构造本地来源。
当前响应服务是顺序脚本工具，不自动推断认证、文件或表单业务；领域对象提供对应的本地交互。

## 可复现运行

宿主机或容器均不需要网络。镜像准备好以后可以关闭外部网络再运行上述命令。
仓库内不含真实账号、新闻正文或人员资料；不要将生产凭据写入样本或日志。

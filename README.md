# VPNGate SSTP 家宽节点（edgetunnel 链式代理） 🚀

自动抓取 [VPN Gate](https://www.vpngate.net/) 的 SSTP 家宽/机房节点，调用检测 Worker 逐个验证可用性，按国家分组、标注住宅/机房，生成可直接通过 **URL 自动轮换** 的节点清单。**每 30 分钟自动更新一次。**

> 核心价值：VPN Gate 的 SSTP 节点 30 分钟就换一批，手动测试筛选太痛苦。本仓库把它全自动了——你只需把 `nodes.txt` 的网址填进 edgetunnel 后台一次，之后节点每 30 分钟自动换，零手动。

---

## 架构（数据流向）

~~~text
VPN Gate 官方源
      │  (每 30 分钟，GitHub Actions 定时抓取)
      ▼
筛选 SSTP 节点 → 去重
      │
      ▼
检测 Worker (CheckSocks5，部署在 Cloudflare)
      │  GET /check?sstp=vpn:vpn@host:port
      │  返回 success + 出口 IP(住宅/机房判定)
      ▼
保留成功节点 → 按国家分组 → 住宅/机房标注 → 延迟排序
      │
      ▼
生成 nodes.txt (GitHub Pages 发布)
      │
      ▼
edgetunnel 后台「自定义优选IP」框填 https://…/nodes.txt
      │  edgetunnel 每次生成订阅时自动 fetch → 解析 $sstp:// → 套链式代理
      ▼
客户端订阅 edgetunnel 订阅 → 使用 SSTP 家宽节点 (每 30 分钟自动换)
~~~

---

## 一、完整部署教程（从零开始）

### 前置条件
- 一个 Cloudflare 账号（免费即可）
- 一个 GitHub 账号
- 一个已转入 Cloudflare 的域名（可选，但强烈推荐）

### 第 1 步：部署 edgetunnel（核心使用端）

1. 登录 Cloudflare 控制台，点击左侧 **Workers 和 Pages**
2. 点击 **创建** → 选择 **创建 Worker**，起名 `edgetunnel`，点击 **部署**
3. 打开 https://github.com/cmliu/edgetunnel/blob/main/_worker.js ，复制全部代码
4. 回到 Cloudflare Worker 编辑器，粘贴代码，点击 **保存并部署**
5. 在 **设置** → **变量** 中，添加变量 `ADMIN`，值填你的管理员密码
6. 在 **绑定** 中，添加 KV 命名空间绑定，变量名称填 `KV`
7. （推荐）在 **触发器** 中绑定自定义域名
8. 浏览器访问 `https://你的域名/admin`，登录后台
9. **在后台首页记下你的 UUID 和节点域名**（后面要用）

> **关键**：`UUID` 和 `节点域名` 是 edgetunnel 自己的配置，**不需要**在 `vpngate.py` 中设置。`vpngate.py` 只负责生成 `nodes.txt`，edgetunnel 会用它自己的 UUID/域名去生成最终订阅。

### 第 2 步：部署检测 Worker（CheckSocks5）

1. 打开 https://github.com/lsh8848/cm-Workers-CheckSocks5 ，点 **Fork**
2. 进 Cloudflare 控制台 → Workers 和 Pages → 创建 → 创建 Worker
3. 把 `_worker.js` 的全部内容粘贴进编辑器，点「部署」
4. 记下这个 Worker 的域名，形如 `https://xxx.你的用户名.workers.dev`
5. 验证：浏览器打开 `https://你的Worker域名/check?sstp=vpn:vpn@任意节点:端口` ，能返回 JSON 即成功

### 第 3 步：Fork 本仓库

在 GitHub 上打开本仓库，点 **Fork**，复制到你账号下。

### 第 4 步：修改配置（重点）

进你 fork 的仓库，修改 `vpngate.py`：

| 文件 | 位置 | 改成什么 | 为什么 |
| :--- | :--- | :--- | :--- |
| .github/workflows/check.yml | env 里的 `CHECK_WORKER` | 你的检测 Worker 域名，形如 `https://xxx.workers.dev/check?sstp=vpn:vpn@` | 检测统一走你自己的 Worker |
| vpngate.py | `NODES_URL` | 把里面写死的固定地址换成 `你的用户名/仓库名` | 自动更新时用到的固定地址 |

> **注意**：`vpngate.py` 中**不需要**配置 `EDT_UUID` 和 `EDT_DOMAIN`。你之前看到的这两个变量是旧版遗留，现已删除。edgetunnel 后台会自己处理 UUID 和域名。

### 第 5 步：开启 GitHub Pages 与 Actions

1. 进你 fork 的仓库 → Settings → Pages，Source 设为 **GitHub Actions**
2. 进 Actions 页，若提示启用 Actions 就点启用
3. 手动触发一次：Actions → VPN Gate Node Check → Run workflow → Run workflow
4. 等它跑完（约 1 分钟），看到绿色 ✓ 即成功

### 第 6 步：确认产物

跑完后，你的站点地址是：
~~~text
https://你的GitHub用户名.github.io/仓库名/nodes.txt
~~~
浏览器打开，能看到一堆 `优选域名:443#国家-住宅-XX …` 的行，就说明全部打通了。

---

## 二、使用教程（URL 自动轮换，一次配置永久生效）

1. 进 edgetunnel 后台（你的域名/admin），找到「自定义优选IP」文本框
2. 粘贴**一行网址**：
   ~~~text
   https://你的GitHub用户名.github.io/仓库名/nodes.txt
   ~~~
3. 点保存
4. 客户端刷新订阅 → 每次刷新 edgetunnel 都重新拉取一次 nodes.txt，节点自动更新

> 原理：`nodes.txt` 是纯节点行版本（无注释头），每行 `入口域名:443#国家-住宅-01$sstp://vpn:vpn@节点:端口`。edgetunnel 下次生成订阅时会 fetch 这个网址、逐行解析成优选入口 + 链式代理指令。你只填一次，之后节点每 30 分钟自动换、零手动。

---

## 三、如何更换优选域名

入口地址用的是「优选域名」，决定客户端连 Cloudflare 用哪个 IP、稳不稳。

### 在哪个文件改
- 文件：vpngate.py
- 位置：`EDGE_HOSTS = [ ... ]`

### 改法
1. 用测速工具（如 bestcf）测一批 Cloudflare 优选域名，挑「延迟低 + 实际能连通」的
2. 打开 vpngate.py，把 `EDGE_HOSTS` 里的域名列表换成你测出来的（逗号分隔，格式 `域名:443`）
3. 提交推送，等下一次自动运行或手动触发 Action

---

## 四、配置速查表（vpngate.py）

| 常量 | 说明 |
| :--- | :--- |
| `EDGE_HOSTS` | 内置静态入口表，仅在动态池全部失败时回退 |
| `WORKER_CHECK_URL` | 检测 Worker（本地运行默认值，Action 里用 workflow 的 `CHECK_WORKER` 覆盖） |
| `NODES_URL` | 自动更新时用到的固定地址（fork 后改成你自己的） |

### 入口池相关环境变量（在 workflow 的 `env` 里设置）

| 变量 | 默认 | 说明 |
| :--- | :--- | :--- |
| `EDGE_POOL_APIS` | 内置 9 个源 | 整体覆盖入口来源列表，逗号分隔；条目可写 `url`、`url\|json`、`url\|html\|备注` |
| `EDGE_DOMAIN_DNS_CHECK` | `on` | 优选域名 DoH 反查模式：`on`=分级判定 / `strict`=境外解析不到也剔除 / `off`=不校验 |
| `EDGE_DOH_URLS` | `dns.google,cloudflare-dns.com` | 反查用的 DoH 服务，**逗号分隔且依次尝试**（多服务是解决分线路误杀的关键） |
| `EDGE_DOH_ECS` | 空 | 加中国方向 ECS 前缀再问一次。实测对万网分线路域名无效，默认关闭 |
| `EDGE_WILDCARD_PREFIX` | `bestcf` | 把 `*.example.com` 这类泛域名补成 `bestcf.example.com` |

内置来源（优选 IP + 优选域名，全部由社区众包、每 12 小时重建）：

~~~text
# 优选 IP
https://addressesapi.090227.xyz/CloudFlareYes                  带 CM/CU/CT 标签
https://ipdb.api.030101.xyz/?type=bestcf&country=true
https://raw.githubusercontent.com/cmliu/WorkerVless2sub/main/addressesapi.txt
https://raw.githubusercontent.com/DustinWin/BestCF/bestcf/cmcc-ip.txt   移动专用（含 IPv6，自动剔除）
https://raw.githubusercontent.com/DustinWin/BestCF/bestcf/cucc-ip.txt   联通专用
https://raw.githubusercontent.com/DustinWin/BestCF/bestcf/ctcc-ip.txt   电信专用

# 优选域名（CNAME 池，客户端 DNS 就近解析）
https://raw.githubusercontent.com/DustinWin/BestCF/bestcf/bestcf-domain.txt
https://vps789.com/openApi/cfIpTop20
https://www.wetest.vip/page/cloudflare/cname.html
~~~

> 池子按「来源交错」排序后再轮询入口，避免排在后面的源永远轮不到。
>
> 优选域名会先做 **DoH 分级反查**（默认 `on`）：
>
> | 反查结果 | 处理 | 原因 |
> | :--- | :--- | :--- |
> | 解析到 Cloudflare 段 | 保留 | 确认是 CF 前端 |
> | 解析到但不在 CF 段 | **剔除** | 源站根本不在 Cloudflare（实测抓到挂在谷歌云 `34.41.x` 的假优选域名），当入口必然连不通 |
> | `NOERROR` 但没有 A 记录 | **保留并标记** | 万网/DNSPod 的**分线路解析**：境外视图本来就是空的，国内用户能解析。`bestcf.top` 属于这一类 |
> | `SERVFAIL` / `NXDOMAIN` | **剔除** | 域名真死（注册失效或权威 NS 挂了） |
>
> 关键点一：单个 DoH 服务会误杀，`dns.google` 对 `bestcf.top` 返回空记录，而 `cloudflare-dns.com` 能给出 `172.65.x` —— 所以**默认依次问两个服务**。
> 关键点二：**只有 DoH 全部连不上时才退回系统解析**，否则本地/运营商 DNS 污染会把已死的域名"救活"成看起来可用的 CF IP。
>
> 每个域名的判定结论写在 `edge_pool.txt` 第 4 列（`CF确认(x)` / `境外视图无A记录(分线路解析)` / `非CF段(x)` / `NXDOMAIN` …），第 5 列是该入口的来源域名，排查时直接看这个文件（列序：`入口 / 运营商 / 备注 / DNS判定 / 来源`）。
>
> `nodes-cu/cm/ct.txt` 与 `EDGE_ISP` 的内容 = 该运营商标签的优选 IP + 全部优选域名（域名与运营商无关）。若某个运营商标签一条都没有，则不生成对应文件。

### 中转池（CF 反代 IP）探活

除了 Cloudflare 官方段的优选 IP，还可以额外抓一批**中转/反代 IP**（不在 CF 段内，靠对端按 SNI 把 TLS 转交给 Cloudflare，例如 `8.210.x` 阿里云 HK、`150.230.x` Oracle 这类 VPS）。它们不能用上面的 Cloudflare 段校验（会被全部剔除），所以走独立通道，**只做存活探测**：

| 变量 | 默认 | 说明 |
| :--- | :--- | :--- |
| `EDGE_RELAY_MODE` | `off`（workflow 里设为 `file`） | `off` 关闭 / `file` 单独产出 `nodes-relay.txt` / `append` 按比例混进 `nodes.txt` |
| `EDGE_RELAY_APIS` | seeck + ipdb bestproxy | 用**分号**分隔（URL 内含逗号和 `{}`），条目写法同 `EDGE_POOL_APIS` |
| `EDGE_RELAY_SNI` | 空 | 填你的伪装域名：探活从「仅 TCP 可连」升级为「TLS 透传 + Cloudflare 证书校验」，能剔除自签假中转 |
| `EDGE_RELAY_LIMIT` | `120` | 探活候选**总**上限；最坏耗时 ≈ `120 × 4s / 8` ≈ 60s |
| `EDGE_RELAY_QUOTA` | `60` | **每个源**最多贡献多少条候选（`0`=不限）。不加配额时一家源就能吃满总上限，另一家分不到名额 |
| `EDGE_RELAY_TIMEOUT` | `4` | 单个 TCP/TLS 探测超时（秒） |
| `EDGE_RELAY_CONCURRENCY` | `8` | 探活并发。别调太高，容易被目标或中间设备判为滥用扫描 |
| `EDGE_RELAY_EVERY` | `4` | `append` 模式下每 N 条主池入口插 1 条中转（保证主池仍占多数） |
| `EDGE_RELAY_MIN_ALIVE` | `3` | 存活数低于此值则判定本次不可用，不产出中转订阅 |

产物：`nodes-relay.txt`（只含探活通过的入口）与 `relay_pool.txt`（调试表：`入口 / ALIVE-DEAD / 耗时 / 判定细节 / 备注 / 来源域名`，最后一列用来对比各源的存活率）。日志里还会直接打印一行 `各源存活率: seeck 60/60 | ipdb 52/60`。

内置中转源：

~~~text
https://proxy.seeck.cn/api/nodes?region=HK%2CJP%2CSG%2CTW&limit=40&format={ip}:{port}%23{name}%20{region}
https://ipdb.api.030101.xyz/?type=bestproxy&country=true
~~~

> ⚠️ **视角限制**：GitHub Actions 在美国，中转探活只能判定「这台机器现在活着、且确实是 TLS 透传」，**测不出它对你好不好用**（美国到阿里云 HK 是 150ms，深圳到它是 20ms）。延迟数字一律不参与排序。要按你的线路选最快的入口，把 `EDGE_FANOUT` 设为 2~3，交给客户端 `url-test` 自选。
>
> seeck 的 `/api/nodes` 不带 `region` 会返回 5000+ 条全量且响应很慢，所以默认 URL 里固定带 region 与 `limit`，代码侧还有 `EDGE_RELAY_LIMIT` 二次截断。
>
> 每个中转源都会记录 `候选数 / 响应字节 / 行数`；当某源候选数少于 5 条时会把**响应原文前 180 字符**打进日志——用于定位"源被限流"还是"返回了错误页"（实测 Actions 上 seeck 只贡献过 1 条，而本地同一 URL 返回 180 条）。

> 再次强调：`vpngate.py` **不需要**配置 `EDT_UUID` 和 `EDT_DOMAIN`，这两个参数属于 edgetunnel 本身。

---

## 五、常见问题

### 只有几个节点能连
入口优选域名大部分被墙。用 bestcf 重新测速，把 `EDGE_HOSTS` 换成实测能通的域名。

### 全部 -1
检查：edgetunnel 是否部署好、域名是否解析到 Cloudflare、UUID 是否填对、传输协议是否对得上。

### 30 分钟没更新
到 Actions 页看最近一次运行是否成功、cron 是否还在。

### 检测 Worker 报错
确认 Worker 部署成功、域名填对（workflow 里的 `CHECK_WORKER`），浏览器直接访问 `https://你的Worker/check?sstp=...` 看是否返回 JSON。

### 不知道 UUID 和节点域名在哪里看
登录 edgetunnel 后台（`https://你的域名/admin`），在后台首页就能看到。

---

*流水线：GitHub Actions（每 30 分钟 cron） → vpngate.py → 检测 Worker → GitHub Pages*

---

## 引用与致谢

本项目的实现离不开以下开源项目和服务的支持，在此表示衷心的感谢：

| 项目 | 用途 | 链接 |
| :--- | :--- | :--- |
| **cmliu/edgetunnel** | VLESS 代理 + 链式代理，节点最终通过它使用 | https://github.com/cmliu/edgetunnel |
| **lsh8848/cm-Workers-CheckSocks5** | 检测 Worker：验证 SSTP 节点可用性并读取出口 IP | https://github.com/lsh8848/cm-Workers-CheckSocks5 |
| **fdciabdul/Vpngate-Scraper-API** | VPN Gate 节点数据的 GitHub 镜像（官方源失效时回退） | https://github.com/fdciabdul/Vpngate-Scraper-API |
| **VPN Gate** | SSTP 节点数据源 | https://www.vpngate.net/ |
| **Star History** | 提供项目热度曲线图生成服务 | https://star-history.com/ |

---

## 项目热度

[![Star History Chart](https://api.star-history.com/svg?repos=hezhanleiok/gate&type=Date)](https://star-history.com/#hezhanleiok/gate&Date)

---

**特别感谢**：
感谢所有为开源社区做出贡献的开发者们！没有你们的无私奉献，就没有这个项目的诞生。也感谢每一位使用、测试和反馈问题的用户，是你们的支持让这个项目不断完善。

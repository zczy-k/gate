#!/usr/bin/env python3
"""
VPN Gate SSTP 节点检测流水线 (精简版)
=====================================
流程:
  1. 获取 VPN Gate 原始节点
  2. 只保留带 TCP 入口的 SSTP 节点
  3. 去重
  4. 预筛: 国家白名单 (默认 日韩港台新 + 美国) + VPN Gate 自带质量数据 (带宽 / 在线时长 / 有效带宽)
  5. 并发调用检测 Worker
  6. 排序: 先按地区 (离大陆近的优先, 美国排最后), 同地区内按检测延迟升序, 只输出前 N 个
  7. 获取并合并优选域名与优选 API (Cloudflare 入口)
  8. 生成 public/data.json + public/index.html + public/nodes.txt + public/nodes-residential.txt
"""

import base64
import csv
import io
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import quote

import requests

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------
REPO_DIR = os.path.dirname(os.path.abspath(__file__))

VPNGATE_API = os.environ.get("VPNGATE_API", "http://www.vpngate.net/api/iphone/")
VPNGATE_MIRROR = os.environ.get(
    "VPNGATE_MIRROR",
    "https://raw.githubusercontent.com/fdciabdul/Vpngate-Scraper-API/main/json/data.json",
)
WORKER_CHECK_URL = os.environ.get("CHECK_WORKER", "https://你的域名/check?sstp=vpn:vpn@")
CONCURRENCY = max(1, int(os.environ.get("CHECK_CONCURRENCY", "32")))
# 经 Worker 做一次 SSTP 握手通常要 2~7 秒, 超时不能设太短, 否则会误杀好节点
CHECK_TIMEOUT = float(os.environ.get("CHECK_TIMEOUT", "20"))
MAX_CHECK_NODES = int(os.environ.get("MAX_CHECK_NODES", "0"))
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "60"))
PUBLIC_DIR = os.environ.get("PUBLIC_DIR", os.path.join(REPO_DIR, "public"))
TEMPLATE_HTML = os.path.join(REPO_DIR, "web", "index.html")

# ---- 质量筛选 (A1) ----
# 节点标称带宽下限 (Mbps)。VPN Gate 的 Speed 字段单位是 bps
MIN_SPEED_MBPS = float(os.environ.get("MIN_SPEED_MBPS", "50"))
# 节点连续在线时长下限 (小时)。刚上线的节点容易掉线。VPN Gate 的 Uptime 字段单位是毫秒
MIN_UPTIME_HOURS = float(os.environ.get("MIN_UPTIME_HOURS", "1"))
# 有效带宽下限 (Mbps) = 标称带宽 / (在线人数 + 1)。低于它说明节点太挤
MIN_EFF_MBPS = float(os.environ.get("MIN_EFF_MBPS", "2"))
# 国家白名单 + 优先级 (越靠前越优先, 按离中国大陆的距离排)。留空 = 不限制国家
COUNTRY_PRIORITY = [
    c.strip().upper()
    for c in os.environ.get("COUNTRY_PRIORITY", "HK,TW,JP,KR,SG,US").split(",")
    if c.strip()
]
# 在线人数上限, 超过的节点带宽被分得太薄。0 = 不限制
MAX_SESSIONS = int(os.environ.get("MAX_SESSIONS", "0"))
# nodes.txt 最多输出多少个节点 (宽进严出)。0 = 不限制
MAX_OUTPUT_NODES = int(os.environ.get("MAX_OUTPUT_NODES", "30"))
# nodes-residential.txt 最多输出多少个住宅节点。0 = 不限制
MAX_RESIDENTIAL_NODES = int(os.environ.get("MAX_RESIDENTIAL_NODES", "15"))

DATA_CENTER_ORG_KEYWORDS = [
    "GOOGLE", "AMAZON", "AWS", "MICROSOFT", "OVH", "HETZNER", "DIGITALOCEAN",
    "AKAMAI", "CLOUDFLARE", "FASTLY", "RACKSPACE", "EQUINIX", "LINODE", "VULTR",
    "HURRICANE", "TENCENT", "ALIBABA", "ALIYUN", "LEASWEB",
]
RESIDENTIAL_ORG_KEYWORDS = [
    "NTT EAST", "NTT WEST", "NTT COMMUNICATIONS", "NTT BROADBAND", "KDDI", "DOCOMO",
    "SOFTBANK", "AU COMMUNICATIONS", "J:COM", "JCOM", "OCN", "BIGLOBE",
    "IIJ", "SEIKO", "CLEVER-NET", "AT&T", "COMCAST", "XFINITY", "VERIZON",
    "TELUS", "ROGERS", "BELL CANADA", "VODAFONE", "ORANGE", "DEUTSCHE TELEKOM",
    "BREEZE", "TIM S.P.A", "LIBERO", "FASTWEB", "FREE FRANCE", "BT OPEN",
]

COUNTRY_ZH = {
    "JP": "日本", "KR": "韩国", "US": "美国", "CA": "加拿大", "RU": "俄罗斯",
    "RO": "罗马尼亚", "TH": "泰国", "VN": "越南", "DE": "德国", "FR": "法国",
    "GB": "英国", "UK": "英国", "SG": "新加坡", "TW": "台湾", "HK": "香港",
    "CN": "中国", "AU": "澳大利亚", "NL": "荷兰", "SE": "瑞典", "CH": "瑞士",
    "IT": "意大利", "ES": "西班牙", "PL": "波兰", "IN": "印度", "BR": "巴西",
    "MX": "墨西哥", "ID": "印度尼西亚", "MY": "马来西亚", "PH": "菲律宾",
    "TR": "土耳其", "UA": "乌克兰", "CZ": "捷克", "GR": "希腊", "PT": "葡萄牙",
    "FI": "芬兰", "NO": "挪威", "DK": "丹麦", "IE": "爱尔兰", "BE": "比利时",
    "AT": "奥地利", "HU": "匈牙利", "AR": "阿根廷", "CL": "智利", "CO": "哥伦比亚",
    "NZ": "新西兰", "ZA": "南非", "IL": "以色列", "AE": "阿联酋", "SA": "沙特",
    "EG": "埃及", "HR": "克罗地亚", "BY": "白俄罗斯", "GD": "格林纳达",
    "LV": "拉脱维亚", "EE": "爱沙尼亚", "LT": "立陶宛", "SK": "斯洛伐克",
    "SI": "斯洛文尼亚", "BG": "保加利亚", "RS": "塞尔维亚", "GE": "格鲁吉亚",
    "MD": "摩尔多瓦", "AM": "亚美尼亚", "KZ": "哈萨克斯坦", "UZ": "乌兹别克斯坦",
    "MN": "蒙古", "NP": "尼泊尔", "LK": "斯里兰卡", "MM": "缅甸",
}

# 静态优选域名池
EDGE_HOSTS = [
    h.strip()
    for h in os.environ.get(
        "EDGE_HOSTS",
        "saas.sin.fan:443,cdn.204910.best:443,www.mfyx.cn:443,p.etime.vip:443,cdn.ctn32.us.kg:443,cf.877774.xyz:443,spring.io:443,"
        "cf.nyanya.moe:443,www.sloomb.com:443,op.chinwa.eu.cc:443,www.leics.police.uk:443,securecircle.com:443,www.shopify.com:443,"
        "www.carousell.sg:443,www.dbs.com.sg:443,openai.com:443,linear.app:443,www.bilibili.com:443,uspto.gov:443,www.vmware.com:443",
    ).split(",")
    if h.strip()
]

# 优选 API 地址配置 (支持逗号分隔多个，可配置在 GitHub Action Secret / Env 中)
# 顺序即优先级: 联通 (主用) → 电信 → 移动。API 拿到的 IP 会排在写死的 EDGE_HOSTS 前面
OPTIMAL_API = os.environ.get(
    "OPTIMAL_API",
    "https://cf.090227.xyz/cu?ips=10&port=443,"
    "https://cf.090227.xyz/ct?ips=6&port=443,"
    "https://cf.090227.xyz/cmcc?ips=8&port=443"
)

NODES_URL = os.environ.get("NODES_URL", "https://zczy-k.github.io/gate/nodes.txt")

# ---------------------------------------------------------------------------
# 日志
# ---------------------------------------------------------------------------
_section = None

def log(section, msg=""):
    global _section
    if section != _section:
        print(f"========== {section} ==========")
        _section = section
    if msg:
        print(msg, flush=True)

def die(msg):
    log("FATAL", f"[失败] {msg}")
    sys.exit(1)

# ---------------------------------------------------------------------------
# 数据抓取
# ---------------------------------------------------------------------------
def fetch_vpngate():
    try:
        log("VPN GATE", f"获取官方 API: {VPNGATE_API}")
        resp = requests.get(VPNGATE_API, timeout=HTTP_TIMEOUT, headers={"User-Agent": "Mozilla/5.0 (compatible; gate-checker)"})
        resp.raise_for_status()
        rows = parse_csv(resp.text)
        if rows:
            log("VPN GATE", f"主源(官方 API) 获取到 {len(rows)} 个原始节点")
            return rows, "vpngate.net/api/iphone"
        raise RuntimeError("官方 API 返回 0 行数据")
    except Exception as exc:
        log("VPN GATE", f"官方 API 获取失败: {exc}")

    try:
        log("VPN GATE", f"回退镜像: {VPNGATE_MIRROR}")
        resp = requests.get(VPNGATE_MIRROR, timeout=HTTP_TIMEOUT, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        rows = parse_mirror_json(resp.json())
        if rows:
            log("VPN GATE", f"回退源(镜像) 获取到 {len(rows)} 个原始节点")
            return rows, "github-mirror"
    except Exception as exc:
        log("VPN GATE", f"回退镜像也失败: {exc}")
    die("VPN Gate 官方 API 与回退镜像均不可用, 数据源完全失败")

def parse_csv(text):
    lines = [ln for ln in text.splitlines() if ln.strip()]
    header_idx = None
    for i, ln in enumerate(lines):
        if ln.lstrip("#").startswith("HostName"):
            header_idx = i
            break
    if header_idx is None:
        raise RuntimeError("找不到 CSV 表头行 (HostName)")

    header = lines[header_idx].lstrip("#").split(",")
    data_lines = lines[header_idx + 1:]
    idx = {}
    for col in ("hostname", "ip", "score", "ping", "speed", "countrylong", "countryshort",
                "numvpnsessions", "uptime", "openvpn_configdata_base64"):
        for i, h in enumerate(header):
            if h.strip().lstrip("*").lower() == col:
                idx[col] = i
                break
    if "openvpn_configdata_base64" not in idx:
        for i, h in enumerate(header):
            if "base64" in h.lower():
                idx["openvpn_configdata_base64"] = i
                break
    pos = {
        "hostname": idx.get("hostname", 0),
        "ip": idx.get("ip", 1),
        "countrylong": idx.get("countrylong", 5),
        "countryshort": idx.get("countryshort", 6),
        "score": idx.get("score", 2),
        "ping": idx.get("ping", 3),
        "speed": idx.get("speed", 4),
        "numvpnsessions": idx.get("numvpnsessions", 7),
        "uptime": idx.get("uptime", 8),
        "openvpn_configdata_base64": idx.get("openvpn_configdata_base64", len(header) - 1),
    }

    rows = []
    for ln in data_lines:
        fields = next(csv.reader(io.StringIO(ln)))
        if len(fields) < 7: continue
        host = fields[pos["hostname"]].strip()
        ip = fields[pos["ip"]].strip()
        if not host or not ip: continue
        rows.append({
            "host": host,
            "ip": ip,
            "country_long": fields[pos["countrylong"]].strip(),
            "country_short": fields[pos["countryshort"]].strip(),
            "score": _to_num(_field(fields, pos["score"])),
            "ping": _to_num(_field(fields, pos["ping"])),
            "speed": _to_num(_field(fields, pos["speed"])),
            "sessions": _to_num(_field(fields, pos["numvpnsessions"])),
            "uptime": _to_num(_field(fields, pos["uptime"])),
            "config_b64": fields[pos["openvpn_configdata_base64"]].strip(),
        })
    return rows

def _field(fields, i):
    return fields[i] if 0 <= i < len(fields) else ""

def _to_num(v):
    """把 CSV / JSON 里的数字字段转成 float, 缺失或非法返回 None (表示未知, 不参与过滤)"""
    try:
        v = str(v).strip()
        if v == "" or v == "-":
            return None
        return float(v)
    except Exception:
        return None

def parse_mirror_json(data):
    servers = []
    items = data if isinstance(data, list) else [data]
    for item in items:
        if isinstance(item, dict) and isinstance(item.get("servers"), list):
            servers.extend(item["servers"])
        elif isinstance(item, dict):
            servers.append(item)
    rows = []
    for s in servers:
        host = str(s.get("hostname") or s.get("host") or "").strip()
        ip = str(s.get("ip") or "").strip()
        if not host or not ip: continue
        rows.append({
            "host": host,
            "ip": ip,
            "country_long": str(s.get("countrylong") or s.get("country_long") or s.get("country") or "").strip(),
            "country_short": str(s.get("countryshort") or s.get("country_short") or "").strip(),
            "score": _to_num(s.get("score")),
            "ping": _to_num(s.get("ping")),
            "speed": _to_num(s.get("speed")),
            "sessions": _to_num(s.get("numvpnsessions") or s.get("sessions")),
            "uptime": _to_num(s.get("uptime")),
            "config_b64": str(s.get("openvpn_configdata_base64") or s.get("config_b64") or "").strip(),
        })
    return rows

# ---------------------------------------------------------------------------
# 优选 API 获取与解析
# ---------------------------------------------------------------------------
def fetch_optimal_api_endpoints(api_urls_str):
    """
    拉取优选 API，解析如: 172.66.1.179:443#CF 电信优选 格式的内容
    提取出纯 'IP:端口' (例如 172.66.1.179:443)
    """
    if not api_urls_str.strip():
        return []
    
    endpoints = []
    urls = [u.strip() for u in api_urls_str.split(",") if u.strip()]
    for url in urls:
        try:
            log("OPTIMAL API", f"请求优选 API: {url}")
            resp = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code == 200:
                lines = resp.text.strip().splitlines()
                count = 0
                for line in lines:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    # 剥离 # 后的备注，提取 IP:端口
                    endpoint = line.split("#")[0].strip()
                    if endpoint:
                        # 补充缺失端口默认 443
                        if ":" not in endpoint:
                            endpoint = f"{endpoint}:443"
                        endpoints.append(endpoint)
                        count += 1
                log("OPTIMAL API", f"成功从 API 获取到 {count} 个优选端点")
            else:
                log("OPTIMAL API", f"API 返回状态码异常: {resp.status_code}")
        except Exception as e:
            log("OPTIMAL API", f"拉取优选 API 失败 ({url}): {e}")
    return endpoints

def get_combined_edge_hosts():
    """
    合并优选 API 端点与静态优选域名
    顺序: API (联通 → 电信 → 移动) 在前, 写死的 EDGE_HOSTS / HOSTS_ENTRY 在后
    """
    # 1. 优先读取 HOSTS_ENTRY 或 EDGE_HOSTS
    _entry = os.environ.get("HOSTS_ENTRY", "").strip()
    static_hosts = [e.strip() for e in _entry.split(",") if e.strip()] if _entry else EDGE_HOSTS

    # 2. 拉取优选 API 端点
    api_hosts = fetch_optimal_api_endpoints(OPTIMAL_API)

    # 3. 合并与去重: API 在前, 静态域名在后 (保留各自原始顺序)
    combined = []
    seen = set()
    for h in api_hosts + static_hosts:
        if h and h not in seen:
            seen.add(h)
            combined.append(h)

    log("EDGE POOL", f"汇总入口地址池: API 获取 {len(api_hosts)} 个 (排在前面), 静态域名 {len(static_hosts)} 个 (排在后面), 合并有效入口共 {len(combined)} 个")
    return combined if combined else EDGE_HOSTS

# ---------------------------------------------------------------------------
# 筛选 SSTP 节点
# ---------------------------------------------------------------------------
_PROTO_TCP_RE = re.compile(r"^proto\s+(tcp|tcp4|tcp6)\b", re.M)
_REMOTE_RE = re.compile(r"^remote\s+\S+\s+(\d+)", re.M)

def to_sstp_nodes(rows):
    nodes = []
    for r in rows:
        cfg = ""
        if r["config_b64"]:
            try:
                cfg = base64.b64decode(r["config_b64"], validate=False).decode("utf-8", "replace")
            except Exception:
                cfg = ""
        if not _PROTO_TCP_RE.search(cfg): continue
        m = _REMOTE_RE.search(cfg)
        if not m: continue
        port = int(m.group(1))
        if not (1 <= port <= 65535): continue
        host = r["host"]
        if not host.endswith(".opengw.net"):
            host = f"{host}.opengw.net"
        speed = r.get("speed")
        sessions = r.get("sessions")
        nodes.append({
            "host": host, "port": port, "ip": r["ip"],
            "country": r["country_long"], "country_code": r["country_short"],
            "speed_mbps": round(speed / 1e6, 1) if speed is not None else None,
            "sessions": int(sessions) if sessions is not None else None,
            "uptime_h": round(r["uptime"] / 3.6e6, 1) if r.get("uptime") is not None else None,
            "score": r.get("score"),
            "eff_mbps": effective_mbps(speed, sessions),
        })
    return nodes

def effective_mbps(speed_bps, sessions):
    """有效带宽 = 标称带宽 / (在线人数 + 1), 即你连上去大概能分到的带宽 (Mbps)"""
    if speed_bps is None:
        return None
    return round(speed_bps / 1e6 / ((sessions or 0) + 1), 2)

def quality_filter(nodes):
    """
    检测前的预筛: 用 VPN Gate 自带的数据先淘汰明显慢/不稳的节点,
    字段缺失 (None, 比如走了回退镜像) 的不做该项过滤。
    """
    kept, drop_country, drop_speed, drop_uptime, drop_eff, drop_sessions = [], 0, 0, 0, 0, 0
    for n in nodes:
        if COUNTRY_PRIORITY and str(n.get("country_code") or "").upper() not in COUNTRY_PRIORITY:
            drop_country += 1
            continue
        if n["speed_mbps"] is not None and n["speed_mbps"] < MIN_SPEED_MBPS:
            drop_speed += 1
            continue
        if n["uptime_h"] is not None and n["uptime_h"] < MIN_UPTIME_HOURS:
            drop_uptime += 1
            continue
        if n["eff_mbps"] is not None and n["eff_mbps"] < MIN_EFF_MBPS:
            drop_eff += 1
            continue
        if MAX_SESSIONS > 0 and n["sessions"] is not None and n["sessions"] > MAX_SESSIONS:
            drop_sessions += 1
            continue
        kept.append(n)
    log("QUALITY", f"预筛: 不在白名单 {','.join(COUNTRY_PRIORITY) or '-'} 淘汰 {drop_country}, "
        f"带宽 < {MIN_SPEED_MBPS:g} Mbps 淘汰 {drop_speed}, 在线 < {MIN_UPTIME_HOURS:g} 小时淘汰 {drop_uptime}, "
        f"有效带宽 < {MIN_EFF_MBPS:g} Mbps 淘汰 {drop_eff}"
        + (f", 在线人数 > {MAX_SESSIONS} 淘汰 {drop_sessions}" if MAX_SESSIONS > 0 else "")
        + f", 保留 {len(kept)}")
    return kept

def region_rank(n):
    code = str(n.get("country_code") or "").upper()
    return COUNTRY_PRIORITY.index(code) if code in COUNTRY_PRIORITY else len(COUNTRY_PRIORITY)

def bandwidth_key(n):
    """检测前用: 有效带宽降序 (去重时保留带宽最好的那条)"""
    eff = n.get("eff_mbps")
    return (eff is None, -(eff or 0), n.get("host") or "")

def rank_key(n):
    """
    最终排序: 地区优先级 (港台日韩新在前, 美国在后)
            → 检测延迟升序 (每个连接都要做 SSTP 握手, 延迟比带宽更影响打开网页的速度)
            → 有效带宽降序
    """
    lat = n.get("latency_ms")
    eff = n.get("eff_mbps")
    return (region_rank(n), lat is None, lat or 0, -(eff or 0), n.get("host") or "")

def dedupe(nodes):
    seen = set()
    out = []
    for n in nodes:
        key = (n["host"].lower(), n["port"], "sstp")
        if key in seen: continue
        seen.add(key)
        out.append(n)
    return out

# ---------------------------------------------------------------------------
# 检测 Worker
# ---------------------------------------------------------------------------
def classify_network(host, exit_org, is_datacenter=None):
    if is_datacenter is True: return "datacenter"
    if is_datacenter is False: return "residential"
    org = (exit_org or "").upper()
    if org:
        if any(k in org for k in DATA_CENTER_ORG_KEYWORDS): return "datacenter"
        if any(k in org for k in RESIDENTIAL_ORG_KEYWORDS): return "residential"
    h = host.lower()
    if h.startswith("public-vpn"): return "datacenter"
    if re.match(r"^vpn\d{5,}", h) or re.match(r"^vpnv\d+", h): return "residential"
    return "unknown"

def check_one(node, session):
    url = WORKER_CHECK_URL + quote(f"{node['host']}:{node['port']}", safe="")
    out = dict(node)
    out["protocol"] = "sstp"
    out["link"] = f"sstp://vpn:vpn@{node['host']}:{node['port']}"
    out["status"] = "failed"
    out["checked_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    out["exit"] = None
    out["residential"] = "unknown"
    try:
        r = session.get(url, timeout=CHECK_TIMEOUT, headers={"User-Agent": "Mozilla/5.0 (gate-checker)"})
        if r.status_code != 200:
            out["error"] = f"HTTP {r.status_code}"
            out["worker_error"] = True
            return out
        j = r.json()
        ok = bool(j.get("success"))
        out["success"] = ok
        out["status"] = "success" if ok else "failed"
        out["latency_ms"] = j.get("responseTime")
        out["colo"] = j.get("colo")
        out["error"] = (None if ok else (j.get("error") or j.get("message") or "check failed"))
        exit_info = j.get("exit") or {}
        if exit_info:
            asn = exit_info.get("asn") or {}
            org = asn.get("org") or asn.get("name") or ""
            out["exit"] = {
                "ip": exit_info.get("ip"),
                "country": exit_info.get("country"),
                "country_code": exit_info.get("country_code"),
                "city": exit_info.get("city"),
                "continent": exit_info.get("continent"),
                "asn": asn.get("asn"),
                "org": org,
                "type": asn.get("type"),
                "is_datacenter": exit_info.get("is_datacenter"),
            }
            out["residential"] = classify_network(out["host"], org, exit_info.get("is_datacenter"))
        else:
            out["residential"] = classify_network(out["host"], None, None)
        return out
    except Exception as exc:
        out["error"] = f"{type(exc).__name__}: {exc}"
        out["worker_error"] = True
        return out

def check_all(nodes, session):
    results = []
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        futures = [pool.submit(check_one, n, session) for n in nodes]
        for fut in as_completed(futures):
            results.append(fut.result())
    return results

# ---------------------------------------------------------------------------
# 生成数据
# ---------------------------------------------------------------------------
def build_outputs(results, raw_count, sstp_count, source):
    available = [r for r in results if r.get("success")]
    countries = {}
    for n in available:
        c = n["country"] or "未知"
        countries.setdefault(c, {"code": n["country_code"] or "?", "nodes": []})["nodes"].append(n)

    stats = {
        "raw_nodes": raw_count,
        "sstp_nodes": sstp_count,
        "checked": len(results),
        "success": len(available),
        "failed": len(results) - len(available),
        "countries": len(countries),
        "residential_est": sum(1 for n in available if n["residential"] == "residential"),
        "datacenter_est": sum(1 for n in available if n["residential"] == "datacenter"),
    }
    by_country = {}
    for name, grp in countries.items():
        grp["count"] = len(grp["nodes"])
        grp["residential"] = sum(1 for n in grp["nodes"] if n["residential"] == "residential")
        grp["datacenter"] = sum(1 for n in grp["nodes"] if n["residential"] == "datacenter")
        grp["nodes"].sort(key=rank_key)
        by_country[name] = grp

    data = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "source": source,
        "worker": WORKER_CHECK_URL,
        "stats": stats,
        "countries": by_country,
        "available": available,
    }
    return data

def _ranked_available(data):
    """全部可用节点按 地区 → 延迟 → 有效带宽 排序"""
    return sorted(data["available"], key=rank_key)

def _node_line(entry, n, rank):
    code = str(n.get("country_code") or "?").upper()
    zh = COUNTRY_ZH.get(code) or (code if code != "?" else (n.get("country") or "未知"))
    tag = "住宅" if n.get("residential") == "residential" else "机房"
    eff = n.get("eff_mbps")
    lat = n.get("latency_ms")
    bw = f"-{eff:g}M" if eff is not None else ""
    ms = f"-{int(lat)}ms" if lat is not None else ""
    return f"{entry}#{rank:02d}-{zh}-{tag}{ms}{bw}$sstp://vpn:vpn@{n['host']}:{n['port']}"

def build_nodes_text(data, edge_hosts, residential_only=False, limit=0):
    """
    生成纯节点行版本 (无注释): 每行 = 入口地址#序号-国家-住宅/机房-有效带宽$sstp://...
    节点按 地区 → 延迟 → 有效带宽 排列, 序号越小越优先。
    """
    nodes = _ranked_available(data)
    if residential_only:
        nodes = [n for n in nodes if n.get("residential") == "residential"]
    if limit > 0:
        nodes = nodes[:limit]
    edge = edge_hosts
    lines = [_node_line(edge[i % len(edge)], n, i + 1) for i, n in enumerate(nodes)]
    return "\n".join(lines) + "\n"

def write_outputs(data, edge_hosts):
    os.makedirs(PUBLIC_DIR, exist_ok=True)
    data_path = os.path.join(PUBLIC_DIR, "data.json")
    with open(data_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)

    html_path = os.path.join(PUBLIC_DIR, "index.html")
    if os.path.exists(TEMPLATE_HTML):
        with open(TEMPLATE_HTML, "r", encoding="utf-8") as f:
            html = f.read()
    else:
        html = (
            "<html><head><meta charset='utf-8'><title>VPN Gate SSTP 节点</title></head>"
            "<body><h1>VPN Gate SSTP 节点</h1><pre id='out'></pre></body>"
            "<script>fetch('data.json').then(r=>r.json()).then(d=>out.textContent=JSON.stringify(d.stats)).catch(e=>out.textContent='加载失败:'+e)</script></html>"
        )
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

    nodes_path = os.path.join(PUBLIC_DIR, "nodes.txt")
    with open(nodes_path, "w", encoding="utf-8") as f:
        f.write(build_nodes_text(data, edge_hosts, limit=MAX_OUTPUT_NODES))

    # 只在需要住宅 IP 时使用 (注册账号、过风控等)
    res_path = os.path.join(PUBLIC_DIR, "nodes-residential.txt")
    with open(res_path, "w", encoding="utf-8") as f:
        f.write(build_nodes_text(data, edge_hosts, residential_only=True, limit=MAX_RESIDENTIAL_NODES))

    return data_path, html_path, nodes_path

# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main():
    session = requests.Session()
    rows, source = fetch_vpngate()
    raw_count = len(rows)
    if raw_count == 0:
        die("VPN Gate 返回 0 个原始节点 (数据源异常, 不允许生成空结果)")

    sstp_nodes = to_sstp_nodes(rows)
    sstp_count = len(sstp_nodes)
    if sstp_count == 0:
        die(f"从 {raw_count} 个原始节点中没有解析出任何 SSTP(TCP) 节点 — 数据格式可能已变化, 需要人工适配")
    # 先按有效带宽排序再去重, 同一 host 保留质量最好的那条
    sstp_nodes.sort(key=bandwidth_key)
    uniq = dedupe(sstp_nodes)
    deduped_count = len(uniq)
    uniq = quality_filter(uniq)
    if not uniq:
        die(f"预筛后没有节点 (带宽下限 {MIN_SPEED_MBPS:g} Mbps / 在线下限 {MIN_UPTIME_HOURS:g} 小时 过严?), 可调低 MIN_SPEED_MBPS / MIN_UPTIME_HOURS")

    if MAX_CHECK_NODES > 0:
        uniq = uniq[:MAX_CHECK_NODES]

    log("VPN GATE", f"获取原始节点: {raw_count}")
    log("VPN GATE", f"SSTP 节点: {sstp_count}")
    log("VPN GATE", f"去重后: {deduped_count}")
    log("VPN GATE", f"预筛后提交检测: {len(uniq)}")

    log("CLOUDFLARE WORKER", f"提交检测: {len(uniq)} (并发 {CONCURRENCY}, 单请求超时 {CHECK_TIMEOUT}s)")
    t0 = time.time()
    results = check_all(uniq, session)
    elapsed = time.time() - t0

    success = [r for r in results if r.get("success")]
    failed = [r for r in results if not r.get("success")]
    worker_errors = [r for r in failed if r.get("worker_error")]

    log("CLOUDFLARE WORKER", f"检测成功: {len(success)}")
    log("CLOUDFLARE WORKER", f"检测失败: {len(failed)}" + (f" (其中 Worker 异常 {len(worker_errors)})" if worker_errors else ""))
    log("CLOUDFLARE WORKER", f"耗时: {elapsed:.1f}s")

    if uniq and not success and len(worker_errors) == len(uniq):
        die("Worker 全部请求异常, 检测服务不可用 — 本次运行判定失败 (不生成空结果)")

    data = build_outputs(results, raw_count, sstp_count, source)
    log("RESULT", f"可用节点: {len(success)}")
    log("RESULT", f"国家数量: {data['stats']['countries']}")
    top = sorted(success, key=rank_key)[:5]
    for i, n in enumerate(top, 1):
        log("RESULT", f"TOP{i}: {n['host']}:{n['port']} {n.get('country_code')} 有效带宽 {n.get('eff_mbps')} Mbps "
            f"(标称 {n.get('speed_mbps')} Mbps / {n.get('sessions')} 人在线) 检测延迟 {n.get('latency_ms')} ms")

    # 动态拉取 API 并与静态优选域名混合
    edge_hosts = get_combined_edge_hosts()

    data_path, html_path, nodes_path = write_outputs(data, edge_hosts)
    log("WEBSITE", f"生成 {os.path.relpath(data_path, REPO_DIR)}")
    log("WEBSITE", f"生成 {os.path.relpath(html_path, REPO_DIR)}")
    log("WEBSITE", f"生成 {os.path.relpath(nodes_path, REPO_DIR)} (最多 {MAX_OUTPUT_NODES} 个) 和 nodes-residential.txt (最多 {MAX_RESIDENTIAL_NODES} 个)")
    log("USAGE", f"自动轮换: 把 {NODES_URL} 填入 edgetunnel 后台「自定义优选IP」框 (一次配置, 之后自动更新)")
    log("WEBSITE", "完成 (GitHub Pages 部署由 workflow 执行)")

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as exc:
        die(f"程序异常: {type(exc).__name__}: {exc}")

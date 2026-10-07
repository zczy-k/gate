#!/usr/bin/env python3
"""
VPN Gate SSTP 节点检测流水线 (精简版)
=====================================
流程:
  1. 获取 VPN Gate 原始节点
  2. 只保留带 TCP 入口的 SSTP 节点
  3. 去重
  4. 并发调用检测 Worker
  5. 生成 public/data.json + public/index.html + public/nodes.txt
"""

import base64
import csv
import io
import ipaddress
import json
import os
import re
import socket
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
CHECK_TIMEOUT = float(os.environ.get("CHECK_TIMEOUT", "90"))
MAX_CHECK_NODES = int(os.environ.get("MAX_CHECK_NODES", "0"))
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "60"))
PUBLIC_DIR = os.environ.get("PUBLIC_DIR", os.path.join(REPO_DIR, "public"))
TEMPLATE_HTML = os.path.join(REPO_DIR, "web", "index.html")

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
    for col in ("hostname", "ip", "countrylong", "countryshort", "openvpn_configdata_base64"):
        for i, h in enumerate(header):
            if h.strip().lstrip("*").lower() == col:
                idx[col] = i
                break
    if "openvpn_configdata_base64" not in idx:
        for i, h in enumerate(header):
            if "base64" in h.lower():
                idx["openvpn_configdata_base64"] = i
                break
    pos = {"hostname": idx.get("hostname", 0), "ip": idx.get("ip", 1), "countrylong": idx.get("countrylong", 5), "countryshort": idx.get("countryshort", 6), "openvpn_configdata_base64": idx.get("openvpn_configdata_base64", len(header) - 1)}

    rows = []
    for ln in data_lines:
        fields = next(csv.reader(io.StringIO(ln)))
        if len(fields) < 7: continue
        host = fields[pos["hostname"]].strip()
        ip = fields[pos["ip"]].strip()
        if not host or not ip: continue
        rows.append({"host": host, "ip": ip, "country_long": fields[pos["countrylong"]].strip(), "country_short": fields[pos["countryshort"]].strip(), "config_b64": fields[pos["openvpn_configdata_base64"]].strip()})
    return rows

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
        rows.append({"host": host, "ip": ip, "country_long": str(s.get("countrylong") or s.get("country_long") or s.get("country") or "").strip(), "country_short": str(s.get("countryshort") or s.get("country_short") or "").strip(), "config_b64": str(s.get("openvpn_configdata_base64") or s.get("config_b64") or "").strip()})
    return rows

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
        nodes.append({"host": host, "port": port, "ip": r["ip"], "country": r["country_long"], "country_code": r["country_short"]})
    return nodes

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
            out["exit"] = {"ip": exit_info.get("ip"), "country": exit_info.get("country"), "country_code": exit_info.get("country_code"), "city": exit_info.get("city"), "continent": exit_info.get("continent"), "asn": asn.get("asn"), "org": org, "type": asn.get("type"), "is_datacenter": exit_info.get("is_datacenter")}
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

    stats = {"raw_nodes": raw_count, "sstp_nodes": sstp_count, "checked": len(results), "success": len(available), "failed": len(results) - len(available), "countries": len(countries), "residential_est": sum(1 for n in available if n["residential"] == "residential"), "datacenter_est": sum(1 for n in available if n["residential"] == "datacenter")}
    by_country = {}
    for name, grp in countries.items():
        grp["count"] = len(grp["nodes"])
        grp["residential"] = sum(1 for n in grp["nodes"] if n["residential"] == "residential")
        grp["datacenter"] = sum(1 for n in grp["nodes"] if n["residential"] == "datacenter")
        grp["nodes"].sort(key=lambda n: (n.get("latency_ms") is None, n.get("latency_ms") or 0, n["host"]))
        by_country[name] = grp

    data = {"generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"), "source": source, "worker": WORKER_CHECK_URL, "stats": stats, "countries": by_country, "available": available}
    return data

# ---------------------------------------------------------------------------
# 自动优选入口池 (动态)
# ---------------------------------------------------------------------------
# 入口来源格式:
#   text -> 每行 "IP" / "IP:port" / "IP#备注" / "域名:port#备注"
#   json -> 递归取 ip/domain/host/value 字段里的地址
#   html -> 用 pattern 正则从页面里抠地址 (优选 CNAME 域名表格)
# 环境变量 EDGE_POOL_APIS 可整体覆盖, 条目格式 "url" 或 "url|kind|备注"
EDGE_POOL_DEFAULT_SOURCES = [
    {"url": "https://addressesapi.090227.xyz/CloudFlareYes", "kind": "text"},
    {"url": "https://ipdb.api.030101.xyz/?type=bestcf&country=true", "kind": "text"},
    {"url": "https://raw.githubusercontent.com/cmliu/WorkerVless2sub/main/addressesapi.txt", "kind": "text"},
    # ---- 优选域名 (社区众包 CNAME 池, 由国内用户实测; 泛域名自动补前缀) ----
    {"url": "https://raw.githubusercontent.com/DustinWin/BestCF/bestcf/bestcf-domain.txt",
     "kind": "text", "label": "优选域名"},
    {"url": "https://vps789.com/openApi/cfIpTop20", "kind": "json", "label": "优选域名"},
    {"url": "https://www.wetest.vip/page/cloudflare/cname.html",
     "kind": "html", "label": "优选域名",
     "pattern": r'(?<=<td data-label="地址名称">)[^<]+'},
    # ---- 分运营商优选 IP (每 12 小时重建, 备注自带 CMCC/CUCC/CTCC, 让三网专用订阅稳定) ----
    {"url": "https://raw.githubusercontent.com/DustinWin/BestCF/bestcf/cmcc-ip.txt", "kind": "text"},
    {"url": "https://raw.githubusercontent.com/DustinWin/BestCF/bestcf/cucc-ip.txt", "kind": "text"},
    {"url": "https://raw.githubusercontent.com/DustinWin/BestCF/bestcf/ctcc-ip.txt", "kind": "text"},
]

_ENV_SOURCES = []
for _item in os.environ.get("EDGE_POOL_APIS", "").split(","):
    _item = _item.strip()
    if not _item:
        continue
    _parts = _item.split("|")
    _ENV_SOURCES.append({"url": _parts[0], "kind": _parts[1] if len(_parts) > 1 else "text",
                         "label": _parts[2] if len(_parts) > 2 else ""})
EDGE_POOL_APIS = _ENV_SOURCES or EDGE_POOL_DEFAULT_SOURCES

# Cloudflare 官方 IPv4 段, 用来剔除优选源里混进来的非 CF 脏 IP
# (例如 ipdb 的 type=bestproxy 返回的是 Oracle/阿里云 IP, 当入口必然连不通)
CF_V4_URL = os.environ.get("CF_V4_URL", "https://www.cloudflare.com/ips-v4")

# 域名归属校验用的 DoH 服务 (可换 https://cloudflare-dns.com/dns-query 等)
EDGE_DOH_URL = os.environ.get("EDGE_DOH_URL", "https://dns.google/resolve")

# 优选域名里有 *.example.com 这种泛域名, 客户端不能直接用, 补一个可用前缀
WILDCARD_PREFIX = os.environ.get("EDGE_WILDCARD_PREFIX", "bestcf").strip(".") or "bestcf"

_JSON_ADDR_KEYS = ("ip", "domain", "host", "value")
_DOMAIN_RE = re.compile(
    r"^[A-Za-z0-9](?:[A-Za-z0-9\-]{0,61}[A-Za-z0-9])?"
    r"(?:\.[A-Za-z0-9](?:[A-Za-z0-9\-]{0,61}[A-Za-z0-9])?)*\.[A-Za-z]{2,}$"
)
_ISP_PAT = re.compile(r"(?:^|[^A-Za-z])(CMCC|CUCC|CTCC|移动|联通|电信|CM|CU|CT)(?:[^A-Za-z]|$)", re.I)
_ISP_ALIAS = {"CMCC": "CM", "CUCC": "CU", "CTCC": "CT", "移动": "CM", "联通": "CU", "电信": "CT"}

def _isp_of(tag):
    """从备注里识别运营商标签: CM=移动 CU=联通 CT=电信, 兼容 CMCC/CUCC/CTCC 写法"""
    m = _ISP_PAT.search(tag or "")
    if not m:
        return ""
    key = m.group(1).upper()
    return _ISP_ALIAS.get(key, key)

def _load_cf_networks(session):
    try:
        r = session.get(CF_V4_URL, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        nets = [ipaddress.ip_network(ln.strip()) for ln in r.text.splitlines() if ln.strip()]
        log("EDGE-POOL", f"载入 Cloudflare 官方段: {len(nets)} 条")
        return nets
    except Exception as exc:
        log("EDGE-POOL", f"载入 CF 段失败({exc}), 本次跳过 IP 归属校验")
        return None

def _is_ip_literal(host):
    try:
        ipaddress.ip_address(host)
        return True
    except ValueError:
        return False

def _is_cf_addr(host, nets):
    """域名形态一律放行; IP 形态必须落在 CF 官方段内"""
    if nets is None:
        return True
    if not _is_ip_literal(host):
        return True
    ip = ipaddress.ip_address(host)
    return any(ip in n for n in nets)

def _norm_addr(raw):
    """规范化地址: 去空白/尾点, 泛域名 *.x.com 补前缀; 非法返回 ''"""
    s = (raw or "").strip().strip('"\'<>,;').rstrip(".")
    if s.startswith("*."):
        s = f"{WILDCARD_PREFIX}.{s[2:]}"
    host = s.partition(":")[0]
    if not host:
        return ""
    if _is_ip_literal(host) or _DOMAIN_RE.match(host):
        return s
    return ""

def _walk_json_addrs(node, out=None):
    out = [] if out is None else out
    if isinstance(node, dict):
        for k, v in node.items():
            if isinstance(v, str) and k.lower() in _JSON_ADDR_KEYS:
                addr = _norm_addr(v)
                if addr:
                    out.append(addr)
            else:
                _walk_json_addrs(v, out)
    elif isinstance(node, list):
        for item in node:
            _walk_json_addrs(item, out)
    return out

def _iter_pool_entries(src, body):
    """按来源格式产出 (地址, 备注)"""
    kind = src.get("kind", "text")
    if kind == "json":
        try:
            parsed = json.loads(body)
        except Exception as exc:
            log("EDGE-POOL", f"[解析失败] {src['url']} -> JSON: {exc}")
            return
        for addr in _walk_json_addrs(parsed):
            yield addr, src.get("label", "")
        return
    if kind == "html":
        pattern = src.get("pattern")
        if not pattern:
            return
        for m in re.findall(pattern, body):
            addr = _norm_addr(m)
            if addr:
                yield addr, src.get("label", "")
        return
    for ln in body.splitlines():
        ln = ln.strip()
        if not ln or ln.startswith("#"):
            continue
        addr, _, tag = ln.partition("#")
        addr = _norm_addr(addr)
        if addr:
            yield addr, tag.strip()

def _interleave_sources(buckets):
    """按来源交错合并: nodes.txt 是轮询取入口的, 不交错的话排后面的源永远轮不到"""
    out = []
    for i in range(max((len(b) for b in buckets), default=0)):
        for b in buckets:
            if i < len(b):
                out.append(b[i])
    return out

def fetch_edge_pool(session, limit=0):
    """聚合多个众包优选 API/IP 与域名源, 产出 [{entry, isp, tag, src}]"""
    nets = _load_cf_networks(session)
    seen, buckets = set(), []
    for src in EDGE_POOL_APIS:
        url, kind = src["url"], src.get("kind", "text")
        try:
            if kind == "json":
                r = session.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0",
                                                          "Accept": "application/json, text/plain, */*"})
            else:
                r = session.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            r.encoding = "utf-8"
        except Exception as exc:
            log("EDGE-POOL", f"[跳过] {url} -> {type(exc).__name__}: {exc}")
            continue
        bucket = []
        for addr, tag in _iter_pool_entries(src, r.text):
            host, _, port = addr.partition(":")
            # 域名一律放行(靠客户端 DNS 就近解析), IP 必须落在 CF 官方段内
            if not _is_cf_addr(host, nets):
                continue
            if not tag and not _is_ip_literal(host):
                tag = src.get("label") or tag
            entry = f"{host}:{port.strip() or '443'}"
            if entry in seen:
                continue
            seen.add(entry)
            bucket.append({"entry": entry, "isp": _isp_of(tag), "tag": tag, "src": url})
        buckets.append(bucket)
        log("EDGE-POOL", f"[OK:{kind}] {url} -> 新增 {len(bucket)}")
    pool = _interleave_sources(buckets)
    pool = _validate_domain_pool(pool, nets)
    if limit > 0:
        pool = pool[:limit]
    isp_stat = {k: sum(1 for p in pool if p["isp"] == k) for k in ("CM", "CU", "CT")}
    domain_n = sum(1 for p in pool if _is_domain_entry(p["entry"]))
    log("EDGE-POOL", f"池子合计 {len(pool)} 个入口 (域名 {domain_n} / IP {len(pool) - domain_n}), 运营商标注: {isp_stat}")
    return pool

def _resolve_domain(host):
    """优选域名解析: 优先 DoH (避开本机/运营商 DNS 污染造成的误杀), DoH 不可用时退回系统解析"""
    try:
        j = requests.get(EDGE_DOH_URL, params={"name": host, "type": "A"}, timeout=8,
                         headers={"User-Agent": "Mozilla/5.0"}).json()
        ips = [a["data"] for a in j.get("Answer", []) if a.get("type") == 1 and _is_ip_literal(a.get("data", ""))]
        if ips:
            return ips
    except Exception:
        pass
    try:
        return [socket.gethostbyname(host)]
    except Exception:
        return []

def _validate_domain_pool(pool, nets):
    """优选域名逐个反查: 解析不到或不在 CF 段的直接丢弃 (池子里的 IP 原样保留)"""
    if nets is None or os.environ.get("EDGE_DOMAIN_DNS_CHECK", "1") != "1":
        return pool
    domains = [p["entry"].partition(":")[0] for p in pool if _is_domain_entry(p["entry"])]
    if not domains:
        return pool
    resolved = {}
    with ThreadPoolExecutor(max_workers=16) as ex:
        fut_map = {ex.submit(_resolve_domain, d): d for d in domains}
        for fut in as_completed(fut_map):
            d = fut_map[fut]
            try:
                resolved[d] = fut.result()
            except Exception:
                resolved[d] = []
    kept, dropped = [], []
    for p in pool:
        host = p["entry"].partition(":")[0]
        if _is_ip_literal(host):
            kept.append(p)
            continue
        hits = [i for i in (resolved.get(host) or []) if any(ipaddress.ip_address(i) in n for n in nets)]
        if hits:
            kept.append(p)
        else:
            dropped.append(f"{host}->{(resolved.get(host) or ['NXDOMAIN'])[0]}")
    kept_domain = len(domains) - len(dropped)
    log("EDGE-POOL", f"域名 DNS 校验: 保留 {kept_domain}/{len(domains)}"
                     + (f", 剔除 {' '.join(dropped[:6])}" if dropped else ""))
    return kept

def _is_domain_entry(entry):
    return not _is_ip_literal(entry.partition(":")[0])

def _isp_entries(pool, isp):
    """运营商专用入口 = 该运营商标签的 IP + 无运营商标签的优选域名 (域名走客户端 DNS, 与运营商无关)"""
    tagged = [p["entry"] for p in pool if p.get("isp") == isp]
    if not tagged:
        return []
    domains = [p["entry"] for p in pool if not p.get("isp") and _is_domain_entry(p["entry"])]
    return _interleave_sources([tagged, domains]) if domains else tagged

def resolve_edge(pool=None):
    """确定最终入口池: HOSTS_ENTRY 手填 > 动态池(可按 EDGE_ISP 过滤) > 内置静态表"""
    manual = os.environ.get("HOSTS_ENTRY", "").strip()
    if manual:
        return [e.strip() for e in manual.split(",") if e.strip()], "HOSTS_ENTRY(手工指定)"
    if pool:
        want = os.environ.get("EDGE_ISP", "").strip().upper()
        if want:
            picked = _isp_entries(pool, want)
            if picked:
                return picked, f"动态优选池(ISP={want}: {len(picked)} 入口)"
            log("EDGE-POOL", f"动态池中没有 ISP={want} 的入口, 回退使用全池")
            return [p["entry"] for p in pool], f"动态优选池({len(pool)}, ISP={want} 无匹配已回退全池)"
        return [p["entry"] for p in pool], f"动态优选池({len(pool)})"
    return list(EDGE_HOSTS), "内置静态表(回退)"

# edgetunnel 入口地址池
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

NODES_URL = os.environ.get("NODES_URL", "https://zczy-k.github.io/gate/nodes.txt")

def build_nodes_text(data, edge=None, label="内置静态表"):
    """生成纯节点行版本 (无注释): 每行 = 入口地址#名字$sstp://..."""
    countries = data["countries"]
    if not edge:
        edge, label = list(EDGE_HOSTS), "内置静态表(回退)"
    fanout = max(1, int(os.environ.get("EDGE_FANOUT", "1")))
    log("EDGE", f"入口来源: {label} | 入口数 {len(edge)} | 每节点入口数 {fanout}")
    lines = []
    idx = 0
    ordered = sorted(countries.items(), key=lambda kv: (-int(kv[1].get("count") or 0), str(kv[1].get("code") or kv[0])))
    for cname, grp in ordered:
        code = str(grp.get("code") or "?").upper()
        zh = COUNTRY_ZH.get(code) or (code if code and code != "?" else cname)
        nodes = sorted(grp["nodes"], key=lambda n: (0 if n.get("residential") == "residential" else 1, n.get("latency_ms") is None, n.get("latency_ms") or 0, n.get("host") or ""))
        groups = (
            ("住宅", [n for n in nodes if n.get("residential") == "residential"]),
            ("机房", [n for n in nodes if n.get("residential") != "residential"]),
        )
        for kind, sel in groups:
            for i, n in enumerate(sel, 1):
                for k in range(fanout):
                    entry = edge[idx % len(edge)]
                    idx += 1
                    suffix = f"-{k + 1}" if fanout > 1 else ""
                    lines.append(f"{entry}#{zh}-{kind}-{i:02d}{suffix}$sstp://vpn:vpn@{n['host']}:{n['port']}")
    return "\n".join(lines) + "\n"

def write_outputs(data, pool=None):
    os.makedirs(PUBLIC_DIR, exist_ok=True)
    written = []

    data_path = os.path.join(PUBLIC_DIR, "data.json")
    with open(data_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)
    written.append(data_path)

    html_path = os.path.join(PUBLIC_DIR, "index.html")
    if os.path.exists(TEMPLATE_HTML):
        with open(TEMPLATE_HTML, "r", encoding="utf-8") as f:
            html = f.read()
    else:
        html = ("<html><head><meta charset='utf-8'><title>VPN Gate SSTP 节点</title></head>"
                "<body><h1>VPN Gate SSTP 节点</h1><pre id='out'></pre></body>"
                "<script>fetch('data.json').then(r=>r.json()).then(d=>out.textContent=JSON.stringify(d.stats)).catch(e=>out.textContent='加载失败:'+e)</script></html>")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    written.append(html_path)

    edge, label = resolve_edge(pool)
    nodes_path = os.path.join(PUBLIC_DIR, "nodes.txt")
    with open(nodes_path, "w", encoding="utf-8") as f:
        f.write(build_nodes_text(data, edge, label))
    written.append(nodes_path)

    if pool:
        pool_path = os.path.join(PUBLIC_DIR, "edge_pool.txt")
        with open(pool_path, "w", encoding="utf-8") as f:
            for p in pool:
                f.write(f"{p['entry']}\t{p.get('isp') or '-'}\t{p.get('tag') or '-'}\n")
        written.append(pool_path)

        # 按运营商拆分的订阅: 用户挑自己运营商那一份填进 edgetunnel 即可
        for isp in ("CM", "CU", "CT"):
            sub = _isp_entries(pool, isp)
            if not sub:
                continue
            sub_path = os.path.join(PUBLIC_DIR, f"nodes-{isp.lower()}.txt")
            with open(sub_path, "w", encoding="utf-8") as f:
                f.write(build_nodes_text(data, sub, f"ISP={isp}(专用, {len(sub)} 入口)"))
            written.append(sub_path)

    return written

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
    uniq = dedupe(sstp_nodes)

    if MAX_CHECK_NODES > 0:
        uniq = uniq[:MAX_CHECK_NODES]

    pool = fetch_edge_pool(session, int(os.environ.get("EDGE_POOL_LIMIT", "0")))

    log("VPN GATE", f"获取原始节点: {raw_count}")
    log("VPN GATE", f"SSTP 节点: {sstp_count}")
    log("VPN GATE", f"去重后: {len(uniq)}")

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

    written = write_outputs(data, pool)
    for _p in written:
        log("WEBSITE", f"生成 {os.path.relpath(_p, REPO_DIR)}")
    if len(written) > 3:
        base = NODES_URL.rsplit("/", 1)[0]
        log("USAGE", f"运营商专用订阅: {base}/nodes-cu.txt(联通) / nodes-cm.txt(移动) / nodes-ct.txt(电信)")
    log("USAGE", f"自动轮换: 把 {NODES_URL} 填入 edgetunnel 后台「自定义优选IP」框 (一次配置, 之后每 30 分钟自动更新)")
    log("WEBSITE", "完成 (GitHub Pages 部署由 workflow 执行)")

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as exc:
        die(f"程序异常: {type(exc).__name__}: {exc}")

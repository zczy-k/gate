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
import ssl
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

def _sources_from_env(var_name, default, sep=","):
    """环境变量覆盖来源表; 条目写法 'url' / 'url|kind' / 'url|kind|备注', 条目之间按 sep 分隔"""
    raw = os.environ.get(var_name, "").strip()
    if not raw:
        return default
    out = []
    for item in raw.split(sep):
        item = item.strip()
        if not item:
            continue
        parts = item.split("|")
        out.append({"url": parts[0], "kind": parts[1] if len(parts) > 1 else "text",
                    "label": parts[2] if len(parts) > 2 else ""})
    return out or default

EDGE_POOL_APIS = _sources_from_env("EDGE_POOL_APIS", EDGE_POOL_DEFAULT_SOURCES)

# Cloudflare 官方 IPv4 段, 用来剔除优选源里混进来的非 CF 脏 IP
# (例如 ipdb 的 type=bestproxy 返回的是 Oracle/阿里云 IP, 当入口必然连不通)
CF_V4_URL = os.environ.get("CF_V4_URL", "https://www.cloudflare.com/ips-v4")

# 域名归属校验用的 DoH 服务(依次尝试)。不写死一个是因为优选域名大量使用
# 「分线路解析」(万网/DNSPod): 境外视图 NOERROR 但无 Answer, 必须靠多视图交叉 + ECS 判断
EDGE_DOH_URLS = [u.strip() for u in os.environ.get(
    "EDGE_DOH_URLS",
    "https://dns.google/resolve,https://cloudflare-dns.com/dns-query",
).split(",") if u.strip()]
# 带中国方向 ECS 前缀再问一次。实测 dns.google 对万网分线路域名加 ECS 仍返回空记录,
# 真正的解法是问第二个 DoH (cloudflare-dns.com), 所以这里默认关闭, 留作可调。
EDGE_DOH_ECS = os.environ.get("EDGE_DOH_ECS", "").strip()
# on=分级判定(默认) / strict=解析不到即剔除 / off=不校验
DNS_CHECK_MODE = os.environ.get("EDGE_DOMAIN_DNS_CHECK", "on").strip().lower()

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
            bucket.append({"entry": entry, "isp": _isp_of(tag), "tag": tag, "src": url,
                           "src_host": url.split("/")[2]})
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

DNS_STATUS = {1: "FORMERR", 2: "SERVFAIL", 3: "NXDOMAIN", 4: "NOTIMP", 5: "REFUSED"}

def _doh_query(url, host, ecs=None):
    params = {"name": host, "type": "A"}
    if ecs:
        params["edns_client_subnet"] = ecs
    try:
        j = requests.get(url, params=params, timeout=8,
                         headers={"User-Agent": "Mozilla/5.0", "Accept": "application/dns-json"}).json()
    except Exception:
        return None
    ips = [a["data"] for a in (j.get("Answer") or [])
           if a.get("type") == 1 and _is_ip_literal(a.get("data", "") or "")]
    return ips, j.get("Status")

def _resolve_domain(host):
    """返回 (IP 列表, DNS Status); Status=None 表示所有 DoH 都没答上。
    依次问多个 DoH -> 无记录时带大陆 ECS 再问一次(还原分线路解析) -> 最后退回系统解析"""
    status = None
    for url in EDGE_DOH_URLS:
        got = _doh_query(url, host)
        if got is None:
            continue
        ips, st = got
        if ips:
            return ips, st
        if status is None:
            status = st
    if EDGE_DOH_ECS and status != 3:          # NXDOMAIN 是权威定论, 不必再问
        for url in EDGE_DOH_URLS:
            got = _doh_query(url, host, ecs=EDGE_DOH_ECS)
            if got is None:
                continue
            ips, st = got
            if ips:
                return ips, st
            if status is None:
                status = st
    # 只有 DoH 全部连不上才退回系统解析; DoH 已给出 NXDOMAIN/SERVFAIL 时不能被本地污染 DNS 翻案
    if status is None:
        try:
            sys_ip = socket.gethostbyname(host)
            if sys_ip:
                return [sys_ip], status
        except Exception:
            pass
    return [], status

def _validate_domain_pool(pool, nets):
    """优选域名反查分级判定:
      解析到 CF 段          -> 保留
      解析到但不在 CF 段    -> 剔除 (源站不在 Cloudflare, 当入口必然连不通)
      NOERROR 但无 A 记录   -> 保留并标记 (万网/DNSPod 分线路解析, 境外视图本就为空)
      SERVFAIL / NXDOMAIN   -> 剔除 (域名真死)
    """
    if nets is None or DNS_CHECK_MODE in ("off", "0", "no"):
        return pool
    domains = [p["entry"].partition(":")[0] for p in pool if _is_domain_entry(p["entry"])]
    if not domains:
        return pool
    log("EDGE-POOL", f"DoH 反查 {len(domains)} 个域名 ({', '.join(u.split('/')[2] for u in EDGE_DOH_URLS)}"
                     + (f" + 大陆ECS={EDGE_DOH_ECS}" if EDGE_DOH_ECS else "") + ")")
    resolved = {}
    with ThreadPoolExecutor(max_workers=16) as ex:
        fut_map = {ex.submit(_resolve_domain, d): d for d in domains}
        for fut in as_completed(fut_map):
            d = fut_map[fut]
            try:
                resolved[d] = fut.result()
            except Exception:
                resolved[d] = ([], None)
    strict = DNS_CHECK_MODE == "strict"
    kept, dropped = [], []
    for p in pool:
        host = p["entry"].partition(":")[0]
        if _is_ip_literal(host):
            kept.append(p)
            continue
        ips, st = resolved.get(host, ([], None))
        hits = [i for i in ips if any(ipaddress.ip_address(i) in n for n in nets)]
        if hits:
            p["dns"] = f"CF确认({hits[0]})"
            kept.append(p)
        elif ips:
            p["dns"] = f"非CF段({ips[0]})"
            dropped.append(f"{host} {p['dns']}")
        elif st == 3:
            p["dns"] = "NXDOMAIN"
            dropped.append(f"{host} NXDOMAIN")
        elif st not in (0, None):
            p["dns"] = f"DNS错误({DNS_STATUS.get(st, st)})"
            dropped.append(f"{host} {p['dns']}")
        elif strict:
            p["dns"] = "无A记录(strict剔除)"
            dropped.append(f"{host} 无A记录")
        else:
            p["dns"] = "境外视图无A记录(分线路解析)"
            kept.append(p)
    kept_domain = sum(1 for p in kept if _is_domain_entry(p["entry"]))
    viewless = sum(1 for p in kept if str(p.get("dns", "")).startswith("境外视图"))
    log("EDGE-POOL", f"域名校验: 保留 {kept_domain}/{len(domains)}"
                     + (f" (含 {viewless} 个境外视图无记录的分线路域名)" if viewless else "")
                     + (f", 剔除 {' '.join(dropped[:6])}" if dropped else ""))
    return kept

# ---------------------------------------------------------------------------
# 中转池 (CF 反代/中转 IP, 故意不做 Cloudflare 段校验, 只做存活探测)
# ---------------------------------------------------------------------------
# 这些地址不在 CF 官方段内, 靠对端把 TLS 按 SNI 转发给 Cloudflare 才能当 edgetunnel 入口。
# 视角限制: Actions 在美国, 这里只判定 "活着 / 是不是 TLS 透传", 延迟数字不参与任何排序,
#           谁对你好用由客户端 urltest 决定。
EDGE_RELAY_DEFAULT_SOURCES = [
    # seeck(Senflare) 的 region 用 %2C 分隔; 不写 region 会返回 5000+ 条全量, 必须截断
    {"url": "https://proxy.seeck.cn/api/nodes?region=HK%2CJP%2CSG%2CTW&limit=40"
            "&format={ip}:{port}%23{name}%20{region}", "kind": "text", "label": "中转"},
    {"url": "https://ipdb.api.030101.xyz/?type=bestproxy&country=true", "kind": "text", "label": "中转"},
]
# 多个中转源用分号分隔 (URL 里本身带逗号和 {})
EDGE_RELAY_APIS = _sources_from_env("EDGE_RELAY_APIS", EDGE_RELAY_DEFAULT_SOURCES, sep=";")

RELAY_MODE = os.environ.get("EDGE_RELAY_MODE", "off").strip().lower()      # off | file | append
RELAY_SNI = os.environ.get("EDGE_RELAY_SNI", "").strip()                    # 你的伪装域名, 填了才做强校验
RELAY_TIMEOUT = float(os.environ.get("EDGE_RELAY_TIMEOUT", "4"))
RELAY_CONCURRENCY = max(1, int(os.environ.get("EDGE_RELAY_CONCURRENCY", "8")))
RELAY_LIMIT = int(os.environ.get("EDGE_RELAY_LIMIT", "120"))                # 探测候选总上限
RELAY_QUOTA = int(os.environ.get("EDGE_RELAY_QUOTA", "60"))                 # 每个源最多贡献多少条 (0=不限)
RELAY_MIN_ALIVE = int(os.environ.get("EDGE_RELAY_MIN_ALIVE", "3"))          # 存活太少视为本功能不可用
RELAY_EVERY = max(0, int(os.environ.get("EDGE_RELAY_EVERY", "4")))          # append 模式: 每 N 条主池插 1 条中转
RELAY_DEBUG_MIN = 5                                        # 某个源候选数低于此值时把响应原文贴进日志

def _probe_relay(entry):
    """L1 TCP 连接; 配了 EDGE_RELAY_SNI 再加 L2 严格 TLS 证书校验(透传型中转原样回传 CF 证书才过)"""
    host, _, port = entry.partition(":")
    port = int(port or 443)
    t0 = time.time()
    def cost():
        return round((time.time() - t0) * 1000)
    try:
        sock = socket.create_connection((host, port), timeout=RELAY_TIMEOUT)
    except Exception as exc:
        return {"alive": False, "detail": f"TCP不通({type(exc).__name__})", "ms": cost()}
    try:
        if not RELAY_SNI:
            return {"alive": True, "detail": "仅TCP(未配SNI)", "ms": cost()}
        ctx = ssl.create_default_context()
        try:
            tls = ctx.wrap_socket(sock, server_hostname=RELAY_SNI)
            detail = f"TLS透传OK({tls.version()})"
            tls.close()
            return {"alive": True, "detail": detail, "ms": cost()}
        except Exception as exc:
            return {"alive": False, "detail": f"TLS不过({type(exc).__name__})", "ms": cost()}
    finally:
        try:
            sock.close()
        except Exception:
            pass

def fetch_relay_pool():
    """抓中转池并逐个探活, 返回 (存活入口列表, 全部探测记录)"""
    seen, cand = set(), []
    for src in EDGE_RELAY_APIS:
        url, kind = src["url"], src.get("kind", "text")
        try:
            # seeck 服务端要现做聚合+连通检测, 慢的时候可达 1 分钟
            r = requests.get(url, timeout=60, headers={"User-Agent": "Mozilla/5.0",
                                                       "Accept": "text/plain, application/json, */*"})
            r.raise_for_status()
            r.encoding = "utf-8"
        except Exception as exc:
            log("RELAY", f"[跳过] {url} -> {type(exc).__name__}: {exc}")
            continue
        bucket = []
        for addr, tag in _iter_pool_entries(src, r.text):
            host, _, port = addr.partition(":")
            entry = f"{host}:{port.strip() or '443'}"
            if entry in seen:
                continue
            seen.add(entry)
            bucket.append({"entry": entry, "isp": _isp_of(tag), "tag": tag or src.get("label", ""),
                           "src": url, "src_host": url.split("/")[2]})
        raw_n = len(bucket)
        cap = raw_n
        if RELAY_QUOTA > 0:
            cap = min(cap, RELAY_QUOTA)
        if RELAY_LIMIT > 0:
            cap = min(cap, max(0, RELAY_LIMIT - len(cand)))
        bucket = bucket[:cap]
        cand.extend(bucket)
        body_lines = len([l for l in r.text.splitlines() if l.strip()])
        cut = f" (源内 {raw_n} 条, 配额截到 {len(bucket)})" if len(bucket) < raw_n else ""
        msg = f"[OK:{kind}] {url} -> 候选 {len(bucket)}{cut} (累计 {len(cand)}) | 响应 {len(r.text)} 字节 / {body_lines} 行"
        if raw_n < RELAY_DEBUG_MIN:
            snippet = re.sub(r"\s+", " ", r.text.strip())[:180]
            msg += f" | 响应开头: {snippet}"
        log("RELAY", msg)
        if RELAY_LIMIT > 0 and len(cand) >= RELAY_LIMIT:
            log("RELAY", f"已达候选总上限 {RELAY_LIMIT}, 其余源不再拉取")
            break
    if not cand:
        log("RELAY", "没有拿到任何中转候选")
        return [], []
    log("RELAY", f"开始探活: {len(cand)} 个候选 (并发 {RELAY_CONCURRENCY}, 单个超时 {RELAY_TIMEOUT}s, "
                 f"SNI={'已配 ' + RELAY_SNI if RELAY_SNI else '未配 -> 仅 TCP 判定'})")
    t0 = time.time()
    rows = []
    with ThreadPoolExecutor(max_workers=RELAY_CONCURRENCY) as ex:
        fut_map = {ex.submit(_probe_relay, c["entry"]): c for c in cand}
        for fut in as_completed(fut_map):
            c = dict(fut_map[fut])
            try:
                c.update(fut.result())
            except Exception as exc:
                c.update({"alive": False, "detail": f"探测异常({type(exc).__name__})", "ms": None})
            rows.append(c)
    rows.sort(key=lambda r: (not r["alive"], r["entry"]))
    alive = [r["entry"] for r in rows if r["alive"]]
    log("RELAY", f"探活完成: 存活 {len(alive)}/{len(rows)} (耗时 {time.time() - t0:.1f}s)"
                 f" | 失败样例: " + ", ".join(f"{r['entry']} {r['detail']}" for r in rows if not r["alive"])[:200])
    per_src = {}
    for r in rows:
        h = r.get("src_host") or "-"
        a, t = per_src.get(h, (0, 0))
        per_src[h] = (a + (1 if r["alive"] else 0), t + 1)
    log("RELAY", "各源存活率: " + " | ".join(f"{h} {a}/{t}" for h, (a, t) in per_src.items())
                  + " (美国 runner 视角, 只反映源池质量, 不代表你的体感)")
    if len(alive) < RELAY_MIN_ALIVE:
        log("RELAY", f"存活数 < {RELAY_MIN_ALIVE}, 判定本功能本次不可用, 不产出中转订阅")
        return [], rows
    return alive, rows

def weave_relay(main, relays, every):
    """append 模式: 每 every 条主池入口插 1 条中转, 保证主池仍占多数"""
    if not relays or every <= 0:
        return list(main)
    out, ri = [], 0
    for i, e in enumerate(main, 1):
        out.append(e)
        if i % every == 0:
            out.append(relays[ri % len(relays)])
            ri += 1
    return out

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

# ---------------------------------------------------------------------------
# 历史累积复测
# ---------------------------------------------------------------------------
# 官方 API 每轮只公布约 100 台, 且成员每轮轮换。把上一轮 data.json 里的成功节点
# 作为种子合并进本轮候选一起复测, 可用节点数才能超过单轮源头规模。
# 死节点不会被带进订阅: 必须本轮再次通过 Worker 的 SSTP 握手才会留在 history 里。
HISTORY_URL = os.environ.get("HISTORY_URL", NODES_URL.rsplit("/", 1)[0] + "/data.json")
HISTORY_ENABLED = os.environ.get("HISTORY_RECHECK", "1").strip() != "0"
HISTORY_DAYS = int(os.environ.get("HISTORY_DAYS", "14"))     # 连续多少天没复测成功就淘汰
HISTORY_MAX = int(os.environ.get("HISTORY_MAX", "400"))      # history / 复测总量上限
HISTORY_TIME_FMT = "%Y-%m-%d %H:%M UTC"

def _parse_hist_time(text):
    try:
        return datetime.strptime(str(text).strip(), HISTORY_TIME_FMT).replace(tzinfo=timezone.utc)
    except Exception:
        return None

def fetch_history_nodes(session):
    """读上一轮发布的 data.json, 返回 (可复测节点列表, 原始 history 条目)"""
    if not HISTORY_ENABLED:
        log("HISTORY", "HISTORY_RECHECK=0, 本轮只用官方源")
        return [], []
    try:
        r = session.get(HISTORY_URL, timeout=30, headers={"User-Agent": "Mozilla/5.0 (gate-checker)"})
        if r.status_code != 200:
            log("HISTORY", f"上一轮数据不可用 (HTTP {r.status_code}), 本轮只用官方源")
            return [], []
        prev = r.json()
    except Exception as exc:
        log("HISTORY", f"读取 {HISTORY_URL} 失败({type(exc).__name__}), 本轮只用官方源")
        return [], []
    entries = prev.get("history") or []
    if not entries:                       # 首次升级: 用 available 兜底播种
        entries = [{"host": n.get("host"), "port": n.get("port"), "ip": n.get("ip"),
                    "country": n.get("country"), "country_code": n.get("country_code"),
                    "last_ok": n.get("checked_at") or ""} for n in (prev.get("available") or [])]
        if entries:
            log("HISTORY", "上一轮还没有 history 字段, 改用 available 播种")
    nodes, stale = [], 0
    now = datetime.now(timezone.utc)
    for e in entries:
        host = str(e.get("host") or "").strip()
        try:
            port = int(e.get("port") or 0)
        except Exception:
            port = 0
        if not host or not (1 <= port <= 65535):
            continue
        ts = _parse_hist_time(e.get("last_ok"))
        if ts is not None and (now - ts).total_seconds() > HISTORY_DAYS * 86400:
            stale += 1
            continue
        nodes.append({"host": host, "port": port, "ip": str(e.get("ip") or ""),
                      "country": str(e.get("country") or ""),
                      "country_code": str(e.get("country_code") or ""),
                      "last_ok": str(e.get("last_ok") or "")})
    nodes.sort(key=lambda n: n["last_ok"], reverse=True)
    log("HISTORY", f"载入历史节点 {len(nodes)} 个 (淘汰超过 {HISTORY_DAYS} 天未成功的 {stale} 个) <- {HISTORY_URL}")
    return nodes, nodes

def merge_history(current, history_nodes):
    """官方源优先, 历史种子只补官方本轮没公布的 host:port"""
    seen = {(n["host"].lower(), n["port"]) for n in current}
    extra = []
    for n in history_nodes:
        key = (n["host"].lower(), n["port"])
        if key in seen:
            continue
        seen.add(key)
        extra.append(n)
    if HISTORY_MAX > 0:
        extra = extra[:max(0, HISTORY_MAX - len(current))]
    return current + extra, len(extra)

def build_history(results, prev_entries):
    """本轮成功的节点刷新 last_ok; 本轮没测到的旧记录原样保留, 到龄自动出局"""
    now = datetime.now(timezone.utc).strftime(HISTORY_TIME_FMT)
    merged = {}
    for r in results:
        if not r.get("success"):
            continue
        merged[(str(r["host"]).lower(), int(r["port"]))] = {
            "host": r["host"], "port": int(r["port"]), "ip": r.get("ip") or "",
            "country": r.get("country") or "", "country_code": r.get("country_code") or "",
            "last_ok": r.get("checked_at") or now}
    keep = 0
    for e in prev_entries:
        key = (str(e.get("host")).lower(), int(e.get("port")))
        if key in merged:
            continue
        ts = _parse_hist_time(e.get("last_ok"))
        if ts is not None and (datetime.now(timezone.utc) - ts).total_seconds() > HISTORY_DAYS * 86400:
            continue
        merged[key] = {k: e.get(k, "") for k in ("host", "port", "ip", "country", "country_code", "last_ok")}
        keep += 1
    out = sorted(merged.values(), key=lambda x: str(x.get("last_ok")), reverse=True)
    if HISTORY_MAX > 0:
        out = out[:HISTORY_MAX]
    log("HISTORY", f"写回 history: 本轮刷新 {len(merged) - keep} 个 + 沿用上轮 {keep} 个 = {len(out)} 个")
    return out

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

def write_outputs(data, pool=None, relay=None, relay_rows=None):
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
    if edge and relay and RELAY_MODE == "append":
        edge = weave_relay(edge, relay, RELAY_EVERY)
        label = f"{label} + 中转{len(relay)}条(每 {RELAY_EVERY} 条主池插 1 条)"
    nodes_path = os.path.join(PUBLIC_DIR, "nodes.txt")
    with open(nodes_path, "w", encoding="utf-8") as f:
        f.write(build_nodes_text(data, edge, label))
    written.append(nodes_path)

    if relay and RELAY_MODE in ("file", "append"):
        relay_label = f"中转池(存活 {len(relay)} 个 | 美国 runner 判活" + \
                      (f", TLS 透传校验 SNI={RELAY_SNI}" if RELAY_SNI else ", 仅 TCP 校验") + ")"
        relay_path = os.path.join(PUBLIC_DIR, "nodes-relay.txt")
        with open(relay_path, "w", encoding="utf-8") as f:
            f.write(build_nodes_text(data, relay, relay_label))
        written.append(relay_path)

    if relay_rows:
        rows_path = os.path.join(PUBLIC_DIR, "relay_pool.txt")
        with open(rows_path, "w", encoding="utf-8") as f:
            for r in relay_rows:
                f.write(f"{r['entry']}\t{'ALIVE' if r['alive'] else 'DEAD'}\t{r.get('ms') or '-'}"
                        f"\t{r.get('detail') or '-'}\t{r.get('tag') or '-'}\t{r.get('src_host') or '-'}\n")
        written.append(rows_path)

    if pool:
        pool_path = os.path.join(PUBLIC_DIR, "edge_pool.txt")
        with open(pool_path, "w", encoding="utf-8") as f:
            for p in pool:
                f.write(f"{p['entry']}\t{p.get('isp') or '-'}\t{p.get('tag') or '-'}"
                        f"\t{p.get('dns') or '-'}\t{p.get('src_host') or '-'}\n")
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

    hist_nodes, hist_prev = fetch_history_nodes(session)
    uniq, hist_added = merge_history(uniq, hist_nodes)

    if MAX_CHECK_NODES > 0:
        uniq = uniq[:MAX_CHECK_NODES]

    pool = fetch_edge_pool(session, int(os.environ.get("EDGE_POOL_LIMIT", "0")))

    log("VPN GATE", f"获取原始节点: {raw_count}")
    log("VPN GATE", f"SSTP 节点: {sstp_count}")
    log("VPN GATE", f"去重后: {len(uniq) - hist_added} + 历史补种 {hist_added} = 复测 {len(uniq)}")

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

    relay, relay_rows = ([], [])
    if RELAY_MODE != "off":
        relay, relay_rows = fetch_relay_pool()

    data = build_outputs(results, raw_count, sstp_count, source)
    data["history"] = build_history(results, hist_prev)
    data["recheck"] = {"seeds": hist_added, "checked_total": len(uniq),
                       "days": HISTORY_DAYS, "max": HISTORY_MAX, "source": HISTORY_URL}
    data["stats"]["history_kept"] = len(data["history"])
    log("RESULT", f"可用节点: {len(success)}")
    log("RESULT", f"国家数量: {data['stats']['countries']}")
    log("RESULT", f"history 写入: {len(data['history'])} 个 (下一轮的复测种子)")

    written = write_outputs(data, pool, relay, relay_rows)
    for _p in written:
        log("WEBSITE", f"生成 {os.path.relpath(_p, REPO_DIR)}")
    if len(written) > 3:
        base = NODES_URL.rsplit("/", 1)[0]
        log("USAGE", f"运营商专用订阅: {base}/nodes-cu.txt(联通) / nodes-cm.txt(移动) / nodes-ct.txt(电信)")
    if relay and RELAY_MODE in ("file", "append"):
        base = NODES_URL.rsplit("/", 1)[0]
        log("USAGE", f"中转专用订阅: {base}/nodes-relay.txt (仅判活, 速度请交给客户端 urltest)")
    log("USAGE", f"自动轮换: 把 {NODES_URL} 填入 edgetunnel 后台「自定义优选IP」框 (一次配置, 之后每 30 分钟自动更新)")
    log("WEBSITE", "完成 (GitHub Pages 部署由 workflow 执行)")

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as exc:
        die(f"程序异常: {type(exc).__name__}: {exc}")

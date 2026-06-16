#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
投资作战室 · 市场数据抓取脚本 v2.0
===========================
用途：每日抓取指数估值(PE/PB/分位) + 市场信号(TS/MC/Stage/板块评分)，
      输出标准作战室 JSON 片段，可直接导入作战室「数据管理」页。

运行环境：Python 3.8+，优先 akshare（pip install akshare），降级 requests
运行方式：
  python warroom_market_fetch_v2.py          # 交互模式，输出文件
  python warroom_market_fetch_v2.py --cron   # 静默模式，适合定时任务
  python warroom_market_fetch_v2.py --pretty # 终端输出格式化 JSON

输出文件：market_data_YYYY-MM-DD.json（可直接粘贴到作战室导入框）

作者：Kimi Work 修复整合版
"""

import json, math, os, sys, argparse, hashlib, warnings, datetime
from decimal import Decimal
from typing import List, Dict, Any, Optional, Tuple

warnings.filterwarnings('ignore')

# ==================== 配置区 ====================

# 作战室 PE_PROXY 映射关系：{持仓代码: 估值指数代码}
# 注意：akshare 指数代码通常不带 .SH/.SZ 后缀，部分需要转换
INDEX_PE_MAP = {
    # 核心仓
    "930841": {"name": "中证红利低波动", "index": "930841", "asset": "equity"},    # 红利低波 563020
    "930050": {"name": "中证A50", "index": "930050", "asset": "equity"},           # 中证A50 021208
    "510300": {"name": "沪深300", "index": "000300", "asset": "equity"},            # 沪深300 110020
    # 战术仓
    "930713": {"name": "中证人工智能", "index": "930713", "asset": "equity"},       # AI算力 159819
    "H30590": {"name": "中证机器人", "index": "H30590", "asset": "equity"},         # 机器人 562500
    "930813": {"name": "中华半导体芯片", "index": "930813", "asset": "equity"},     # 半导体 512480
    # 对冲仓
    "513100": {"name": "纳指ETF", "index": "NDX", "asset": "equity"},               # 纳斯达克100（美股，可能受限）
    "518880": {"name": "黄金ETF", "index": "AU9999", "asset": "commodity"},         # 黄金（用价格/分位）
    # 其他常用指数（用于估值参考）
    "510500": {"name": "中证500ETF", "index": "000905", "asset": "equity"},
    "512100": {"name": "中证1000ETF", "index": "000852", "asset": "equity"},
    "588000": {"name": "科创50ETF", "index": "000688", "asset": "equity"},
    "159915": {"name": "创业板ETF", "index": "399006", "asset": "equity"},
    "513050": {"name": "中概互联ETF", "index": "H30533", "asset": "equity"},
    "159920": {"name": "恒生ETF", "index": "HSI", "asset": "equity"},             # 恒生指数（可能受限）
    "513500": {"name": "标普500ETF", "index": "SPX", "asset": "equity"},         # 标普500（可能受限）
}

# 板块评分涉及的指数/ETF（用于计算 sectorScores）
SECTOR_MAP = {
    "AI算力":      {"index": "930713", "etf": "159819"},
    "人形机器人":   {"index": "H30532", "etf": "562500"},
    "半导体":      {"index": "H30184", "etf": "512480"},
    "消费电子":    {"index": "930652", "etf": "159732"},
    "港股互联网":  {"index": "H30533", "etf": "513050"},
    "低空经济":    {"index": "931551", "etf": "159232"},
    "新能源":      {"index": "399808", "etf": "516160"},
    "军工":        {"index": "399967", "etf": "512560"},
    "创新药":      {"index": "931152", "etf": "159992"},
    "央企价值":    {"index": "932039", "etf": "560700"},
    "黄金":        {"index": "AU9999", "etf": "518880"},
    "券商":        {"index": "399975", "etf": "512000"},
}

# 腾讯行情映射（ETF 代码 → 腾讯格式）
TX_MAP = {
    '563020': 'sh563020', '159819': 'sz159819', '562500': 'sh562500',
    '512480': 'sh512480', '518880': 'sh518880', '513100': 'sh513100',
    '513500': 'sh513500', '510300': 'sh510300', '510500': 'sh510500',
    '510880': 'sh510880', '510050': 'sh510050', '588000': 'sh588000',
    '159915': 'sz159915', '159920': 'sz159920', '513050': 'sh513050',
    '512100': 'sh512100', '159732': 'sz159732', '516160': 'sh516160',
    '512560': 'sh512560', '159992': 'sz159992', '560700': 'sh560700',
    '512000': 'sh512000', '159232': 'sz159232',
}

# 历史数据天数（用于计算分位、均线、涨跌幅）
HISTORY_DAYS = 250
SECTOR_DAYS = 20

# 自检阈值
PE_VALID_RANGE = (0, 300)           # PE 有效范围
PB_VALID_RANGE = (0, 50)              # PB 有效范围
MAX_DAILY_CHANGE = 0.25             # 日波动告警阈值 25%
MAX_DATA_STALE_DAYS = 1              # 数据过期告警天数
SOURCE_DEVIATION_THRESHOLD = 0.05    # 多源偏差告警阈值 5%

# 内置预存数据（作为降级备用）
PRESTORED_PE = {
    "510300": {"pe": 13.65, "pb": 1.43, "pct": 64.73, "date": "2026-06-12"},
    "930050": {"pe": 14.2, "pb": 1.55, "pct": 45.0, "date": "2026-06-12"},    # 中证A50（估算）
    "930841": {"pe": 8.1, "pb": 0.92, "pct": 78.0, "date": "2026-06-12"},    # 红利低波（估算）
    "930713": {"pe": 52.0, "pb": 4.2, "pct": 65.0, "date": "2026-06-12"},    # 人工智能（估算）
    "H30590": {"pe": 48.0, "pb": 3.8, "pct": 55.0, "date": "2026-06-12"},    # 机器人（估算）
    "930813": {"pe": 75.0, "pb": 5.5, "pct": 72.0, "date": "2026-06-12"},    # 半导体（估算）
    "513100": {"pe": 37.02, "pb": None, "pct": 83.5, "date": "2026-06-12"},
    "513500": {"pe": 26.4, "pb": None, "pct": 86.7, "date": "2026-06-12"},
    "518880": {"price": 9.25, "pct": 75.0, "date": "2026-06-12"},  # 黄金用价格
}

# ==================== 工具函数 ====================

def _today() -> str:
    return datetime.date.today().strftime("%Y-%m-%d")

def _now() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def num(v) -> float:
    try:
        n = float(v)
        return n if math.isfinite(n) else 0.0
    except (TypeError, ValueError):
        return 0.0

def fmt_log(msg: str) -> str:
    return f"[{_now()}] {msg}"

def print_log(msg: str, silent: bool = False):
    if not silent:
        print(fmt_log(msg))

def compute_checksum(data: dict) -> str:
    raw = json.dumps(data.get("peData", []), sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

def compute_percentile(values: List[float], current: float) -> Optional[float]:
    """计算历史分位（0-100）。"""
    if not values or current is None or not math.isfinite(current):
        return None
    valid = [v for v in values if v is not None and v > 0 and math.isfinite(v)]
    if not valid:
        return None
    valid.sort()
    # 使用线性插值法
    n = len(valid)
    if current <= valid[0]:
        return 0.0
    if current >= valid[-1]:
        return 100.0
    # 找到位置
    for i in range(n - 1):
        if valid[i] <= current <= valid[i + 1]:
            return round((i + (current - valid[i]) / (valid[i + 1] - valid[i])) / n * 100, 2)
    return round((sum(1 for v in valid if v <= current) / n) * 100, 2)

def compute_ma(closes: List[float], days: int = 20) -> Optional[float]:
    if not closes or len(closes) < days:
        return None
    return round(sum(closes[-days:]) / days, 4)

def compute_sector_score(closes: List[float], days: int = 20) -> Optional[float]:
    """基于近N日涨跌幅计算板块评分，标准化到 0-1。"""
    if not closes or len(closes) < days + 1:
        return None
    start_price = closes[-days]  # N 天前的收盘价
    end_price = closes[-1]       # 最新收盘价
    if start_price <= 0:
        return None
    change_pct = (end_price - start_price) / start_price
    # 映射到 0-1：假设 -20%~+20% 为合理范围
    score = (change_pct + 0.20) / 0.40
    score = max(0.0, min(1.0, score))
    return round(score, 3)

def compute_gini(values: List[float]) -> Optional[float]:
    """计算Gini系数。"""
    if not values or len(values) < 2 or sum(values) == 0:
        return None
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    cumsum = sum((2 * (i + 1) - n - 1) * v for i, v in enumerate(sorted_vals))
    gini = cumsum / (n * sum(sorted_vals))
    return round(abs(gini), 3)

def stage_from_ts(ts_raw: Optional[float]) -> float:
    if ts_raw is None:
        return 1.5
    if ts_raw >= 70: return 3.0
    if ts_raw >= 50: return 2.0
    if ts_raw >= 30: return 1.5
    if ts_raw >= 10: return 1.0
    return 0.0

def mainline_from_ts(ts_raw: Optional[float]) -> str:
    if ts_raw is None:
        return "—"
    if ts_raw >= 70: return "突破"
    if ts_raw >= 50: return "增强"
    if ts_raw >= 30: return "震荡"
    return "弱势"

# ==================== 数据获取层 ====================

def _try_import_akshare() -> bool:
    try:
        import akshare as ak
        return True
    except ImportError:
        return False

def fetch_akshare_index_history(symbol: str, days: int = 250) -> List[Dict]:
    """使用 akshare 获取指数历史行情。"""
    try:
        import akshare as ak
        # akshare 指数历史接口，需要处理日期格式
        end_date = datetime.date.today().strftime("%Y%m%d")
        start_date = (datetime.date.today() - datetime.timedelta(days=days + 30)).strftime("%Y%m%d")
        
        # 尝试 akshare 的指数历史接口
        df = ak.index_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date)
        if df is None or df.empty:
            return []
        
        records = df.to_dict('records')
        return [
            {
                "date": str(r.get("日期", r.get("date", ""))),
                "close": num(r.get("收盘", r.get("close", 0))),
                "open": num(r.get("开盘", r.get("open", 0))),
                "high": num(r.get("最高", r.get("high", 0))),
                "low": num(r.get("最低", r.get("low", 0))),
                "volume": num(r.get("成交量", r.get("volume", 0))),
            }
            for r in records
        ]
    except Exception as e:
        print_log(f"[WARN] akshare 获取 {symbol} 历史失败: {e}", silent=False)
        return []

def fetch_akshare_index_pe(symbol: str) -> Optional[Dict]:
    """使用 akshare 获取指数最新估值。"""
    try:
        import akshare as ak
        # 优先使用 funddb 估值接口
        try:
            df = ak.index_value_hist_funddb(symbol=symbol)
            if df is not None and not df.empty:
                latest = df.iloc[-1]
                pe = num(latest.get("市盈率", latest.get("PE", latest.get("pe", 0))))
                pb = num(latest.get("市净率", latest.get("PB", latest.get("pb", 0))))
                date_str = str(latest.get("日期", latest.get("date", _today())))
                return {"pe": pe if pe > 0 else None, "pb": pb if pb > 0 else None, "date": date_str}
        except Exception:
            pass
        
        # 备用：使用指数估值名称接口
        try:
            df = ak.index_value_name_funddb(symbol=symbol)
            if df is not None and not df.empty:
                latest = df.iloc[-1]
                pe = num(latest.get("市盈率", latest.get("PE", latest.get("pe", 0))))
                pb = num(latest.get("市净率", latest.get("PB", latest.get("pb", 0))))
                date_str = str(latest.get("日期", latest.get("date", _today())))
                return {"pe": pe if pe > 0 else None, "pb": pb if pb > 0 else None, "date": date_str}
        except Exception:
            pass
        
        return None
    except Exception as e:
        print_log(f"[WARN] akshare 获取 {symbol} PE 失败: {e}", silent=False)
        return None

def fetch_tencent_prices(codes: List[str]) -> Dict[str, float]:
    """通过腾讯接口获取ETF实时价格。"""
    import requests
    tx_map = {c: TX_MAP.get(c, ('sh' if c.startswith('5') else 'sz') + c) for c in codes}
    url = f"https://qt.gtimg.cn/q={','.join(tx_map.values())}"
    try:
        r = requests.get(url, timeout=15)
        r.encoding = 'gbk'
        text = r.text
    except Exception as e:
        print_log(f"[WARN] 腾讯行情请求失败: {e}", silent=False)
        return {}
    
    prices = {}
    for line in text.strip().split(';'):
        if not line.strip():
            continue
        import re
        m = re.match(r'v_([a-z]+(\d+))="([^"]*)"', line)
        if not m:
            continue
        raw_code = m.group(2)
        parts = m.group(3).split('~')
        if len(parts) > 3:
            for c, tx in tx_map.items():
                if tx.endswith(raw_code):
                    prices[c] = float(parts[3])
                    break
    return prices

def fetch_sector_history_akshare(symbol: str, days: int = 60) -> List[float]:
    """获取板块指数历史收盘价列表。"""
    hist = fetch_akshare_index_history(symbol, days=days + 10)
    if not hist:
        return []
    return [h["close"] for h in hist if h.get("close", 0) > 0]

# ==================== 自检修复层 ====================

def validate_pe_record(code: str, pe: Optional[float], pb: Optional[float],
                       pct: Optional[float], data_date: str,
                       yesterday_pe: Optional[float] = None) -> Tuple[str, str]:
    """数据自检：返回 (status, notes)。"""
    today = datetime.date.today()
    
    # 1. PE 范围检查
    if pe is not None and not (PE_VALID_RANGE[0] < pe < PE_VALID_RANGE[1]):
        return "abnormal", f"PE {pe} 超出有效范围 {PE_VALID_RANGE}"
    
    # 2. 数据日期检查
    if data_date:
        try:
            dd = datetime.datetime.strptime(data_date, "%Y-%m-%d").date()
            days_diff = (today - dd).days
            if days_diff > MAX_DATA_STALE_DAYS:
                return "stale", f"数据日期 {data_date}，距今 {days_diff} 天"
        except ValueError:
            pass
    
    # 3. 日波动检查
    if yesterday_pe is not None and pe is not None and yesterday_pe > 0:
        change = abs(pe - yesterday_pe) / yesterday_pe
        if change > MAX_DAILY_CHANGE:
            return "abnormal", f"日波动 {change*100:.1f}% > {MAX_DAILY_CHANGE*100:.0f}%"
    
    return "ok", ""

def validate_commodity(price: Optional[float], pct: Optional[float], data_date: str) -> Tuple[str, str]:
    if price is None or price <= 0:
        return "abnormal", f"price {price} 无效"
    if pct is not None and not (0 <= pct <= 100):
        return "abnormal", f"price_percentile {pct} 超出 [0,100]"
    
    if data_date:
        try:
            dd = datetime.datetime.strptime(data_date, "%Y-%m-%d").date()
            days_diff = (datetime.date.today() - dd).days
            if days_diff > MAX_DATA_STALE_DAYS:
                return "stale", f"数据日期 {data_date}，距今 {days_diff} 天"
        except ValueError:
            pass
    return "ok", ""

# ==================== 主数据构建 ====================

def build_pe_data(silent: bool = False) -> List[Dict]:
    """构建 peData（指数估值数据）。"""
    pe_data = []
    has_ak = _try_import_akshare()
    print_log(f"akshare 可用: {has_ak}", silent=silent)
    
    for code, cfg in INDEX_PE_MAP.items():
        symbol = cfg["index"]
        asset = cfg.get("asset", "equity")
        name = cfg["name"]
        
        print_log(f"抓取 {code} ({name}) ...", silent=silent)
        
        pe = pb = pct = None
        data_date = _today()
        status = "ok"
        notes = ""
        sources = []
        
        if asset == "commodity":
            # 黄金/商品类：用价格代替 PE
            # 尝试获取 ETF 实时价格
            prices = fetch_tencent_prices([code])
            price = prices.get(code)
            
            # 获取历史价格计算分位
            if has_ak:
                hist = fetch_sector_history_akshare(symbol, days=HISTORY_DAYS)
                if hist:
                    pct = compute_percentile(hist, price) if price else None
            
            # 降级到预存数据
            if price is None:
                preset = PRESTORED_PE.get(code, {})
                price = preset.get("price")
                pct = preset.get("pct")
                data_date = preset.get("date", data_date)
                status = "stale" if status == "ok" else status
                sources.append("prestored")
            else:
                sources.append("tencent")
            
            # 自检
            v_status, v_notes = validate_commodity(price, pct, data_date)
            if v_status != "ok":
                status = v_status
                notes = v_notes
            
            record = {
                "code": code,
                "name": name,
                "pe": None,
                "pb": None,
                "price": round(price, 2) if price else None,
                "pct": pct,
                "date": data_date,
                "source": "|".join(sources) if sources else "manual",
                "status": status,
                "notes": notes,
            }
            pe_data.append(record)
            print_log(f"  → price={record.get('price')}, pct={pct}%, status={status}", silent=silent)
            continue
        
        # 权益类：获取 PE/PB/分位
        history = []
        if has_ak:
            # 获取历史行情（用于计算分位）
            history = fetch_akshare_index_history(symbol, days=HISTORY_DAYS)
            # 获取最新估值
            pe_info = fetch_akshare_index_pe(symbol)
            if pe_info:
                pe = pe_info.get("pe")
                pb = pe_info.get("pb")
                data_date = pe_info.get("date", data_date)
                sources.append("akshare")
        
        # 如果 akshare 失败或不可用，使用预存数据
        if pe is None or not math.isfinite(pe) or pe <= 0:
            preset = PRESTORED_PE.get(code, {})
            pe = preset.get("pe")
            pb = preset.get("pb")
            pct = preset.get("pct")
            data_date = preset.get("date", data_date)
            status = "stale" if status == "ok" else status
            sources.append("prestored")
        else:
            # 计算分位
            if history:
                pe_history = [h["close"] for h in history]  # 这里用价格历史，如果需要PE历史需要另外获取
                # 实际上 akshare 的 funddb 接口已经返回分位，这里用价格分位作为近似
                # 或者如果有 PE 历史数据，可以直接计算
                # 简化：如果 akshare 没给分位，我们计算价格分位作为参考
                if pct is None:
                    latest_close = history[-1]["close"] if history else None
                    pct = compute_percentile([h["close"] for h in history], latest_close) if latest_close else None
        
        # 自检
        v_status, v_notes = validate_pe_record(code, pe, pb, pct, data_date)
        if v_status != "ok":
            status = v_status
            notes = v_notes
        
        record = {
            "code": code,
            "name": name,
            "pe": round(pe, 2) if pe and math.isfinite(pe) else None,
            "pb": round(pb, 2) if pb and math.isfinite(pb) else None,
            "pct": pct,
            "date": data_date,
            "source": "|".join(sources) if sources else "manual",
            "status": status,
            "notes": notes,
        }
        pe_data.append(record)
        print_log(f"  → PE={record.get('pe')}, PB={record.get('pb')}, pct={pct}%, status={status}", silent=silent)
    
    return pe_data

def build_sector_scores(silent: bool = False) -> Dict[str, float]:
    """构建板块评分（sectorScores）。"""
    has_ak = _try_import_akshare()
    sector_scores = {}
    
    for sector, cfg in SECTOR_MAP.items():
        symbol = cfg["index"]
        etf = cfg.get("etf")
        
        print_log(f"计算板块评分 {sector} ...", silent=silent)
        
        score = None
        if has_ak:
            history = fetch_sector_history_akshare(symbol, days=SECTOR_DAYS + 5)
            if history and len(history) >= SECTOR_DAYS + 1:
                score = compute_sector_score(history, days=SECTOR_DAYS)
        
        # 如果 akshare 失败，尝试用 ETF 价格涨跌幅
        if score is None and etf:
            # 获取近20日 vs 当前价格
            # 简化：获取当前价格和一个预存的20日前价格（或从预存估算）
            prices = fetch_tencent_prices([etf])
            if prices.get(etf) and PRESTORED_PE.get(etf):
                preset = PRESTORED_PE.get(etf, {})
                # 无法获取历史价格，跳过
                pass
        
        # 降级到预存估算（如果有的话）或给个默认值
        if score is None:
            # 从预存的 sector_scores 中找（如果有）
            preset_scores = {
                "AI算力": 0.733, "人形机器人": 0.588, "半导体": 0.84,
                "消费电子": 0.937, "港股互联网": 0.277, "低空经济": 0.119,
                "新能源": 0.231, "军工": 0.242, "创新药": 0.184,
                "央企价值": 0.415, "黄金": 0.347, "券商": 0.322,
            }
            score = preset_scores.get(sector, 0.5)
            print_log(f"  → 使用预存评分: {score}", silent=silent)
        else:
            print_log(f"  → 评分: {score}", silent=silent)
        
        sector_scores[sector] = score
    
    return sector_scores

def build_weekly_signals(sector_scores: Dict[str, float], silent: bool = False) -> Dict:
    """构建 weeklySignals（市场信号）。"""
    # 从沪深300数据获取 TS 基础
    has_ak = _try_import_akshare()
    ts_raw = None
    
    if has_ak:
        hs300_history = fetch_akshare_index_history("000300", days=HISTORY_DAYS)
        if hs300_history:
            closes = [h["close"] for h in hs300_history if h.get("close", 0) > 0]
            if closes:
                # 用价格分位作为 TS 代理
                ts_raw = compute_percentile(closes, closes[-1])
    
    # 如果没有 TS，从板块评分均值推导
    if ts_raw is None and sector_scores:
        avg_score = sum(sector_scores.values()) / len(sector_scores)
        ts_raw = avg_score * 100  # 映射到 0-100
    
    if ts_raw is None:
        ts_raw = 43.6  # 默认预存值
    
    ts = round(ts_raw / 100, 3) if ts_raw else 0.436
    
    # 计算 MC（主线集中度）
    if sector_scores:
        scores = list(sector_scores.values())
        all_avg = sum(scores) / len(scores)
        top3 = sorted(scores, reverse=True)[:3]
        s_avg = sum(top3) / 3 if top3 else 0
        if 100 - all_avg > 0:
            mc = (s_avg - all_avg) / (100 - all_avg)
        else:
            mc = 0
    else:
        mc = 0.888
    
    mc = round(mc, 3)
    
    # Gini 系数
    gini = compute_gini(list(sector_scores.values())) if sector_scores else None
    
    # Stage
    stage = stage_from_ts(ts_raw)
    
    # MainlineTrend
    mainline = mainline_from_ts(ts_raw)
    
    # S级板块
    s_grade = [s for s, v in sector_scores.items() if v >= 0.85]
    
    # 极端信号
    extreme = {"triggered": False, "flags": []}
    if ts_raw and ts_raw > 85:
        extreme["triggered"] = True
        extreme["flags"].append("TS>85")
    if mc and mc > 0.95:
        extreme["triggered"] = True
        extreme["flags"].append("MC>0.95")
    
    # 突破/均衡模式
    is_breakout = ts_raw >= 70 if ts_raw else False
    is_balanced = 50 <= ts_raw < 70 if ts_raw else False
    
    weekly_signals = {
        "tradeDate": _today(),
        "DATA_TIMESTAMP": datetime.datetime.now().isoformat(),
        "TS": ts,
        "TS_raw": round(ts_raw, 1) if ts_raw else 43.6,
        "MC": mc,
        "GiniCoefficient": gini,
        "MainlineTrend": mainline,
        "stage": stage,
        "isBreakoutMode": is_breakout,
        "isBalancedMode": is_balanced,
        "extremeFlag": extreme,
        "sectorScores": sector_scores,
        "sGradeSectors": s_grade,
        "engineVersion": "4.3-auto",
    }
    
    print_log(f"weeklySignals → TS={ts_raw}, MC={mc}, Stage={stage}, Mainline={mainline}", silent=silent)
    print_log(f"S级板块: {s_grade}", silent=silent)
    
    return weekly_signals

def build_summary(pe_data: List[Dict], weekly_signals: Dict, silent: bool = False) -> str:
    """生成飞书/摘要格式的文本。"""
    today = _today()
    ts_raw = weekly_signals.get("TS_raw", "N/A")
    mc = weekly_signals.get("MC", "N/A")
    stage = weekly_signals.get("stage", "N/A")
    mainline = weekly_signals.get("MainlineTrend", "N/A")
    top3 = weekly_signals.get("sGradeSectors", [])
    
    lines = [
        f"📊 投资作战室市场数据 — {today}",
        "═══════════════════════════════════════",
        "",
        f"📈 市场状态:",
        f"   TS 趋势强度: {ts_raw}",
        f"   MC 主线集中: {mc}",
        f"   市场阶段: Stage {stage} ({mainline})",
        "",
        f"🏆 S级板块: {', '.join(top3) if top3 else '无'}",
        "",
        "📊 估值概览:",
    ]
    
    for p in sorted(pe_data, key=lambda x: x.get("pct") or 0, reverse=True):
        pct = p.get("pct")
        pct_str = f"分位{pct}%" if pct is not None else "分位N/A"
        zone = ""
        if pct is not None:
            if pct > 95: zone = "💥 极端"
            elif pct > 80: zone = "🔴 止盈区"
            elif pct > 60: zone = "🟡 谨慎区"
            elif pct < 30: zone = "🟢 加码区"
            else: zone = "⚪ 中性"
        
        pe = p.get("pe")
        pe_str = f"PE {pe}" if pe is not None else "—"
        lines.append(f"   {p['name']} ({p['code']}): {pe_str}, {pct_str} {zone}")
    
    lines.extend([
        "",
        "📁 输出文件:",
        f"   • market_data_{today}.json",
        "",
        "═══════════════════════════════════════",
    ])
    
    return "\n".join(lines)

# ==================== 主入口 ====================

def main():
    parser = argparse.ArgumentParser(description="投资作战室市场数据抓取")
    parser.add_argument("--cron", action="store_true", help="Cron 静默模式（仅输出文件，不打印日志）")
    parser.add_argument("--pretty", action="store_true", help="终端输出格式化 JSON")
    parser.add_argument("--summary", action="store_true", help="仅输出摘要文本")
    parser.add_argument("--output", type=str, default=".", help="输出目录")
    args = parser.parse_args()
    
    silent = args.cron
    output_dir = args.output
    os.makedirs(output_dir, exist_ok=True)
    
    if not silent:
        print("=" * 60)
        print("投资作战室 · 市场数据抓取 v2.0")
        print(f"日期: {_today()}")
        print("=" * 60)
    
    # 1. 构建 PE 数据
    pe_data = build_pe_data(silent=silent)
    
    # 2. 构建板块评分
    sector_scores = build_sector_scores(silent=silent)
    
    # 3. 构建市场信号
    weekly_signals = build_weekly_signals(sector_scores, silent=silent)
    
    # 4. 组装输出
    output = {
        "peData": pe_data,
        "weeklySignals": weekly_signals,
        "generatedAt": datetime.datetime.now().isoformat(),
        "generator": "warroom_market_fetch_v2.py",
        "checksum": compute_checksum({"peData": pe_data}),
    }
    
    # 5. 保存文件
    filename = f"market_data_{_today()}.json"
    filepath = os.path.join(output_dir, filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    if not silent:
        print(f"\n[OK] 数据已保存: {os.path.abspath(filepath)}")
    
    # 6. 输出格式化 JSON 到终端（供复制粘贴）
    if args.pretty and not silent:
        print("\n" + "=" * 60)
        print("可直接复制粘贴到作战室「数据管理」的 JSON:")
        print("=" * 60)
        print(json.dumps(output, ensure_ascii=False, indent=2))
    
    # 7. 输出摘要
    if args.summary and not silent:
        print("\n" + "=" * 60)
        print(build_summary(pe_data, weekly_signals, silent=silent))
    
    # Cron 模式下输出文件路径到 stdout（方便后续脚本处理）
    if silent:
        print(filepath)
    
    return filepath

if __name__ == "__main__":
    main()

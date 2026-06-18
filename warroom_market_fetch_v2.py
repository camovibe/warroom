#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
投资作战室 · 市场数据抓取脚本 v2.2（纯腾讯行情版）
===========================
用途：每日抓取指数估值 + 市场信号，使用腾讯行情实时涨跌幅计算动态板块评分。

运行环境：Python 3.8+，优先 akshare（pip install akshare），降级 requests
运行方式：
  python warroom_market_fetch_v2.py          # 交互模式，输出文件
  python warroom_market_fetch_v2.py --cron   # 静默模式，适合定时任务
  python warroom_market_fetch_v2.py --pretty # 终端输出格式化 JSON

输出文件：market_data_YYYY-MM-DD.json

作者：Kimi Work v2.2（纯腾讯行情，无需 akshare 历史数据）
"""

import json, math, os, sys, argparse, hashlib, warnings, datetime, re
from decimal import Decimal
from typing import List, Dict, Any, Optional, Tuple

warnings.filterwarnings('ignore')

# ==================== 配置区 ====================

INDEX_PE_MAP = {
    "930841": {"name": "中证红利低波动", "index": "930841", "asset": "equity"},
    "930050": {"name": "中证A50", "index": "930050", "asset": "equity"},
    "510300": {"name": "沪深300", "index": "000300", "asset": "equity"},
    "930713": {"name": "中证人工智能", "index": "930713", "asset": "equity"},
    "H30590": {"name": "中证机器人", "index": "H30590", "asset": "equity"},
    "930813": {"name": "中华半导体芯片", "index": "930813", "asset": "equity"},
    "513100": {"name": "纳指ETF", "index": "NDX", "asset": "equity"},
    "518880": {"name": "黄金ETF", "index": "AU9999", "asset": "commodity"},
    "510500": {"name": "中证500ETF", "index": "000905", "asset": "equity"},
    "512100": {"name": "中证1000ETF", "index": "000852", "asset": "equity"},
    "588000": {"name": "科创50ETF", "index": "000688", "asset": "equity"},
    "159915": {"name": "创业板ETF", "index": "399006", "asset": "equity"},
    "513050": {"name": "中概互联ETF", "index": "H30533", "asset": "equity"},
    "159920": {"name": "恒生ETF", "index": "HSI", "asset": "equity"},
    "513500": {"name": "标普500ETF", "index": "SPX", "asset": "equity"},
}

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

HISTORY_DAYS = 250
SECTOR_DAYS = 20

PE_VALID_RANGE = (0, 300)
PB_VALID_RANGE = (0, 50)
MAX_DAILY_CHANGE = 0.25
MAX_DATA_STALE_DAYS = 1

PRESTORED_PE = {
    "510300": {"pe": 13.65, "pb": 1.43, "pct": 64.73, "date": "2026-06-12"},
    "930050": {"pe": 14.2, "pb": 1.55, "pct": 45.0, "date": "2026-06-12"},
    "930841": {"pe": 8.1, "pb": 0.92, "pct": 78.0, "date": "2026-06-12"},
    "930713": {"pe": 52.0, "pb": 4.2, "pct": 65.0, "date": "2026-06-12"},
    "H30590": {"pe": 48.0, "pb": 3.8, "pct": 55.0, "date": "2026-06-12"},
    "930813": {"pe": 75.0, "pb": 5.5, "pct": 72.0, "date": "2026-06-12"},
    "513100": {"pe": 37.02, "pb": None, "pct": 83.5, "date": "2026-06-12"},
    "513500": {"pe": 26.4, "pb": None, "pct": 86.7, "date": "2026-06-12"},
    "518880": {"price": 9.25, "pct": 75.0, "date": "2026-06-12"},
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

def compute_percentile(values: List[float], current: float) -> Optional[float]:
    if not values or current is None or not math.isfinite(current):
        return None
    valid = [v for v in values if v is not None and v > 0 and math.isfinite(v)]
    if not valid:
        return None
    valid.sort()
    n = len(valid)
    if current <= valid[0]:
        return 0.0
    if current >= valid[-1]:
        return 100.0
    for i in range(n - 1):
        if valid[i] <= current <= valid[i + 1]:
            return round((i + (current - valid[i]) / (valid[i + 1] - valid[i])) / n * 100, 2)
    return round((sum(1 for v in valid if v <= current) / n) * 100, 2)

def compute_sector_score_from_change(change_pct: float) -> float:
    """基于涨跌幅映射到 0-1 评分。日涨跌幅 -3%~+3% 映射到 0-1。"""
    score = (change_pct + 0.03) / 0.06
    return round(max(0.0, min(1.0, score)), 3)

def compute_gini(values: List[float]) -> Optional[float]:
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
    try:
        import akshare as ak
        end_date = datetime.date.today().strftime("%Y%m%d")
        start_date = (datetime.date.today() - datetime.timedelta(days=days + 30)).strftime("%Y%m%d")
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
    try:
        import akshare as ak
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

# ========== 核心修复：fetch_tencent_prices 同时获取日涨跌幅 ==========

def fetch_tencent_prices_with_change(codes: List[str]) -> Dict[str, Dict]:
    """通过腾讯接口获取ETF实时价格 + 日涨跌幅。
    
    返回格式：{code: {"price": x, "prev_close": y, "change_pct": z}}
    """
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
    
    result = {}
    for line in text.strip().split(';'):
        if not line.strip():
            continue
        m = re.match(r'v_([a-z]+(\d+))="([^"]*)"', line)
        if not m:
            continue
        raw_code = m.group(2)
        parts = m.group(3).split('~')
        # 腾讯接口格式：~名称~代码~当前价~昨收~今开~...
        # 关键修复：len(parts) > 4 即可获取 price(parts[3]) 和 prev_close(parts[4])
        if len(parts) > 4:
            for c, tx in tx_map.items():
                if tx.endswith(raw_code):
                    try:
                        price = float(parts[3])      # 当前价
                        prev_close = float(parts[4])  # 昨收
                        change_pct = (price - prev_close) / prev_close if prev_close > 0 else 0
                        result[c] = {
                            "price": price,
                            "prev_close": prev_close,
                            "change_pct": change_pct,
                        }
                    except (ValueError, IndexError, ZeroDivisionError) as e:
                        # 调试：打印异常但不中断
                        print(f"[DEBUG] {c}({tx}) 解析失败: {e}, parts={parts[:6]}")
                    break
    # 调试：强制打印获取结果数量（即使 silent 模式）
    print(f"[DEBUG] 腾讯行情最终获取到 {len(result)} 只 ETF: {list(result.keys())}")
    return result

# 保留旧接口兼容
def fetch_tencent_prices(codes: List[str]) -> Dict[str, float]:
    info = fetch_tencent_prices_with_change(codes)
    return {k: v["price"] for k, v in info.items()}

def fetch_sector_history_akshare(symbol: str, days: int = 60) -> List[float]:
    hist = fetch_akshare_index_history(symbol, days=days + 10)
    if not hist:
        return []
    return [h["close"] for h in hist if h.get("close", 0) > 0]

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
            prices = fetch_tencent_prices_with_change([code])
            price_info = prices.get(code)
            price = price_info["price"] if price_info else None
            
            if has_ak:
                hist = fetch_sector_history_akshare(symbol, days=HISTORY_DAYS)
                if hist:
                    pct = compute_percentile(hist, price) if price else None
            
            if price is None:
                preset = PRESTORED_PE.get(code, {})
                price = preset.get("price")
                pct = preset.get("pct")
                data_date = preset.get("date", data_date)
                status = "stale"
                sources.append("prestored")
            else:
                sources.append("tencent")
            
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
        
        history = []
        if has_ak:
            history = fetch_akshare_index_history(symbol, days=HISTORY_DAYS)
            pe_info = fetch_akshare_index_pe(symbol)
            if pe_info:
                pe = pe_info.get("pe")
                pb = pe_info.get("pb")
                data_date = pe_info.get("date", data_date)
                sources.append("akshare")
        
        if pe is None or not math.isfinite(pe) or pe <= 0:
            preset = PRESTORED_PE.get(code, {})
            pe = preset.get("pe")
            pb = preset.get("pb")
            pct = preset.get("pct")
            data_date = _today()
            status = "stale"
            sources.append("prestored")
        else:
            if history:
                latest_close = history[-1]["close"] if history else None
                if pct is None:
                    pct = compute_percentile([h["close"] for h in history], latest_close) if latest_close else None
        
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


# ========== 核心修复：build_sector_scores 使用腾讯日涨跌幅 ==========

def build_sector_scores(silent: bool = False) -> Dict[str, float]:
    """构建板块评分（sectorScores）——核心修复：使用腾讯行情日涨跌幅，无需 akshare 历史数据。"""
    sector_scores = {}
    has_ak = _try_import_akshare()
    
    # 获取所有 ETF 的当前价格 + 日涨跌幅（腾讯行情）
    all_etfs = [cfg.get("etf") for cfg in SECTOR_MAP.values() if cfg.get("etf")]
    price_info = fetch_tencent_prices_with_change(all_etfs) if all_etfs else {}
    # 强制打印调试信息（即使 silent 模式）
    print(f"[DEBUG] build_sector_scores: 请求 {len(all_etfs)} 只 ETF, 获取到 {len(price_info)} 只")
    
    # 获取历史数据（备用，如果 akshare 可用）
    history_map = {}
    if has_ak:
        for sector, cfg in SECTOR_MAP.items():
            symbol = cfg["index"]
            hist = fetch_sector_history_akshare(symbol, days=SECTOR_DAYS + 5)
            if hist and len(hist) >= SECTOR_DAYS:
                history_map[sector] = hist
        print_log(f"akshare 获取到 {len(history_map)} 个板块历史数据", silent=silent)
    
    for sector, cfg in SECTOR_MAP.items():
        etf = cfg.get("etf")
        score = None
        
        # 方案1：腾讯行情日涨跌幅（最优先，无需历史数据）
        if etf and etf in price_info:
            change_pct = price_info[etf]["change_pct"]
            score = compute_sector_score_from_change(change_pct)
            print_log(f"  → {sector}: 腾讯日涨跌 {change_pct*100:.2f}%, 评分 {score}", silent=silent)
        
        # 方案2：历史数据 MA 对比（akshare 备用）
        if score is None and sector in history_map:
            hist = history_map[sector]
            if len(hist) >= SECTOR_DAYS + 1:
                start_price = hist[-SECTOR_DAYS]
                end_price = hist[-1]
                if start_price > 0:
                    change_pct = (end_price - start_price) / start_price
                    score = compute_sector_score_from_change(change_pct)
                    print_log(f"  → {sector}: 历史对比评分 {score}", silent=silent)
        
        # 方案3：降级预存（极少触发）
        if score is None:
            preset_scores = {
                "AI算力": 0.733, "人形机器人": 0.588, "半导体": 0.84,
                "消费电子": 0.937, "港股互联网": 0.277, "低空经济": 0.119,
                "新能源": 0.231, "军工": 0.242, "创新药": 0.184,
                "央企价值": 0.415, "黄金": 0.347, "券商": 0.322,
            }
            score = preset_scores.get(sector, 0.5)
            print_log(f"  → {sector}: 预存评分 {score}（无数据）", silent=silent)
        
        sector_scores[sector] = score
    
    return sector_scores


# ========== 核心修复：build_weekly_signals 使用腾讯日涨跌幅 ==========

def build_weekly_signals(sector_scores: Dict[str, float], silent: bool = False) -> Dict:
    """构建 weeklySignals（市场信号）——核心修复：用腾讯行情沪深300日涨跌幅计算动态 TS。"""
    # 获取沪深300的实时价格 + 日涨跌幅
    hs300_info = fetch_tencent_prices_with_change(["510300"])
    hs300_data = hs300_info.get("510300")
    hs300_price = hs300_data["price"] if hs300_data else None
    hs300_change = hs300_data["change_pct"] if hs300_data else None
    
    ts_raw = None
    has_ak = _try_import_akshare()
    
    # 方案1：akshare 历史分位（优先）
    if has_ak:
        hs300_history = fetch_akshare_index_history("000300", days=HISTORY_DAYS)
        if hs300_history:
            closes = [h["close"] for h in hs300_history if h.get("close", 0) > 0]
            if closes:
                if hs300_price:
                    closes[-1] = hs300_price
                ts_raw = compute_percentile(closes, closes[-1])
    
    # 方案2：基于日涨跌幅映射（核心修复，无需 akshare）
    if ts_raw is None and hs300_change is not None:
        # 日涨跌幅映射到 TS_raw (0-100)
        # -3% → 10, 0% → 43.6, +3% → 77
        ts_raw = 43.6 + hs300_change * 1000
        ts_raw = max(0, min(100, ts_raw))
        print_log(f"沪深300 日涨跌 {hs300_change*100:.2f}%, 映射 TS_raw={ts_raw}", silent=silent)
    
    # 方案3：基于板块评分均值
    if ts_raw is None and sector_scores:
        avg_score = sum(sector_scores.values()) / len(sector_scores)
        ts_raw = avg_score * 100
    
    # 方案4：默认
    if ts_raw is None:
        ts_raw = 43.6
    
    ts = round(ts_raw / 100, 3) if ts_raw else 0.436
    
    # 计算 MC（主线集中度）——修复：分母从 100 改为 1
    if sector_scores:
        scores = list(sector_scores.values())
        all_avg = sum(scores) / len(scores)
        top3 = sorted(scores, reverse=True)[:3]
        s_avg = sum(top3) / 3 if top3 else 0
        # 关键修复：分母改为 (1 - all_avg) 而非 (100 - all_avg)
        denominator = max(1 - all_avg, 0.001)
        mc = (s_avg - all_avg) / denominator
        mc = max(0, min(1, mc))  # 限制在 0-1
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
        "engineVersion": "4.3-auto-v2.2",
    }
    
    print_log(f"weeklySignals → TS_raw={ts_raw}, MC={mc}, Stage={stage}, Mainline={mainline}", silent=silent)
    print_log(f"S级板块: {s_grade}", silent=silent)
    
    return weekly_signals


def build_summary(pe_data: List[Dict], weekly_signals: Dict, silent: bool = False) -> str:
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
        print("投资作战室 · 市场数据抓取 v2.2")
        print(f"日期: {_today()}")
        print("=" * 60)
    
    # 1. 构建 PE 数据
    pe_data = build_pe_data(silent=silent)
    
    # 2. 构建板块评分（核心：使用腾讯行情日涨跌幅，无需历史数据）
    sector_scores = build_sector_scores(silent=silent)
    
    # 3. 构建市场信号
    weekly_signals = build_weekly_signals(sector_scores, silent=silent)
    
    # 4. 组装输出
    output = {
        "peData": pe_data,
        "weeklySignals": weekly_signals,
        "generatedAt": datetime.datetime.now().isoformat(),
        "generator": "warroom_market_fetch_v2.py v2.2",
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
    
    # Cron 模式下输出文件路径到 stdout
    if silent:
        print(filepath)
    
    return filepath

if __name__ == "__main__":
    main()

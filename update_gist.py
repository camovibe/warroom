#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
更新 GitHub Gist（GitHub Actions 配套脚本）
用法：python update_gist.py --market-file market_data_YYYY-MM-DD.json
流程：
1. 从 Gist 拉取现有的 portfolio.json（保留 holdings + monthlyTargets）
2. 合并新的 market_data（peData + weeklySignals）
3. 推送回 Gist
"""

import sys, os, glob, json, argparse, urllib.request

def fetch_gist(token, gist_id):
    url = f"https://api.github.com/gists/{gist_id}"
    req = urllib.request.Request(url)
    req.add_header('Authorization', f'Bearer {token}')
    req.add_header('Accept', 'application/vnd.github+json')
    try:
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode())
            files = data.get('files', {})
            pf = files.get('portfolio.json', {})
            content = pf.get('content', '{}')
            return json.loads(content)
    except Exception as e:
        print(f'⚠️ 拉取现有 Gist 失败: {e}，将创建新文件')
        return {}

def update_gist(token, gist_id, content):
    url = f"https://api.github.com/gists/{gist_id}"
    data = json.dumps({
        "files": {
            "portfolio.json": {
                "content": json.dumps(content, ensure_ascii=False, indent=2)
            }
        }
    }).encode('utf-8')
    req = urllib.request.Request(url, data=data, method='PATCH')
    req.add_header('Authorization', f'Bearer {token}')
    req.add_header('Content-Type', 'application/json')
    req.add_header('Accept', 'application/vnd.github+json')
    try:
        with urllib.request.urlopen(req) as resp:
            print(f'✅ Gist updated: HTTP {resp.status}')
            return True
    except Exception as e:
        print(f'❌ 推送失败: {e}')
        return False

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--market-file', required=True, help='market data JSON 文件路径')
    args = parser.parse_args()
    
    token = os.environ.get('GIST_TOKEN')
    gist_id = os.environ.get('GIST_ID')
    if not token or not gist_id:
        print('❌ 环境变量 GIST_TOKEN 或 GIST_ID 未设置')
        sys.exit(1)
    
    files = glob.glob(args.market_file)
    if not files:
        print(f'❌ 未找到文件: {args.market_file}')
        sys.exit(1)
    latest = max(files, key=os.path.getmtime)
    
    with open(latest, 'r', encoding='utf-8') as f:
        market_data = json.load(f)
    
    print(f'📥 拉取现有 Gist...')
    portfolio = fetch_gist(token, gist_id)
    
    # 合并：保留 holdings/customFunds/monthlyTargets/lastSaved，替换 peData/weeklySignals
    if 'peData' in market_data:
        portfolio['peData'] = market_data['peData']
    if 'weeklySignals' in market_data:
        portfolio['weeklySignals'] = market_data['weeklySignals']
    
    # 保留原有字段（如果不存在则补空）
    for key in ['holdings', 'customFunds', 'transactions', 'monthlyTargets', 'lastSaved']:
        if key not in portfolio:
            portfolio[key] = {} if key == 'monthlyTargets' else ([] if key != 'lastSaved' else None)
    
    print(f'📤 推送合并后的数据到 Gist...')
    success = update_gist(token, gist_id, portfolio)
    if not success:
        sys.exit(1)

if __name__ == '__main__':
    main()

"""Offline ladder calculator, extracted from the deployed dashboard path rules.

This module has no UI, Redis, Neo4j, or market API dependency. Raw themes are
preserved; the store maintains effective-dated promotion-chain assignments.
"""
from __future__ import annotations
import bisect, datetime, math, re, sqlite3, hashlib
from pathlib import Path
from decimal import Decimal, InvalidOperation
from contextlib import closing
from coreClient._paths import database_root

def is_closed_limit_up(row):
    """Use unadjusted daily close and the exchange limit price for board counts."""
    close, limit_price = row["close"], row["up_limit"]
    return close is not None and limit_price is not None and float(limit_price)>0 and abs(float(close)-float(limit_price))<0.005

def consecutive_board_history(rows, initial_counts=None):
    """Count consecutive traded sessions; a suspended session adds no board and preserves the run."""
    counts=dict(initial_counts or {}); by_day={}
    for row in rows:
        code=row["ts_code"]; day=str(row["trade_date"])
        counts[code]=counts.get(code,0)+1 if is_closed_limit_up(row) else 0
        if counts[code]: by_day.setdefault(day,{})[code]=counts[code]
    return by_day

def ladder_board_pattern(raw,consecutive):
    '''The ladder uses the board total in KPL multi-session labels; other views do not.'''
    text=str(raw or '').strip()
    match=re.fullmatch(r'(\d+)天(\d+)板',text)
    if match:
        span,total=map(int,match.groups())
        if 1<total<=span:return total,span,text
    number=max(1,int(consecutive or 1))
    return number,number,f'{number}连板' if number>1 else '首板'

def classify_limitup_traits(stock):
    """Classify a sealed limit-up using same-day raw prices and final seal facts."""
    def number(key):
        try:
            raw=stock.get(key)
            if raw is None or isinstance(raw,bool): return None
            value=Decimal(str(raw))
            return value if value.is_finite() else None
        except (InvalidOperation,ValueError,TypeError): return None
    def same_price(a,b):
        return a is not None and b is not None and abs(a-b)<Decimal("0.005")
    def seal_seconds(value):
        clock=str(value or "").strip()
        if re.fullmatch(r"\d{5,6}",clock):
            clock=clock.zfill(6); clock=f"{clock[:2]}:{clock[2:4]}:{clock[4:]}"
        match=re.fullmatch(r"(\d{1,2}):(\d{2})(?::(\d{2}))?",clock)
        if not match: return None
        hour,minute,second=(int(part or 0) for part in match.groups())
        return hour*3600+minute*60+second if 0<=hour<24 and 0<=minute<60 and 0<=second<60 else None
    traits={"reversal_shape":None,"board_strength":None}
    upper,lower,close,low,preclose=(number(key) for key in ("up_limit","down_limit","daily_close","daily_low","pre_close"))
    if upper is None or upper<=0 or not same_price(close,upper): return traits
    if lower is not None and upper>lower>0 and same_price(low,lower):
        traits["reversal_shape"]="earth_sky"
    elif low is not None and preclose is not None and preclose>0 and low*100<=preclose*93:
        traits["reversal_shape"]="near_earth_sky"
    if stock.get("board_shape")=="one_word":
        traits["board_strength"]="strong"
        return traits
    openings=number("open_times")
    if openings is not None and (openings<0 or openings!=openings.to_integral_value()): openings=None
    last=seal_seconds(stock.get("last_time"))
    if (openings is not None and openings>3) or (last is not None and last>14*3600):
        traits["board_strength"]="weak"
    elif openings is not None and openings<=1 and last is not None and last<10*3600+30*60:
        traits["board_strength"]="strong"
    return traits

class LadderCalculator:
    def __init__(self, basic_db=None, kpl_db=None):
        root=database_root()/"offline"
        self.basic_db=Path(basic_db or root/"db_cn_basic.db")
        self.kpl_db=Path(kpl_db or root/"db_cn_kpl.db")
        for path in (self.basic_db,self.kpl_db):
            if not path.is_file():raise FileNotFoundError(f'Required source database does not exist: {path}')
        self._dates=None

    def market_data_dates(self):
        if self._dates is None:
            dates,complete=self._read_market_data_dates()
            if not complete: raise RuntimeError("Source dates are not readable")
            self._dates=dates
        return self._dates

    def _read_market_data_dates(self):
        """Dates are driven by the available limit-up market snapshots, not graph attribution."""
        dates = set()
        complete = True
        with closing(sqlite3.connect(f'file:{self.kpl_db}?mode=ro', uri=True, timeout=3)) as c:
            tables = {r[0] for r in c.execute("select name from sqlite_master where type='table'")}
            if 'tbl_cn_kpl_list' in tables:
                dates.update((str(r[0]) for r in c.execute("SELECT DISTINCT trade_date FROM tbl_cn_kpl_list WHERE tag='涨停' AND trade_date IS NOT NULL")))
            if 'tbl_cn_kpl_limit_performance' in tables:
                dates.update((str(r[0]) for r in c.execute('SELECT DISTINCT trade_date FROM tbl_cn_kpl_limit_performance WHERE trade_date IS NOT NULL')))
            if 'tbl_cn_limit_list' in tables:
                dates.update((str(r[0]) for r in c.execute('SELECT DISTINCT trade_date FROM tbl_cn_limit_list WHERE "limit"=\'U\' AND trade_date IS NOT NULL')))
        dates = {day for day in dates if re.fullmatch('\\d{8}', day)}
        if self.basic_db.exists():
            try:
                with closing(sqlite3.connect(f'file:{self.basic_db}?mode=ro', uri=True, timeout=3)) as c:
                    tables = {r[0] for r in c.execute("select name from sqlite_master where type='table'")}
                    calendar_table = 'tbl_cn_stk_limit' if 'tbl_cn_stk_limit' in tables else 'tbl_cn_day' if 'tbl_cn_day' in tables else None
                    if calendar_table:
                        candidates = sorted(dates)
                        valid_days = set()
                        for offset in range(0, len(candidates), 500):
                            batch = candidates[offset:offset + 500]
                            values = ','.join(('(?)' for _ in batch))
                            valid_days.update((str(row[0]) for row in c.execute(f'WITH candidates(trade_date) AS (VALUES {values}) SELECT trade_date FROM candidates WHERE EXISTS (SELECT 1 FROM {calendar_table} actual WHERE actual.trade_date=candidates.trade_date)', batch)))
                        dates.intersection_update(valid_days)
            except Exception as exc:
                complete = False
                print(f'[review] trading-date filter unavailable: {type(exc).__name__}: {exc}', flush=True)
        return (sorted(dates), complete)

    def calculate(self, end_date, sessions=5, live=None):
        """Actual sealed nodes only, with dated promotion/recovery paths through the cutoff."""
        dates = [day for day in self.market_data_dates() if day <= end_date][-sessions:]
        if live and live.get('available'):
            dates = sorted(set(dates) | {live['date']})[-sessions:]
        if not dates:
            return {'date': end_date, 'dates': [], 'nodes': [], 'links': [], 'themes': [], 'sealedStocks': [], 'contextStocks': []}
        with closing(sqlite3.connect(f'file:{self.basic_db}?mode=ro', uri=True, timeout=4)) as c:
            calendar_days = [row[0] for row in c.execute("SELECT cal_date FROM tbl_cn_tradecal WHERE exchange='SSE' AND is_open=1 AND cal_date<=? ORDER BY cal_date", (end_date,))]
        calendar_days = sorted(set(calendar_days) | set(dates))
        day_index = {day: i for i, day in enumerate(calendar_days)}
        first_index = day_index[dates[0]]
        prior_day = calendar_days[first_index - 1] if first_index else None
        pool_days = sorted(set(dates) | {calendar_days[day_index[day] - 1] for day in dates if day_index[day] > 0})
        marks = ','.join(('?' for _ in pool_days))
        pools = {day: {} for day in pool_days}
        with closing(sqlite3.connect(f'file:{self.kpl_db}?mode=ro', uri=True, timeout=4)) as c:
            c.row_factory = sqlite3.Row
            for row in c.execute(f'SELECT trade_date,ts_code,name,theme,lu_time,close_price,board_period,pct_chg FROM tbl_cn_kpl_limit_performance WHERE trade_date IN ({marks})', pool_days):
                item = dict(row)
                item['theme'] = str(item['theme'] or '').strip() or '暂无题材'
                pools[item['trade_date']][item['ts_code']] = item
            for row in c.execute(f"SELECT trade_date,ts_code,name,lu_time FROM tbl_cn_kpl_list WHERE trade_date IN ({marks}) AND tag='涨停'", pool_days):
                item = dict(row)
                pools[item['trade_date']].setdefault(item['ts_code'], {**item, 'theme': '暂无题材', 'close_price': None, 'board_period': ''})
            for row in c.execute(f"""SELECT trade_date,ts_code,name,first_time AS lu_time,close AS close_price FROM tbl_cn_limit_list WHERE trade_date IN ({marks}) AND "limit"='U' """, pool_days):
                item = dict(row)
                pools[item['trade_date']].setdefault(item['ts_code'], {**item, 'theme': '暂无题材', 'board_period': ''})
        if live:
            pools[live['date']] = dict(live['pool'])
        codes = sorted({code for pool in pools.values() for code in pool})
        previous_dates = {day: calendar_days[day_index[day] - 1] if day_index[day] else None for day in dates}
        if not codes:
            return {'date': end_date, 'dates': dates, 'nodes': [], 'links': [], 'themes': [], 'sealedStocks': [], 'contextStocks': [], 'previousDates': previous_dates}
        max_span = max([90] + [int(match.group(1)) for pool in pools.values() for item in pool.values() if (match := re.fullmatch('(\\d+)天(\\d+)板', str(item.get('board_period') or '').strip()))])
        history_start = calendar_days[max(0, first_index - max_span - 1)]
        code_marks = ','.join(('?' for _ in codes))
        with closing(sqlite3.connect(f'file:{self.basic_db}?mode=ro', uri=True, timeout=4)) as c:
            c.row_factory = sqlite3.Row
            history = list(c.execute(f'SELECT d.trade_date,d.ts_code,d.close,d.pct_chg,l.up_limit,d.open AS daily_open,d.high AS daily_high,d.low AS daily_low,d.close AS daily_close,d.pre_close,l.down_limit FROM tbl_cn_day d LEFT JOIN tbl_cn_stk_limit l ON l.ts_code=d.ts_code AND l.trade_date=d.trade_date WHERE d.trade_date BETWEEN ? AND ? AND d.ts_code IN ({code_marks})', [history_start, end_date, *codes]))
            if live:
                history = [row for row in history if row['trade_date'] != live['date']] + [row for code, row in live['rows'].items() if code in codes]
            recent_history = sorted(history, key=lambda row: (row['trade_date'], row['ts_code']))
            first_rows = {}
            first_events = {}
            for row in recent_history:
                code = row['ts_code']
                first_rows.setdefault(code, row)
                if is_closed_limit_up(row):
                    first_events.setdefault(code, row['trade_date'])
            recent_initial = {}
            for code, first in first_rows.items():
                if not is_closed_limit_up(first):
                    continue
                n = 0
                for previous in c.execute('SELECT d.close,l.up_limit FROM tbl_cn_day d LEFT JOIN tbl_cn_stk_limit l ON l.ts_code=d.ts_code AND l.trade_date=d.trade_date WHERE d.ts_code=? AND d.trade_date<? ORDER BY d.trade_date DESC', (code, first['trade_date'])):
                    if not is_closed_limit_up(previous):
                        break
                    n += 1
                recent_initial[code] = n
            counts = consecutive_board_history(recent_history, recent_initial)
            recent_daily = {(row['trade_date'], row['ts_code']): row for row in recent_history}
            for day, pool in pools.items():
                for code in pool:
                    if (day, code) not in recent_daily and day < first_events.get(code, '99999999'):
                        first_events[code] = day
            anchor_codes = []
            for code, first_day in first_events.items():
                item = pools.get(first_day, {}).get(code)
                if item is None:
                    continue
                raw = recent_daily.get((first_day, code))
                board, _, _ = ladder_board_pattern(item.get('board_period'), counts.get(first_day, {}).get(code, 1))
                if raw is None or board > 1:
                    anchor_codes.append(code)
            history = list(recent_history)
            for code in sorted(anchor_codes):
                earlier = c.execute('SELECT d.trade_date,d.ts_code,d.close,d.pct_chg,l.up_limit,d.open AS daily_open,d.high AS daily_high,d.low AS daily_low,d.close AS daily_close,d.pre_close,l.down_limit FROM tbl_cn_day d JOIN tbl_cn_stk_limit l ON l.ts_code=d.ts_code AND l.trade_date=d.trade_date WHERE d.ts_code=? AND d.trade_date<? AND abs(d.close-l.up_limit)<.005 ORDER BY d.trade_date DESC LIMIT 1', (code, history_start)).fetchone()
                if earlier is None:
                    continue
                history.append(earlier)
                n = 0
                for previous in c.execute('SELECT d.close,l.up_limit FROM tbl_cn_day d LEFT JOIN tbl_cn_stk_limit l ON l.ts_code=d.ts_code AND l.trade_date=d.trade_date WHERE d.ts_code=? AND d.trade_date<? ORDER BY d.trade_date DESC', (code, earlier['trade_date'])):
                    if not is_closed_limit_up(previous):
                        break
                    n += 1
                counts.setdefault(earlier['trade_date'], {})[code] = n + 1
            history.sort(key=lambda row: (row['trade_date'], row['ts_code']))
            daily = {(row['trade_date'], row['ts_code']): row for row in history}
            earliest = min((row['trade_date'] for row in history)) if history else history_start
            suspended = {(row[0], row[1]) for row in c.execute(f"SELECT trade_date,ts_code FROM tbl_cn_suspend WHERE trade_date BETWEEN ? AND ? AND ts_code IN ({code_marks}) AND suspend_type='S' AND coalesce(suspend_timing,'')=''", [earliest, end_date, *codes])}
            limits = {(row[0], row[1]): row[2] for row in c.execute(f'SELECT trade_date,ts_code,up_limit FROM tbl_cn_stk_limit WHERE trade_date IN ({marks})', pool_days)}
        meta = {}
        seal_facts = {}
        with closing(sqlite3.connect(f'file:{self.kpl_db}?mode=ro', uri=True, timeout=4)) as c:
            c.row_factory = sqlite3.Row
            for row in c.execute(f'SELECT trade_date,ts_code,name,theme,lu_time,close_price,board_period,pct_chg FROM tbl_cn_kpl_limit_performance WHERE trade_date BETWEEN ? AND ? AND ts_code IN ({code_marks})', [earliest, end_date, *codes]):
                item = dict(row)
                item['theme'] = str(item['theme'] or '').strip() or '暂无题材'
                meta[item['trade_date'], item['ts_code']] = item
            for row in c.execute(f"""SELECT trade_date,ts_code,last_time,open_times,pct_chg FROM tbl_cn_limit_list WHERE trade_date BETWEEN ? AND ? AND ts_code IN ({code_marks}) AND "limit"='U' """, [earliest, end_date, *codes]):
                item = dict(row)
                seal_facts[item['trade_date'], item['ts_code']] = {'last_time': item['last_time'], 'open_times': item['open_times'], 'limit_pct_chg': item['pct_chg']}
            for row in c.execute(f"SELECT trade_date,ts_code,last_time,pct_chg FROM tbl_cn_kpl_list WHERE trade_date BETWEEN ? AND ? AND ts_code IN ({code_marks}) AND tag='涨停' ", [earliest, end_date, *codes]):
                item = dict(row)
                facts = seal_facts.setdefault((item['trade_date'], item['ts_code']), {})
                if not facts.get('last_time'):
                    facts['last_time'] = item['last_time']
                facts['kpl_pct_chg'] = item['pct_chg']
        if live:
            for code, item in live['pool'].items():
                meta[live['date'], code] = item

        def clock(raw):
            raw = str(raw or '').strip()
            if raw.isdigit() and len(raw) >= 10:
                try:
                    return datetime.datetime.fromtimestamp(int(raw) / (1000 if len(raw) >= 13 else 1), datetime.timezone(datetime.timedelta(hours=8))).strftime('%H:%M:%S')
                except (ValueError, OverflowError):
                    return ''
            if re.fullmatch('\\d{6}', raw):
                return raw[:2] + ':' + raw[2:4] + ':' + raw[4:]
            return raw if re.fullmatch('\\d{2}:\\d{2}:\\d{2}', raw) else ''
        names = {code: item.get('name') or code for pool in pools.values() for code, item in pool.items()}
        events = {code: {} for code in codes}

        def event(day, code, item, consecutive):
            board, span, label = ladder_board_pattern(item.get('board_period'), consecutive)
            raw = daily.get((day, code))
            facts = {**(dict(raw) if raw is not None else {}), **seal_facts.get((day, code), {})}
            facts['last_time'] = clock(facts.get('last_time')) or facts.get('last_time') or ''
            try:
                upper, high, low = (float(facts[key]) for key in ('up_limit', 'daily_high', 'daily_low'))
                same = lambda value: value is not None and abs(float(value) - upper) < 0.005
                if same(high) and same(low):
                    facts['board_shape'] = 'one_word'
                elif same(facts.get('daily_open')) and same(high) and same(facts.get('daily_close')) and (not same(low)):
                    facts['board_shape'] = 't_shape'
            except (KeyError, TypeError, ValueError):
                pass
            traits = classify_limitup_traits(facts)
            pct = None
            pct_source = None
            for source, value in [('kpl_performance', item.get('pct_chg')), ('kpl_list', facts.get('kpl_pct_chg')), ('limit_list', facts.get('limit_pct_chg')), ('daily', facts.get('pct_chg'))]:
                try:
                    candidate = float(str(value).strip().rstrip('%'))
                    if math.isfinite(candidate):
                        pct = candidate
                        pct_source = source
                        break
                except (TypeError, ValueError):
                    pass
            return {'id': day + ':' + code, 'date': day, 'code': code, 'name': item.get('name') or names.get(code, code), 'theme': str(item.get('theme') or '').strip() or '暂无题材', 'board': board, 'consecutiveBoard': max(1, int(consecutive or 1)), 'patternDays': span, 'boardPeriod': label, 'board_shape': facts.get('board_shape'), 'board_strength': traits['board_strength'], 'last_time': facts.get('last_time'), 'open_times': facts.get('open_times'), 'status': 'sealed', 'pct': pct, 'pctSource': pct_source, 'time': clock(item.get('lu_time')), 'representative': False, 'firstCount': 0, 'members': []}
        reported_events = set(meta) | set(seal_facts) | {(day, code) for day, pool in pools.items() for code in pool}
        for row in history:
            day, code = (row['trade_date'], row['ts_code'])
            if is_closed_limit_up(row) and ((day, code) in reported_events or day < history_start):
                events[code][day] = event(day, code, meta.get((day, code), {}), counts.get(day, {}).get(code, 1))
        for day, pool in pools.items():
            for code, item in list(pool.items()):
                raw = daily.get((day, code))
                limit = limits.get((day, code))
                price = item.get('close_price')
                if raw is not None and (not is_closed_limit_up(raw)):
                    del pool[code]
                    continue
                if raw is None and price is not None and limit and (abs(float(price) - float(limit)) >= 0.005):
                    del pool[code]
                    continue
                consecutive = counts.get(day, {}).get(code)
                if consecutive is None:
                    prior = [node for previous, node in events[code].items() if previous < day]
                    previous = max(prior, key=lambda node: node['date']) if prior else None
                    gap = calendar_days[day_index.get(previous['date'], -1) + 1:day_index[day]] if previous else []
                    adjacent = previous and (day_index[day] - day_index.get(previous['date'], -999) == 1 or (gap and all(((d, code) in suspended for d in gap))))
                    consecutive = previous['consecutiveBoard'] + 1 if adjacent else 1
                events[code][day] = event(day, code, item, consecutive)
        promotion = []
        outgoing = {}
        for code, by_date in events.items():
            previous = None
            for node in sorted(by_date.values(), key=lambda item: item['date']):
                if previous is not None and node['board'] > 1 and (node['board'] == previous['board'] + 1):
                    left = day_index.get(previous['date'])
                    right = day_index.get(node['date'])
                    gap = calendar_days[left + 1:right] if left is not None and right is not None else []
                    halted = bool(gap) and all(((day, code) in suspended for day in gap))
                    multi = node['patternDays'] > node['board']
                    continuous = left is not None and right - left == 1
                    if multi or continuous or halted:
                        kind = 'suspension' if halted else 'broken' if gap else 'continuous'
                        edge = {'source': previous['id'], 'target': node['id'], 'status': 'promoted', 'kind': kind, 'value': 1, 'gapDates': gap, 'sourceBoard': previous['board'], 'targetBoard': node['board']}
                        if kind != 'continuous':
                            failure = gap[0] if halted else next((day for day in gap if (day, code) not in suspended))
                            edge.update(code=code, name=node['name'], sourceDate=previous['date'], failureDate=failure, targetDate=node['date'], sessions=right - day_index[failure])
                        promotion.append(edge)
                        outgoing[previous['id']] = edge
                previous = node
        broken_meta = {}
        with closing(sqlite3.connect(f'file:{self.kpl_db}?mode=ro', uri=True, timeout=4)) as c:
            for day, code, name, time, theme, description in c.execute(f"SELECT trade_date,ts_code,name,lu_time,theme,lu_desc FROM tbl_cn_kpl_list WHERE trade_date BETWEEN ? AND ? AND ts_code IN ({code_marks}) AND tag='炸板'", [history_start, end_date, *codes]):
                broken_meta[day, code] = {'name': name, 'time': clock(time), 'theme': re.split('[、,，;；]', str(description or theme or ''))[0].strip() or '暂无题材'}
        raw_by_code = {code: [] for code in codes}
        for raw in recent_history:
            raw_by_code[raw['ts_code']].append(raw)
        broken_events = {}
        for code, raws in raw_by_code.items():
            previous_raw = None
            items = []
            for raw in raws:
                if (raw['trade_date'], code) in suspended:
                    continue
                if previous_raw is not None and (not is_closed_limit_up(previous_raw)) and (raw['up_limit'] is not None) and (raw['daily_high'] is not None) and (float(raw['up_limit']) > 0) and (float(raw['daily_high']) >= float(raw['up_limit']) - 0.005) and (not is_closed_limit_up(raw)):
                    day = raw['trade_date']
                    extra = broken_meta.get((day, code), {})
                    items.append({'id': day + ':' + code + ':broken', 'date': day, 'code': code, 'name': extra.get('name') or names.get(code, code), 'theme': extra.get('theme') or '暂无题材', 'board': 0, 'status': 'broken', 'pct': raw['pct_chg'], 'time': extra.get('time') or ''})
                previous_raw = raw
            broken_events[code] = items
        relimit_links = []
        relimit_context = {}
        relimit_broken = {}
        for code, by_date in events.items():
            previous = None
            for node in sorted(by_date.values(), key=lambda item: item['date']):
                if previous is not None and node['date'] in dates and (node['board'] == 1):
                    left = day_index.get(previous['date'])
                    right = day_index.get(node['date'])
                    if left is not None and right is not None:
                        intervening = calendar_days[left + 1:right]
                        failure = next((day for day in intervening if (day, code) not in suspended), None)
                        raw = daily.get((failure, code)) if failure else None
                        gap = right - day_index[failure] if failure else 0
                        if raw is not None and (not is_closed_limit_up(raw)) and (1 <= gap <= 10):
                            relimit_links.append({'source': previous['id'], 'target': node['id'], 'status': 'relimit', 'kind': 'relimit', 'code': code, 'name': node['name'], 'sourceDate': previous['date'], 'failureDate': failure, 'targetDate': node['date'], 'sessions': gap, 'sourceBoard': previous['board'], 'afterSuspension': failure != calendar_days[left + 1]})
                            relimit_context[previous['id']] = previous
                previous = node
        nearest = {edge['target']: edge for edge in relimit_links}
        for code, by_date in events.items():
            failures = broken_events.get(code, [])
            failure_days = [stock['date'] for stock in failures]
            for node in by_date.values():
                if node['date'] not in dates or node['board'] != 1:
                    continue
                index = bisect.bisect_left(failure_days, node['date']) - 1
                if index < 0:
                    continue
                source = failures[index]
                gap = day_index[node['date']] - day_index[source['date']]
                sealed_between = any((source['date'] < day < node['date'] for day in by_date))
                prior = nearest.get(node['id'])
                if 1 <= gap <= 10 and (not sealed_between) and (prior is None or source['date'] > prior['failureDate']):
                    nearest[node['id']] = {'source': source['id'], 'target': node['id'], 'status': 'relimit', 'kind': 'relimit', 'code': code, 'name': node['name'], 'sourceDate': source['date'], 'failureDate': source['date'], 'targetDate': node['date'], 'sessions': gap, 'sourceBoard': 0, 'afterSuspension': False}
                    relimit_broken[source['id']] = source
        relimit_links = list(nearest.values())
        visible = []
        context = []
        for day, pool in pools.items():
            for code in pool:
                node = events[code][day]
                (visible if day in dates else context).append(node)
        links = [edge for edge in promotion if edge['target'].split(':', 1)[0] in pool_days]
        included = {node['id'] for node in visible + context}
        for edge in links:
            if edge['source'] not in included:
                source_day, code = edge['source'].split(':', 1)
                context.append(events[code][source_day])
                included.add(edge['source'])
        nodes = list(visible)
        now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))
        for node in visible + context:
            if node['id'] in outgoing or node['board'] <= 1:
                continue
            index = day_index[node['date']]
            following = calendar_days[index + 1:]
            active = next((day for day in following if (day, node['code']) not in suspended), None)
            if active is None:
                if following:
                    target = {'id': end_date + ':' + node['code'] + ':paused', 'date': end_date, 'code': node['code'], 'name': node['name'], 'theme': node['theme'], 'board': 0, 'previousBoard': node['board'], 'status': 'paused', 'time': '', 'representative': False, 'firstCount': 0, 'members': []}
                    nodes.append(target)
                    links.append({'source': node['id'], 'target': target['id'], 'status': 'paused', 'value': 1})
                continue
            raw = daily.get((active, node['code']))
            status = 'pending' if live and active == live['date'] and (live.get('mode') != 'close' or raw is None) or (active == now.strftime('%Y%m%d') and now.hour < 15 and (raw is None)) else 'failed'
            target = {'id': active + ':' + node['code'] + ':' + status, 'date': active, 'code': node['code'], 'name': node['name'], 'theme': node['theme'], 'board': 0, 'previousBoard': node['board'], 'status': status, 'pct': raw['pct_chg'] if raw else None, 'time': '', 'representative': False, 'firstCount': 0, 'members': []}
            nodes.append(target)
            links.append({'source': node['id'], 'target': target['id'], 'status': status, 'value': 1})
        links = list({(edge['source'], edge['target']): edge for edge in links}.values())
        suspension_carry = []
        for edge in promotion:
            if edge['kind'] != 'suspension':
                continue
            target_day, code = edge['target'].split(':', 1)
            if target_day not in dates:
                continue
            previous_day = previous_dates.get(target_day)
            source_day = edge['source'].split(':', 1)[0]
            if not previous_day or previous_day <= source_day:
                continue
            source = events[code][source_day]
            suspension_carry.append({'date': previous_day, 'targetDate': target_day, 'code': code, 'board': source['board'], 'theme': source['theme'], 'sourceId': edge['source'], 'targetId': edge['target']})
        unpromoted = []
        for day in dates:
            previous_day = previous_dates.get(day)
            if not previous_day:
                continue
            for code in pools.get(previous_day, {}):
                raw = daily.get((day, code))
                if (day, code) in suspended or raw is None or is_closed_limit_up(raw):
                    continue
                source = events[code][previous_day]
                unpromoted.append({'id': day + ':' + code + ':unpromoted', 'date': day, 'code': code, 'name': source['name'], 'theme': source['theme'], 'sourceId': source['id'], 'previousBoard': source['board'], 'previousBoardPeriod': source['boardPeriod'], 'pct': raw['pct_chg'], 'touchedLimit': raw['daily_high'] is not None and raw['up_limit'] is not None and (abs(float(raw['daily_high']) - float(raw['up_limit'])) < 0.005)})
        first_counts = {day: {} for day in dates}
        for node in visible:
            if node['board'] == 1:
                first_counts[node['date']][node['theme']] = first_counts[node['date']].get(node['theme'], 0) + 1
        themes = sorted({node['theme'] for node in visible})
        live_broken = []
        if live:
            day = live['date']
            prior_failed = {stock['code'] for stock in unpromoted if stock['date'] == day}
            for code, item in live['failed'].items():
                if code not in prior_failed:
                    live_broken.append({'id': day + ':' + code + ':broken', 'date': day, 'code': code, 'name': item['name'], 'theme': item['theme'], 'originalTheme': item['theme'], 'status': 'broken', 'board': 0, 'pct': item['pct'], 'time': clock(item['lu_time']), 'isLive': True})
            for stock in visible + context + nodes + unpromoted + live_broken:
                if stock['date'] == day:
                    stock.update(isLive=True, pct=live['rows'].get(stock['code'], {}).get('pct_chg'), pctSource='live_quote')
        return {'live': {key: live.get(key) for key in ('date', 'updatedAt', 'revision', 'structureRevision', 'frozen', 'sources', 'mode', 'offlineReady', 'sourceKinds')} if live else None, 'brokenStocks': live_broken, 'relimitLinks': relimit_links, 'relimitContextStocks': list(relimit_context.values()), 'relimitBrokenStocks': list(relimit_broken.values()), 'sealedStocks': visible, 'contextStocks': context, 'suspendedStocks': [], 'suspensionCarry': suspension_carry, 'unpromotedStocks': unpromoted, 'date': end_date, 'dates': dates, 'previousDates': previous_dates, 'nodes': nodes, 'links': links, 'themes': [{'name': name} for name in themes], 'firstCounts': first_counts, 'source': 'kpl_performance_board_pattern+verified_actual_limitup_paths'}

    def broken_for_day(self, date):
        include_details = False
        details = {'stocks': {}}
        with closing(sqlite3.connect(f'file:{self.basic_db}?mode=ro', uri=True, timeout=3)) as connection:
            connection.row_factory = sqlite3.Row
            failures = []
            for row in connection.execute('SELECT d.ts_code,d.pct_chg FROM tbl_cn_day d\n            JOIN tbl_cn_stk_limit l ON l.ts_code=d.ts_code AND l.trade_date=d.trade_date\n            WHERE d.trade_date=? AND l.up_limit>0 AND d.high>=l.up_limit-0.005 AND d.close<l.up_limit-0.005', (date,)):
                code = row['ts_code']
                prior = connection.execute('SELECT d.close,l.up_limit FROM tbl_cn_day d\n                LEFT JOIN tbl_cn_stk_limit l ON l.ts_code=d.ts_code AND l.trade_date=d.trade_date\n                WHERE d.ts_code=? AND d.trade_date<? AND d.vol>0 ORDER BY d.trade_date DESC LIMIT 1', (code, date)).fetchone()
                failures.append({'code': code, 'pct': float(row['pct_chg']) if row['pct_chg'] is not None else None, 'isPromotionFailure': prior is not None and is_closed_limit_up(prior)})
            names = {row[0]: row[1] for row in connection.execute('SELECT ts_code,name FROM tbl_cn_basic')}
        market = {}

        def clock(raw):
            value = str(raw or '').strip()
            if re.fullmatch('\\d{10,13}', value):
                stamp = int(value) / (1000 if len(value) == 13 else 1)
                return datetime.datetime.fromtimestamp(stamp, datetime.timezone(datetime.timedelta(hours=8))).strftime('%H:%M:%S')
            if re.fullmatch('\\d{1,6}', value):
                value = value.zfill(6)
                return f'{value[:2]}:{value[2:4]}:{value[4:]}'
            return value
        with closing(sqlite3.connect(f'file:{self.kpl_db}?mode=ro', uri=True, timeout=3)) as connection:
            for code, name, time, theme, description in connection.execute("SELECT ts_code,name,lu_time,theme,lu_desc FROM tbl_cn_kpl_list WHERE trade_date=? AND tag IN ('涨停','炸板')", (date,)):
                raw = str(description or theme or '').strip()
                market[code] = {'name': name or names.get(code) or code[:6], 'time': clock(time), 'theme': re.split('[、,，;；]', raw)[0].strip() or '暂无题材', 'marketReason': ''}
            for code, name, time in connection.execute('SELECT ts_code,name,first_time FROM tbl_cn_limit_list WHERE trade_date=? AND "limit" IN (\'U\',\'Z\')', (date,)):
                value = market.setdefault(code, {'name': name or names.get(code) or code[:6], 'time': '', 'theme': '暂无题材', 'marketReason': ''})
                if not value['time']:
                    value['time'] = clock(time)
            for code, name, time, theme, reason in connection.execute('SELECT ts_code,name,lu_time,theme,limit_reason FROM tbl_cn_kpl_limit_performance WHERE trade_date=?', (date,)):
                value = market.setdefault(code, {'name': name or names.get(code) or code[:6], 'time': '', 'theme': '暂无题材', 'marketReason': ''})
                if theme:
                    value['theme'] = str(theme).strip()
                if not value['time']:
                    value['time'] = clock(time)
                value['marketReason'] = str(reason or '').strip()
        for code, extra in market.items():
            stock = details['stocks'].setdefault(code, {'reason': '', 'reasons': {}})
            stock.update(marketReason=extra['marketReason'], time=extra['time'])
        broken = []
        promotion_failures = []
        for row in failures:
            code = row['code']
            extra = market.get(code, {})
            target = promotion_failures if row['isPromotionFailure'] else broken
            target.append({'id': date + ':' + code + ':broken', 'date': date, 'status': 'broken', 'code': code, 'name': extra.get('name') or names.get(code) or code[:6], 'theme': extra.get('theme') or '暂无题材', 'originalTheme': extra.get('theme') or '暂无题材', 'board': 0, 'pct': row['pct'], 'time': extra.get('time') or ''})
        if include_details:
            details.update(indices=indices(date), marketSummary=ladder_market_summary(date))
        return {**details, 'brokenStocks': broken, 'promotionFailures': promotion_failures}

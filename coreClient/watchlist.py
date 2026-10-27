"""Durable stock watchlist groups with a Redis read-through snapshot."""
from __future__ import annotations
from contextlib import closing
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import threading
import time
import unicodedata
import uuid


class WatchlistConflict(ValueError):
    pass


class WatchlistStore:
    def __init__(self, path=None, redis_client=None):
        if path is None:
            from coreClient._paths import offline_data_dir
            path=offline_data_dir()/'watchlists.db'
        self.path=Path(path).expanduser().resolve()
        self.path.parent.mkdir(parents=True,exist_ok=True)
        self.redis=redis_client
        token=hashlib.sha256(str(self.path).encode()).hexdigest()[:16]
        self.cache_key='tradingalert:watchlists:v1:'+token
        self._dirty=False
        self._lock=threading.RLock()
        with closing(self._connect()) as conn:
            conn.execute('PRAGMA journal_mode=WAL')
            conn.executescript('''
                CREATE TABLE IF NOT EXISTS watchlist_meta(id INTEGER PRIMARY KEY CHECK(id=1),generation TEXT NOT NULL,revision INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS watchlist_group(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL UNIQUE,created_at REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS watchlist_stock(code TEXT PRIMARY KEY,name TEXT NOT NULL,version INTEGER NOT NULL DEFAULT 0,updated_at REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS watchlist_member(code TEXT NOT NULL REFERENCES watchlist_stock(code),group_id INTEGER NOT NULL REFERENCES watchlist_group(id) ON DELETE CASCADE,added_at REAL,PRIMARY KEY(code,group_id));
                CREATE INDEX IF NOT EXISTS watchlist_member_group ON watchlist_member(group_id,code);
            ''')
            try:
                conn.execute('BEGIN IMMEDIATE')
                conn.execute('INSERT OR IGNORE INTO watchlist_meta VALUES (1,?,0)',(uuid.uuid4().hex,))
                columns={row[1] for row in conn.execute('PRAGMA table_info(watchlist_member)')}
                changed='added_at' not in columns
                if changed:
                    # Old memberships have no known join time; do not invent one.
                    conn.execute('ALTER TABLE watchlist_member ADD COLUMN added_at REAL')
                if not conn.execute('SELECT 1 FROM watchlist_group LIMIT 1').fetchone():
                    conn.execute('INSERT INTO watchlist_group(name,created_at) VALUES (?,?)',('默认分组',time.time()))
                    changed=True
                if changed:conn.execute('UPDATE watchlist_meta SET revision=revision+1 WHERE id=1')
                conn.commit()
            except BaseException:
                conn.rollback();raise
        # Do not serve a Redis snapshot from before the schema/default migration.
        if changed:self._dirty=True

    def _connect(self):
        conn=sqlite3.connect(self.path,timeout=5)
        conn.execute('PRAGMA foreign_keys=ON')
        return conn

    @staticmethod
    def _stock(code,name):
        if not isinstance(code,str) or not re.fullmatch(r'\d{6}\.(SH|SZ|BJ)',code):
            raise ValueError('股票代码无效')
        if not isinstance(name,str) or not name.strip() or len(name.strip())>80:
            raise ValueError('股票名称无效')
        return code,name.strip()

    @staticmethod
    def _snapshot(conn):
        generation,revision=conn.execute('SELECT generation,revision FROM watchlist_meta WHERE id=1').fetchone()
        groups=[{'id':gid,'name':name,'count':count} for gid,name,count in conn.execute('SELECT g.id,g.name,count(m.code) FROM watchlist_group g LEFT JOIN watchlist_member m ON m.group_id=g.id GROUP BY g.id ORDER BY g.id')]
        stocks={code:{'name':name,'version':version,'groupIds':[],'groupAddedAt':{}} for code,name,version in conn.execute('SELECT code,name,version FROM watchlist_stock ORDER BY code')}
        for code,gid,added_at in conn.execute('SELECT code,group_id,added_at FROM watchlist_member ORDER BY code,group_id'):
            stocks[code]['groupIds'].append(gid)
            stocks[code]['groupAddedAt'][str(gid)]=added_at
        return {'schemaVersion':1,'generation':generation,'revision':revision,'groups':groups,'stocks':stocks}

    def _publish(self,payload):
        if self.redis is None:return False
        try:
            # A late reader cannot overwrite a newer committed membership edit.
            self.redis.eval('''
                local old=redis.call('GET',KEYS[1])
                if old then
                    local ok,value=pcall(cjson.decode,old)
                    if ok and value.generation==ARGV[1] and tonumber(value.revision)>tonumber(ARGV[2]) then return 0 end
                end
                redis.call('SET',KEYS[1],ARGV[3],'EX',604800)
                return 1
            ''',1,self.cache_key,payload['generation'],payload['revision'],json.dumps(payload,ensure_ascii=False,separators=(',',':')))
            self._dirty=False
            return True
        except Exception:
            self._dirty=True
            return False

    def refresh(self):
        with self._lock,closing(self._connect()) as conn:
            conn.execute('BEGIN')
            payload=self._snapshot(conn)
        published=self._publish(payload)
        return {**payload,'source':'redis' if published else 'sqlite'}

    def snapshot(self):
        if self.redis is not None and not self._dirty:
            try:
                raw=self.redis.get(self.cache_key)
                if raw:
                    payload=json.loads(raw)
                    if payload.get('schemaVersion')==1 and isinstance(payload.get('groups'),list) and isinstance(payload.get('stocks'),dict):
                        return {**payload,'source':'redis'}
            except Exception:pass
        return self.refresh()

    def create_group(self,name,stock=None):
        if not isinstance(name,str):raise ValueError('请输入自选分组名称')
        name=' '.join(unicodedata.normalize('NFKC',name).split())
        if not name or len(name)>40:raise ValueError('分组名称需为1至40个字')
        if stock is not None:
            if not isinstance(stock,dict):raise ValueError('股票信息无效')
            code,stock_name=self._stock(stock.get('code'),stock.get('name'))
        with self._lock,closing(self._connect()) as conn:
            try:
                conn.execute('BEGIN IMMEDIATE')
                existed=conn.execute('SELECT id FROM watchlist_group WHERE name=?',(name,)).fetchone()
                changed=not existed
                if existed:gid=existed[0]
                else:gid=conn.execute('INSERT INTO watchlist_group(name,created_at) VALUES (?,?)',(name,time.time())).lastrowid
                if stock is not None:
                    conn.execute('INSERT OR IGNORE INTO watchlist_stock VALUES (?,?,0,?)',(code,stock_name,time.time()))
                    inserted=conn.execute('INSERT OR IGNORE INTO watchlist_member(code,group_id,added_at) VALUES (?,?,?)',(code,gid,time.time())).rowcount
                    if inserted:
                        conn.execute('UPDATE watchlist_stock SET name=?,version=version+1,updated_at=? WHERE code=?',(stock_name,time.time(),code))
                        changed=True
                if changed:conn.execute('UPDATE watchlist_meta SET revision=revision+1 WHERE id=1')
                payload=self._snapshot(conn)
                conn.commit()
            except BaseException:
                conn.rollback();raise
            published=self._publish(payload)
        return {**payload,'source':'redis' if published else 'sqlite','groupId':gid}

    def set_groups(self,code,name,group_ids,expected_version):
        code,name=self._stock(code,name)
        if not isinstance(group_ids,list) or len(group_ids)>1000 or any(type(gid) is not int or gid<1 for gid in group_ids):
            raise ValueError('自选分组无效')
        if type(expected_version) is not int or expected_version<0:raise ValueError('自选版本无效，请重新打开菜单')
        ids=sorted(set(group_ids))
        with self._lock,closing(self._connect()) as conn:
            try:
                conn.execute('BEGIN IMMEDIATE')
                row=conn.execute('SELECT version FROM watchlist_stock WHERE code=?',(code,)).fetchone()
                version=row[0] if row else 0
                if version!=expected_version:raise WatchlistConflict('该股自选分组已在其他页面修改，请重新勾选')
                known={gid for gid, in conn.execute('SELECT id FROM watchlist_group')}
                if not set(ids)<=known:raise ValueError('自选分组不存在，请重新打开菜单')
                previous=[gid for gid, in conn.execute('SELECT group_id FROM watchlist_member WHERE code=? ORDER BY group_id',(code,))]
                if ids!=previous:
                    conn.execute('INSERT OR IGNORE INTO watchlist_stock VALUES (?,?,0,?)',(code,name,time.time()))
                    # Only change the membership delta, preserving retained join times.
                    conn.executemany('DELETE FROM watchlist_member WHERE code=? AND group_id=?',[(code,gid) for gid in previous if gid not in ids])
                    added_at=time.time()
                    conn.executemany('INSERT INTO watchlist_member(code,group_id,added_at) VALUES (?,?,?)',[(code,gid,added_at) for gid in ids if gid not in previous])
                    conn.execute('UPDATE watchlist_stock SET name=?,version=version+1,updated_at=? WHERE code=?',(name,time.time(),code))
                    conn.execute('UPDATE watchlist_meta SET revision=revision+1 WHERE id=1')
                payload=self._snapshot(conn)
                conn.commit()
            except BaseException:
                conn.rollback();raise
            published=self._publish(payload)
        return {**payload,'source':'redis' if published else 'sqlite'}

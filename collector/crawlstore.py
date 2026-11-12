"""代际遍历的持久化层。

与 storage.py 面向“一次导入”的凭据不同，这里保留每一次遍历（视图）的完整证据：
索引/子图实体、父子引用关系、每次获取结果、叶子条目，以及跨进程的子图完成状态
和条目生命周期。所有判断都基于这些行级证据，而不是内存状态。
"""
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone


def _now():
    return datetime.now(timezone.utc).isoformat()


class RunLocked(RuntimeError):
    """同一遍历已有进程持锁。"""


class CrawlStore:
    def __init__(self, path):
        self.connection = sqlite3.connect(path, timeout=10, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute('PRAGMA foreign_keys=ON')
        self.connection.execute('PRAGMA journal_mode=WAL')
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS crawl_runs(
                run_id TEXT PRIMARY KEY,
                root_urls TEXT NOT NULL,
                parent_run_id TEXT,
                state TEXT NOT NULL,
                complete_view INTEGER NOT NULL DEFAULT 0,
                incomplete_reasons TEXT NOT NULL DEFAULT '[]',
                limits_json TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT);
            CREATE TABLE IF NOT EXISTS crawl_nodes(
                run_id TEXT NOT NULL REFERENCES crawl_runs(run_id),
                url TEXT NOT NULL,
                depth INTEGER NOT NULL,
                kind TEXT,
                observe_status TEXT NOT NULL,
                http_status INTEGER,
                etag TEXT,
                digest TEXT,
                compressed INTEGER NOT NULL DEFAULT 0,
                error TEXT,
                fetched_at TEXT,
                PRIMARY KEY(run_id,url));
            CREATE TABLE IF NOT EXISTS crawl_edges(
                run_id TEXT NOT NULL,
                parent_url TEXT NOT NULL,
                child_url TEXT NOT NULL,
                position INTEGER NOT NULL,
                edge_type TEXT NOT NULL DEFAULT 'index',
                cyclic INTEGER NOT NULL DEFAULT 0,
                duplicate INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS crawl_leaves(
                run_id TEXT NOT NULL,
                url TEXT NOT NULL,
                parent_url TEXT NOT NULL,
                lastmod_claim TEXT,
                position INTEGER NOT NULL,
                PRIMARY KEY(run_id,url));
            CREATE TABLE IF NOT EXISTS node_bodies(
                url TEXT NOT NULL,
                etag TEXT NOT NULL,
                body BLOB NOT NULL,
                PRIMARY KEY(url,etag));
            CREATE TABLE IF NOT EXISTS subgraph_state(
                url TEXT PRIMARY KEY,
                etag TEXT,
                digest TEXT,
                completed_run TEXT,
                version INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS entry_lifecycle(
                url TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                first_seen_run TEXT,
                last_seen_run TEXT,
                miss_streak INTEGER NOT NULL DEFAULT 0,
                evidence_json TEXT NOT NULL DEFAULT '[]');
            CREATE TABLE IF NOT EXISTS run_locks(
                run_id TEXT PRIMARY KEY,
                owner TEXT NOT NULL,
                acquired_at TEXT NOT NULL);
            """
        )

    @contextmanager
    def transaction(self):
        self.connection.execute('BEGIN IMMEDIATE')
        try:
            yield
        except BaseException:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()

    def close(self):
        self.connection.close()

    # -- 遍历运行 -------------------------------------------------------

    def start_run(self, run_id, root_urls, limits, parent_run_id=None):
        with self.transaction():
            self.connection.execute(
                'INSERT INTO crawl_runs(run_id,root_urls,parent_run_id,state,limits_json,started_at)'
                ' VALUES(?,?,?,?,?,?)',
                (run_id, json.dumps(root_urls), parent_run_id, 'running',
                 json.dumps(limits, sort_keys=True), _now()),
            )

    def get_run(self, run_id):
        row = self.connection.execute('SELECT * FROM crawl_runs WHERE run_id=?', (run_id,)).fetchone()
        return dict(row) if row else None

    def list_runs(self):
        return [dict(row) for row in self.connection.execute(
            'SELECT run_id,state,complete_view,root_urls,started_at,finished_at FROM crawl_runs ORDER BY started_at')]

    def finish_run(self, run_id, state, complete_view, reasons):
        with self.transaction():
            self.connection.execute(
                'UPDATE crawl_runs SET state=?, complete_view=?, incomplete_reasons=?, finished_at=? WHERE run_id=?',
                (state, 1 if complete_view else 0, json.dumps(reasons, ensure_ascii=False), _now(), run_id),
            )

    def acquire_lock(self, run_id, owner):
        try:
            with self.transaction():
                self.connection.execute(
                    'INSERT INTO run_locks(run_id,owner,acquired_at) VALUES(?,?,?)',
                    (run_id, owner, _now()),
                )
        except sqlite3.IntegrityError as exc:
            raise RunLocked(f'遍历 {run_id} 已被其他进程占用') from exc

    def release_lock(self, run_id, owner):
        with self.transaction():
            self.connection.execute('DELETE FROM run_locks WHERE run_id=? AND owner=?', (run_id, owner))

    # -- 节点、引用、叶子证据 -------------------------------------------

    def save_node(self, run_id, observation):
        with self.transaction():
            self.connection.execute(
                'INSERT INTO crawl_nodes(run_id,url,depth,kind,observe_status,http_status,etag,digest,'
                'compressed,error,fetched_at) VALUES(?,?,?,?,?,?,?,?,?,?,?) '
                'ON CONFLICT(run_id,url) DO UPDATE SET depth=excluded.depth,kind=excluded.kind,'
                'observe_status=excluded.observe_status,http_status=excluded.http_status,etag=excluded.etag,'
                'digest=excluded.digest,compressed=excluded.compressed,error=excluded.error,'
                'fetched_at=excluded.fetched_at',
                (run_id, observation['url'], observation['depth'], observation.get('kind'),
                 observation['status'], observation.get('http_status'), observation.get('etag'),
                 observation.get('digest'), 1 if observation.get('compressed') else 0,
                 observation.get('error'), _now()),
            )

    def get_node(self, run_id, url):
        row = self.connection.execute(
            'SELECT * FROM crawl_nodes WHERE run_id=? AND url=?', (run_id, url)).fetchone()
        return dict(row) if row else None

    def list_nodes(self, run_id):
        return [dict(row) for row in self.connection.execute(
            'SELECT * FROM crawl_nodes WHERE run_id=? ORDER BY depth,url', (run_id,))]

    def add_edge(self, run_id, parent_url, child_url, position, cyclic, duplicate, edge_type='index'):
        with self.transaction():
            self.connection.execute(
                'INSERT OR IGNORE INTO crawl_edges(run_id,parent_url,child_url,position,edge_type,'
                'cyclic,duplicate) VALUES(?,?,?,?,?,?,?)',
                (run_id, parent_url, child_url, position, edge_type,
                 1 if cyclic else 0, 1 if duplicate else 0),
            )

    def list_edges(self, run_id):
        return [dict(row) for row in self.connection.execute(
            'SELECT * FROM crawl_edges WHERE run_id=? ORDER BY parent_url,position', (run_id,))]

    def parent_map(self, run_id):
        parents = {}
        for edge in self.list_edges(run_id):
            parents.setdefault(edge['child_url'], set()).add(edge['parent_url'])
        return parents

    def add_leaf(self, run_id, url, parent_url, lastmod_claim, position):
        with self.transaction():
            self.connection.execute(
                'INSERT OR IGNORE INTO crawl_leaves(run_id,url,parent_url,lastmod_claim,position)'
                ' VALUES(?,?,?,?,?)', (run_id, url, parent_url, lastmod_claim, position))

    def leaf_set(self, run_id):
        return {row[0] for row in self.connection.execute(
            'SELECT url FROM crawl_leaves WHERE run_id=?', (run_id,))}

    def list_leaves(self, run_id):
        return [dict(row) for row in self.connection.execute(
            'SELECT * FROM crawl_leaves WHERE run_id=? ORDER BY url', (run_id,))]

    def pending_targets(self, run_id):
        """恢复遍历：有索引引用但没有成功观察的节点，需要重新获取。

        叶子（urlset 内的资料地址）不是地图节点，edge_type='leaf' 的边不参与；
        循环边指向的祖先已在别处展开，也不需要重取。
        """
        rows = self.connection.execute(
            "SELECT DISTINCT e.child_url FROM crawl_edges e LEFT JOIN crawl_nodes n "
            "ON n.run_id=e.run_id AND n.url=e.child_url WHERE e.run_id=? AND e.edge_type='index' "
            "AND e.cyclic=0 AND (n.observe_status IS NULL OR n.observe_status <> 'ok')",
            (run_id,)).fetchall()
        return [row[0] for row in rows]

    def save_body(self, url, etag, body):
        if not etag:
            return
        with self.transaction():
            # 同 ETag 若正文换代（窗口态可能发生），以新正文为准
            self.connection.execute(
                'INSERT INTO node_bodies(url,etag,body) VALUES(?,?,?) '
                'ON CONFLICT(url,etag) DO UPDATE SET body=excluded.body',
                (url, etag, body))

    def get_body(self, url, etag):
        row = self.connection.execute(
            'SELECT body FROM node_bodies WHERE url=? AND etag=?', (url, etag)).fetchone()
        return row[0] if row else None

    # -- 子图完成状态：乐观并发控制（CAS） -------------------------------

    def get_subgraph(self, url):
        row = self.connection.execute('SELECT * FROM subgraph_state WHERE url=?', (url,)).fetchone()
        return dict(row) if row else None

    def complete_subgraph(self, url, etag, digest, run_id, expected_version):
        """条件写入子图完成状态。

        expected_version 是本次获取开始时读到的版本。若期间另一进程已换代该子图，
        行的 version 已变化，UPDATE 不会命中，返回 stale，调用方必须重读，
        旧请求携带的 ETag 无法覆盖新的完成状态。
        """
        with self.transaction():
            current = self.get_subgraph(url)
            if current is None:
                if expected_version is not None:
                    return 'stale'
                self.connection.execute(
                    'INSERT INTO subgraph_state(url,etag,digest,completed_run,version,updated_at)'
                    ' VALUES(?,?,?,?,1,?)', (url, etag, digest, run_id, _now()))
                return 'created'
            if expected_version != current['version']:
                return 'stale'
            if current['etag'] == etag and current['digest'] == digest:
                self.connection.execute(
                    'UPDATE subgraph_state SET completed_run=?, updated_at=? WHERE url=?',
                    (run_id, _now(), url))
                return 'unchanged'
            updated = self.connection.execute(
                'UPDATE subgraph_state SET etag=?, digest=?, completed_run=?, version=version+1,'
                ' updated_at=? WHERE url=? AND version=?',
                (etag, digest, run_id, _now(), url, expected_version)).rowcount
            return 'updated' if updated else 'stale'

    # -- 条目生命周期与撤下规则 ------------------------------------------

    def get_lifecycle(self, url):
        row = self.connection.execute('SELECT * FROM entry_lifecycle WHERE url=?', (url,)).fetchone()
        if not row:
            return None
        data = dict(row)
        data['evidence'] = json.loads(data.pop('evidence_json'))
        return data

    def list_lifecycle(self):
        result = []
        for row in self.connection.execute('SELECT * FROM entry_lifecycle ORDER BY url'):
            data = dict(row)
            data['evidence'] = json.loads(data.pop('evidence_json'))
            result.append(data)
        return result

    def apply_complete_view(self, run_id, present):
        """只在可证明完整的视图上调用。

        完整视图缺席一次 -> miss_streak +1（候选）；连续两次独立完整视图缺席 -> withdrawn。
        不完整视图根本不会进入这里，因此限额、解析错误、网络失败永远无法贡献缺失证据。
        """
        with self.transaction():
            existing = {row[0]: dict(row) for row in self.connection.execute(
                'SELECT * FROM entry_lifecycle')}
            for url in sorted(present):
                row = existing.get(url)
                if row is None:
                    self.connection.execute(
                        'INSERT INTO entry_lifecycle(url,state,first_seen_run,last_seen_run,'
                        'miss_streak,evidence_json) VALUES(?,?,?,?,0,?)',
                        (url, 'active', run_id, run_id,
                         json.dumps([dict(run=run_id, event='seen')], ensure_ascii=False)))
                else:
                    evidence = json.loads(row['evidence_json'])
                    evidence.append(dict(run=run_id, event='seen'))
                    self.connection.execute(
                        'UPDATE entry_lifecycle SET state=?, last_seen_run=?, miss_streak=0,'
                        ' evidence_json=? WHERE url=?',
                        ('active', run_id, json.dumps(evidence, ensure_ascii=False), url))
            for url, row in existing.items():
                if url in present or row['state'] == 'withdrawn':
                    continue
                streak = row['miss_streak'] + 1
                evidence = json.loads(row['evidence_json'])
                evidence.append(dict(run=run_id, event='missing_in_complete_view'))
                state = 'withdrawn' if streak >= 2 else 'candidate'
                self.connection.execute(
                    'UPDATE entry_lifecycle SET state=?, miss_streak=?, evidence_json=? WHERE url=?',
                    (state, streak, json.dumps(evidence, ensure_ascii=False), url))

    def revive_if_seen(self, run_id, present):
        """已撤下条目若在新的完整视图中重新出现，恢复为 active。"""
        with self.transaction():
            for url in sorted(present):
                row = self.connection.execute(
                    'SELECT * FROM entry_lifecycle WHERE url=?', (url,)).fetchone()
                if row and row['state'] == 'withdrawn':
                    evidence = json.loads(row['evidence_json'])
                    evidence.append(dict(run=run_id, event='reappeared'))
                    self.connection.execute(
                        'UPDATE entry_lifecycle SET state=?, last_seen_run=?, miss_streak=0,'
                        ' evidence_json=? WHERE url=?',
                        ('active', run_id, json.dumps(evidence, ensure_ascii=False), url))

    # -- 视图差异与子树重读计划 ------------------------------------------

    def diff_views(self, earlier_run_id, later_run_id):
        earlier = self.leaf_set(earlier_run_id)
        later = self.leaf_set(later_run_id)
        return {
            'earlier_run': earlier_run_id,
            'later_run': later_run_id,
            'added': sorted(later - earlier),
            'removed': sorted(earlier - later),
            'retained': sorted(earlier & later),
            'earlier_complete': bool(self.get_run(earlier_run_id)['complete_view']),
            'later_complete': bool(self.get_run(later_run_id)['complete_view']),
        }

    def reread_plan(self, previous_run_id, current_run_id):
        """索引换代后判断每个子树是新增、丢弃、ETag 变化需重读，还是未变可条件校验。"""
        prev_nodes = {n['url']: n for n in self.list_nodes(previous_run_id)}
        cur_nodes = {n['url']: n for n in self.list_nodes(current_run_id)}
        prev_edges = {(e['parent_url'], e['child_url']) for e in self.list_edges(previous_run_id)}
        cur_edges = {(e['parent_url'], e['child_url']) for e in self.list_edges(current_run_id)}
        plan = []
        for url in sorted(set(prev_nodes) | set(cur_nodes)):
            prev, cur = prev_nodes.get(url), cur_nodes.get(url)
            referenced_before = any(child == url for _, child in prev_edges)
            referenced_now = any(child == url for _, child in cur_edges)
            if prev is None and referenced_now:
                decision = 'new'
            elif cur is None and referenced_before:
                decision = 'dropped'
            elif prev and cur and prev.get('etag') != cur.get('etag'):
                decision = 'reread_etag_changed'
            elif prev and cur and prev.get('digest') != cur.get('digest'):
                decision = 'reread_body_changed'
            else:
                decision = 'unchanged_revalidate'
            plan.append({
                'url': url,
                'decision': decision,
                'previous_etag': prev.get('etag') if prev else None,
                'current_etag': cur.get('etag') if cur else None,
                'referenced_before': referenced_before,
                'referenced_now': referenced_now,
            })
        return plan

    def incomplete_reasons(self, run_id):
        return json.loads(self.get_run(run_id)['incomplete_reasons'])

"""完整遍历引擎。

设计要点（对应采集纪律）：
* 每个索引/子图节点的实体标识（URL）、引用边、获取结果都落库，可解释每一条证据；
* gzip 按响应头与魔数双重识别；嵌套索引递归展开，深度与节点数受 fixture 限额约束；
* 循环引用只记录 cyclic 边、不再获取；同一 URL 多处出现仍保留每条引用并标 duplicate；
* lastmod 仅作为来源声称值原样保存，不参与 ETag 判定，也不会成为扫描水位；
* 任何网络失败、HTTP 非 2xx/304、解析错误、限额触发，只会让本次视图“不完整”，
  绝不产生“条目缺失”的证据；
* 中断后依据持久化的边表恢复未完成节点；子图完成状态以版本号 CAS 提交，
  旧请求无法覆盖并发期间已换代的子图。
"""
import gzip
import hashlib
import json
import os
from datetime import datetime, timezone
from urllib.parse import urljoin, urlsplit

from .domain import parse_sitemap


def looks_gzipped(raw):
    return raw[:2] == b'\x1f\x8b'


def decode_body(raw, compressed_header):
    if compressed_header or looks_gzipped(raw):
        return gzip.decompress(raw), True
    return raw, False


def parse_claimed_time(value):
    """来源时间只做可读性解析；解析失败或未来时间都返回结构化说明，绝不抛出。"""
    if not value:
        return None
    text = value.strip()
    normalized = text.replace('Z', '+00:00')
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return dict(raw=text, parseable=False, in_future=None)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return dict(raw=text, parseable=True, in_future=parsed > datetime.now(timezone.utc))


class Fetcher:
    """基于 transport.Client 的获取器，发送条件请求与 gzip 接受头。"""

    def __init__(self, client):
        self.client = client

    def fetch(self, url, etag=None):
        headers = {'Accept-Encoding': 'gzip'}
        if etag:
            headers['If-None-Match'] = etag
        try:
            status, response_headers, raw = self.client.request(url, headers=headers)
        except Exception as exc:  # 网络层异常：没有响应，不能贡献缺失证据
            return dict(ok=False, network_error=type(exc).__name__, detail=str(exc))
        return dict(ok=True, status=status, headers=response_headers, raw=raw)

class Crawler:
    def __init__(self, store, fetcher, limits=None):
        self.store = store
        self.fetcher = fetcher
        self.limits = limits or dict(depth=8, nodes=100)
        self._stale_attempts = {}

    # -- 祖先与去重（恢复后可从边表重建） ---------------------------------

    @staticmethod
    def _ancestors_of(parent_map, url):
        result, stack, seen = set(), list(parent_map.get(url, ())), set()
        while stack:
            current = stack.pop()
            if current in seen:
                continue
            seen.add(current)
            result.add(current)
            stack.extend(parent_map.get(current, ()))
        return result

    # -- 主入口 ----------------------------------------------------------

    def run(self, run_id, root_urls):
        roots = list(root_urls)
        owner = f'pid-{os.getpid()}'
        # 同一 run 同时只允许一个进程推进；其他进程会得到 RunLocked
        self.store.acquire_lock(run_id, owner)
        try:
            return self._run_locked(run_id, roots)
        finally:
            self.store.release_lock(run_id, owner)

    def _run_locked(self, run_id, roots):
        existing = self.store.get_run(run_id)
        if existing is None:
            self.store.start_run(run_id, roots, self.limits)
            frontier = [(url, 0) for url in roots]
            for url in roots:
                self.store.add_edge(run_id, '', url, 0, False, False)
        else:
            # 恢复：只重取有引用但未成功观察的节点；成功节点及其边已在库中
            frontier = [(url, depth) for url in self.store.pending_targets(run_id)
                        if (depth := self._depth_of(run_id, url)) is not None]
        reasons = []
        limit_hit = False
        seen_nodes = 0
        # 实时父链与访问集合：恢复时从已落库的边表重建，保证循环/重复判定不丢上下文
        parent_map = {child: set(parents) for child, parents in
                      self.store.parent_map(run_id).items()}
        visited = {n['url'] for n in self.store.list_nodes(run_id)
                   if n['observe_status'] == 'ok'}
        queued = {url for url, _ in frontier}

        while frontier:
            url, depth = frontier.pop(0)
            queued.discard(url)
            if url in visited:
                continue
            if depth > self.limits['depth']:
                reasons.append(dict(reason='depth_limit', url=url, limit=self.limits['depth']))
                limit_hit = True
                self.store.save_node(run_id, dict(url=url, depth=depth, status='limit',
                                                  error='超过深度限额'))
                continue
            if seen_nodes >= self.limits['nodes']:
                reasons.append(dict(reason='node_limit', url=url, limit=self.limits['nodes']))
                limit_hit = True
                self.store.save_node(run_id, dict(url=url, depth=depth, status='limit',
                                                  error='超过节点限额'))
                continue
            seen_nodes += 1
            observation = self._observe(run_id, url, depth)
            self.store.save_node(run_id, observation)
            if observation['status'] != 'ok':
                reasons.append(dict(reason=observation['status'], url=url,
                                    detail=observation.get('error')))
                continue
            cas = self._commit_subgraph(run_id, url, observation)
            if cas == 'stale':
                # 并发换代：本次观察已过期，先把成功标记撤回再重读；
                # 旧请求携带的实体不得保留为该子图的完成状态
                reasons.append(dict(reason='stale_subgraph', url=url))
                self.store.save_node(run_id, dict(url=url, depth=depth, status='stale',
                                                  http_status=observation.get('http_status'),
                                                  etag=observation.get('etag'),
                                                  error='子图在获取期间已被其他进程换代'))
                if self._stale_attempts.get(url, 0) < 2:
                    self._stale_attempts[url] = self._stale_attempts.get(url, 0) + 1
                    frontier.insert(0, (url, depth))
                    queued.add(url)
                continue
            visited.add(url)
            for child in observation['children']:
                child_url = child['url']
                position = child['position']
                if observation['kind'] == 'urlset':
                    # 资料条目：登记为叶子与 leaf 边，不作为可获取节点
                    self.store.add_leaf(run_id, child_url, url, child.get('lastmod'), position)
                    self.store.add_edge(run_id, url, child_url, position, False, False,
                                        edge_type='leaf')
                    continue
                # 循环：指向自己或当前父链上的祖先；父链在内存中实时维护
                cyclic = child_url == url or child_url in self._ancestors_of(parent_map, url)
                # 同 URL 多处出现：已经访问过，或同批已入队等待
                duplicate = not cyclic and (child_url in visited or child_url in queued)
                parent_map.setdefault(child_url, set()).add(url)
                self.store.add_edge(run_id, url, child_url, position, cyclic, duplicate,
                                    edge_type='index')
                if cyclic:
                    # 循环引用到此为止：记录边但不再获取，视图本身仍然完整
                    continue
                if child_url not in visited and child_url not in queued:
                    frontier.append((child_url, depth + 1))
                    queued.add(child_url)

        complete = not reasons and not limit_hit
        # 恢复场景下还需确认所有被引用节点都成功观察
        if complete and self.store.pending_targets(run_id):
            complete = False
            reasons.append(dict(reason='unfinished_nodes',
                                urls=self.store.pending_targets(run_id)))
        state = 'completed' if complete else 'incomplete'
        self.store.finish_run(run_id, state, complete, reasons)
        return self.summarize(run_id)

    def _depth_of(self, run_id, url):
        node = self.store.get_node(run_id, url)
        if node:
            return node['depth']
        edges = self.store.list_edges(run_id)
        # 新入队尚未观察的节点：沿父节点深度 +1 推断
        parent_depths = [self._depth_of(run_id, edge['parent_url'])
                         for edge in edges if edge['child_url'] == url and edge['parent_url']]
        parent_depths = [depth for depth in parent_depths if depth is not None]
        return min(parent_depths) + 1 if parent_depths else 0

    # -- 单节点获取与解析 -------------------------------------------------

    def _observe(self, run_id, url, depth):
        previous = self.store.get_subgraph(url)
        result = self.fetcher.fetch(url, previous['etag'] if previous else None)
        if not result.get('ok'):
            return dict(url=url, depth=depth, status='network_error',
                        error=f"{result.get('network_error')}: {result.get('detail')}")
        status = result['status']
        headers = {key.lower(): value for key, value in result['headers'].items()}
        etag = headers.get('etag')
        if status == 304:
            if not previous or not previous['etag']:
                return dict(url=url, depth=depth, status='network_error',
                            error='304 响应缺少可复用的本地实体')
            body = self.store.get_body(url, previous['etag'])
            if body is None:
                return dict(url=url, depth=depth, status='network_error',
                            error='304 响应缺少本地缓存正文')
            etag = previous['etag']
            raw = body
            compressed = False
        elif 200 <= status < 300:
            raw = result['raw']
            try:
                body, compressed = decode_body(raw, headers.get('content-encoding') == 'gzip')
            except (gzip.BadGzipFile, OSError) as exc:
                return dict(url=url, depth=depth, status='parse_error', http_status=status,
                            etag=etag, error=f'gzip 解压失败: {exc}')
        else:
            return dict(url=url, depth=depth, status='http_error', http_status=status,
                        etag=etag, error=f'HTTP {status}')
        try:
            parsed = parse_sitemap(body, compressed=False)
        except Exception as exc:  # 解析错误只影响该节点与完整性
            return dict(url=url, depth=depth, status='parse_error', http_status=status,
                        etag=etag, error=f'XML 解析失败: {type(exc).__name__}: {exc}')
        digest = hashlib.sha256(body).hexdigest()
        if etag and status != 304:
            self.store.save_body(url, etag, body)
        children = []
        for position, entry in enumerate(parsed['entries']):
            # 相对 loc 按父地图地址解析；解析后仍非 http(s) 则该节点解析失败
            child_url = urljoin(url, entry['url'])
            if urlsplit(child_url).scheme not in ('http', 'https'):
                return dict(url=url, depth=depth, status='parse_error', http_status=status,
                            etag=etag, error=f'地图地址无效: {entry["url"]}')
            children.append(dict(url=child_url, position=position, lastmod=entry.get('lastmod')))
        return dict(url=url, depth=depth, status='ok', http_status=status, kind=parsed['kind'],
                    etag=etag, digest=digest, compressed=compressed, children=children)

    def _commit_subgraph(self, run_id, url, observation):
        previous = self.store.get_subgraph(url)
        expected_version = previous['version'] if previous else None
        return self.store.complete_subgraph(
            url, observation['etag'], observation['digest'], run_id, expected_version)

    # -- 汇总与可解释输出 -------------------------------------------------

    def summarize(self, run_id):
        run = self.store.get_run(run_id)
        nodes = self.store.list_nodes(run_id)
        edges = self.store.list_edges(run_id)
        leaves = self.store.list_leaves(run_id)
        future_claims, bad_claims = [], []
        for leaf in leaves:
            claim = parse_claimed_time(leaf['lastmod_claim'])
            if claim is None:
                continue
            if not claim['parseable']:
                bad_claims.append(dict(url=leaf['url'], lastmod=claim['raw']))
            elif claim['in_future']:
                future_claims.append(dict(url=leaf['url'], lastmod=claim['raw']))
        return {
            'run_id': run_id,
            'state': run['state'],
            'complete_view': bool(run['complete_view']),
            'incomplete_reasons': json.loads(run['incomplete_reasons']),
            'nodes': [dict(url=n['url'], depth=n['depth'], kind=n['kind'],
                           status=n['observe_status'], http_status=n['http_status'],
                           etag=n['etag'], digest=n['digest'], error=n['error'])
                      for n in nodes],
            'edges': [dict(parent=e['parent_url'], child=e['child_url'],
                           cyclic=bool(e['cyclic']), duplicate=bool(e['duplicate']))
                      for e in edges if e['parent_url']],
            'leaves': sorted(leaf['url'] for leaf in leaves),
            'future_lastmod_claims': future_claims,
            'unparseable_lastmod': bad_claims,
            'watermark_note': 'lastmod 仅为来源声称时间，未用于 ETag 判定或扫描水位',
        }

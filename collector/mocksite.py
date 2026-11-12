"""由 fixtures/sitemaps.json 驱动的本地模拟站点。

站点把抽象资源映射到真实回环 URL：
    root    -> {base}/sitemap.xml
    a.xml   -> {base}/maps/a.xml 等
所有正文以 gzip 下发（.gz 语义），支持 If-None-Match 条件请求；
events 中的覆盖（etag/status/references）按 phase() 切换，用来重现滚动替换窗口。
本模块只合成测试数据，不发起任何外部请求。
"""
import gzip
import json
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

NS = 'http://www.sitemaps.org/schemas/sitemap/0.9'

# 为演示“lastmod 只是来源声称的时间”，换代后的 a.xml 携带一个未来日期；
# 遍历器不会把它当作扫描水位。
_LASTMOD = {
    'g1': '2026-01-01T00:00:00Z',
    'g2': '2099-01-01T00:00:00Z',
}


def load_fixture(path):
    with open(path, encoding='utf-8') as handle:
        data = json.load(handle)
    required = ('generations', 'events', 'limits')
    missing = [key for key in required if key not in data]
    if missing:
        raise ValueError(f'fixture 缺少字段: {",".join(missing)}')
    return data


def resource_path(resource):
    return '/sitemap.xml' if resource == 'root' else '/maps/' + resource.lstrip('/')


def _index_xml(base, children, references=None):
    parts = [f'<sitemapindex xmlns="{NS}">']
    for resource in children:
        loc = base + resource_path(resource)
        parts.append(f'<sitemap><loc>{loc}</loc><lastmod>{_LASTMOD["g1"]}</lastmod></sitemap>')
    if references:
        for resource in references:
            loc = base + resource_path(resource)
            parts.append(f'<sitemap><loc>{loc}</loc></sitemap>')
    parts.append('</sitemapindex>')
    return ''.join(parts).encode('utf-8')


def _urlset_xml(children, lastmod):
    parts = [f'<urlset xmlns="{NS}">']
    for child in children:
        parts.append(f'<url><loc>{child}</loc><lastmod>{lastmod}</lastmod></url>')
    parts.append('</urlset>')
    return ''.join(parts).encode('utf-8')


class MockSite:
    """根据当前代际与事件覆盖生成响应内容。"""

    def __init__(self, fixture, base):
        self.fixture = fixture
        self.base = base
        self.generations = {gen['name']: gen for gen in fixture['generations']}
        self.events = {event['resource']: event for event in fixture['events']}
        self.phase_name = fixture['generations'][0]['name']
        self.rolling = False
        self.lock = threading.Lock()

    def phase(self, name, rolling=False):
        with self.lock:
            if name not in self.generations:
                raise KeyError(f'未知代际: {name}')
            self.phase_name = name
            self.rolling = rolling

    def _generation(self, resource):
        return self.generations[self.phase_name]

    def render(self, resource):
        """返回 (status, etag, body_bytes, compressed)。"""
        gen = self._generation(resource)
        event = self.events.get(resource)
        # 状态码覆盖只对当前代际仍在引用的资源生效；换代后消失的子图应表现为 404
        if self.rolling and event and 'status' in event and resource in gen['root']:
            return event['status'], None, b'service unavailable', False
        if resource == 'root':
            if self.rolling and event and 'etag' in event:
                # 滚动窗口：旧索引（保留旧 ETag）暂时仍在服务，子文件指向当前代际；
                # 事件显式声明 references 时附加这些引用（用于制造循环等窗口态）。
                body = _index_xml(self.base, gen['root'], event.get('references'))
                return 200, event['etag'], gzip.compress(body), True
            body = _index_xml(self.base, gen['root'])
            return 200, gen['name'], gzip.compress(body), True
        if resource not in gen['root']:
            return 404, None, b'not found', False
        if self.rolling and event and 'references' in event:
            # 子文件在滚动窗口中被替换成索引，制造指向 root 的循环引用
            body = _index_xml(self.base, [], references=event['references'])
            etag = f'{self.phase_name}-{resource}-index'
            return 200, etag, gzip.compress(body), True
        leaves = gen['children'].get(resource, [])
        body = _urlset_xml(leaves, _LASTMOD[gen['name']])
        etag = f'{gen["name"]}-{resource}'
        # 滚动窗口中事件可临时宣告不同的实体标识；窗口关闭后恢复代际真实 ETag
        if self.rolling and event and event.get('etag'):
            etag = event['etag']
        return 200, etag, gzip.compress(body), True

    def resolve(self, path):
        if path == '/sitemap.xml':
            return 'root'
        prefix = '/maps/'
        if path.startswith(prefix):
            return path[len(prefix):]
        return None

    def limits(self):
        return dict(self.fixture['limits'])


@contextmanager
def mock_site_server(fixture):
    site = MockSite(fixture, base=None)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format_string, *arguments):
            return None

        def do_GET(self):
            resource = site.resolve(urlsplit(self.path).path)
            if resource is None:
                self._reply(404, None, b'unknown route', False)
                return
            status, etag, body, compressed = site.render(resource)
            if status == 200 and etag and self.headers.get('If-None-Match') == etag:
                self.send_response(304)
                self.send_header('ETag', etag)
                self.send_header('Content-Length', '0')
                self.end_headers()
                return
            headers = {}
            if etag:
                headers['ETag'] = etag
            if compressed:
                headers['Content-Encoding'] = 'gzip'
            self._reply(status, headers, body, compressed)

        def _reply(self, status, headers, body, compressed):
            self.send_response(status)
            for name, value in (headers or {}).items():
                self.send_header(name, value)
            self.send_header('Content-Type', 'application/xml; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f'http://127.0.0.1:{server.server_port}'
    site.base = base
    try:
        yield site
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

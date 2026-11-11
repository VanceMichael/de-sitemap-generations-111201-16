import gzip
from urllib.parse import urlsplit
from xml.etree import ElementTree


def parse_sitemap(raw, compressed=False):
    if compressed:
        raw = gzip.decompress(raw)
    root = ElementTree.fromstring(raw)
    kind = root.tag.rsplit('}', 1)[-1]
    if kind not in ('urlset', 'sitemapindex'):
        raise ValueError('不支持的地图根节点')
    entries = []
    for child in root:
        values = {node.tag.rsplit('}', 1)[-1]: node.text for node in child}
        location = values.get('loc', '')
        if urlsplit(location).scheme not in ('http', 'https'):
            raise ValueError('地图地址无效')
        entries.append(dict(url=location, lastmod=values.get('lastmod')))
    return dict(kind=kind, entries=entries)


def collect(payload):
    parsed = parse_sitemap(payload['xml'].encode('utf-8'))
    return [dict(key=row['url'], kind=parsed['kind'], lastmod=row['lastmod']) for row in parsed['entries']]

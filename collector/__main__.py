import argparse
import json
from pathlib import Path
import tempfile

from .domain import collect
from .storage import Store
from .transport import Client, replay_server


def main():
    parser = argparse.ArgumentParser(description='本地采集、代际遍历与凭据查看')
    parser.add_argument('command',
                        choices=['demo', 'import', 'show', 'serve', 'crawl-demo', 'crawl'])
    parser.add_argument('--input', default='fixtures/sample.json')
    parser.add_argument('--fixture', default='fixtures/sitemaps.json')
    parser.add_argument('--db', default='var/records.sqlite3')
    parser.add_argument('--port', type=int, default=8080)
    parser.add_argument('--root-url', help='crawl 命令的地图根地址，可重复传入')
    parser.add_argument('--run-id', default=None)
    arguments = parser.parse_args()
    if arguments.command == 'serve':
        from .api import serve
        serve(arguments.db, arguments.port)
        return
    if arguments.command == 'crawl-demo':
        from .scenario import run_scenario
        with tempfile.TemporaryDirectory() as directory:
            report = run_scenario(arguments.fixture,
                                  str(Path(directory) / 'crawl.sqlite3'))
            print(json.dumps(report, ensure_ascii=False, indent=2))
            print('\n===== 滚动换代说明 =====')
            print(report['narrative'])
        return
    if arguments.command == 'crawl':
        from .crawlstore import CrawlStore
        from .crawler import Crawler, Fetcher
        if not arguments.root_url:
            parser.error('crawl 需要至少一个 --root-url')
        Path(arguments.db).parent.mkdir(parents=True, exist_ok=True)
        crawl_store = CrawlStore(arguments.db)
        try:
            run_id = arguments.run_id or f'run-{Path(arguments.db).stem}'
            summary = Crawler(crawl_store, Fetcher(Client())).run(run_id,
                                                                  [arguments.root_url])
            print(json.dumps(summary, ensure_ascii=False, indent=2))
        finally:
            crawl_store.close()
        return
    if arguments.command == 'demo':
        with tempfile.TemporaryDirectory() as directory:
            payload = Path(arguments.input).read_bytes()
            with replay_server({'/source': [dict(body=payload.decode('utf-8'))]}) as (base, calls):
                status, headers, raw = Client().request(base + '/source')
                if status != 200:
                    raise RuntimeError('本地来源未返回成功响应')
                store = Store(Path(directory) / 'demo.sqlite3')
                rows = collect(json.loads(raw))
                digest = store.ingest('本地演示来源', raw, rows)
                print(json.dumps(dict(digest=digest, records=store.records(), requests=len(calls)), ensure_ascii=False, indent=2))
                store.close()
        return
    Path(arguments.db).parent.mkdir(parents=True, exist_ok=True)
    store = Store(arguments.db)
    try:
        if arguments.command == 'import':
            raw = Path(arguments.input).read_bytes()
            store.ingest(str(Path(arguments.input)), raw, collect(json.loads(raw)))
        print(json.dumps(store.records(), ensure_ascii=False, indent=2))
    finally:
        store.close()


if __name__ == '__main__':
    main()

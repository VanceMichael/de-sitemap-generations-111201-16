import tempfile
import unittest
from pathlib import Path
from urllib.parse import urlsplit

from collector.crawlstore import CrawlStore, RunLocked
from collector.crawler import Crawler, Fetcher, decode_body, parse_claimed_time
from collector.mocksite import load_fixture, mock_site_server, resource_path
from collector.rollover import RolloverController
from collector.scenario import run_scenario
from collector.transport import Client

FIXTURE = 'fixtures/sitemaps.json'


def leaf_paths(urls):
    return sorted(urlsplit(url).path for url in urls)


class CrawlStoreTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.store = CrawlStore(Path(self.temporary.name) / 'crawl.sqlite3')

    def tearDown(self):
        self.store.close()
        self.temporary.cleanup()

    def test_lock_is_exclusive(self):
        self.store.acquire_lock('r1', 'proc-a')
        with self.assertRaises(RunLocked):
            self.store.acquire_lock('r1', 'proc-b')
        self.store.release_lock('r1', 'proc-a')
        self.store.acquire_lock('r1', 'proc-b')

    def test_subgraph_cas_rejects_stale_version(self):
        url = 'https://data.example/maps/a.xml'
        self.assertEqual(
            self.store.complete_subgraph(url, 'e1', 'd1', 'run-1', None), 'created')
        version = self.store.get_subgraph(url)['version']
        # 旧进程拿着过期版本号提交，必须失败
        self.assertEqual(
            self.store.complete_subgraph(url, 'old', 'old', 'stale-run', version - 1), 'stale')
        # 当前版本号提交新实体成功，版本递增
        self.assertEqual(
            self.store.complete_subgraph(url, 'e2', 'd2', 'run-2', version), 'updated')
        self.assertEqual(self.store.get_subgraph(url)['completed_run'], 'run-2')
        self.assertEqual(self.store.get_subgraph(url)['version'], version + 1)
        # 相同实体重复完成只是刷新完成者，不增加版本
        self.assertEqual(
            self.store.complete_subgraph(url, 'e2', 'd2', 'run-3', version + 1), 'unchanged')

    def test_two_missing_complete_views_required_for_withdrawal(self):
        present_v1 = {'/1', '/2', '/3'}
        present_v2 = {'/1', '/3'}
        present_v3 = {'/1'}
        self.store.start_run('r1', ['root'], dict(depth=8, nodes=100))
        self.store.finish_run('r1', 'completed', True, [])
        self.store.apply_complete_view('r1', present_v1)
        self.store.start_run('r2', ['root'], dict(depth=8, nodes=100))
        self.store.finish_run('r2', 'completed', True, [])
        self.store.apply_complete_view('r2', present_v2)
        item = self.store.get_lifecycle('/2')
        self.assertEqual(item['state'], 'candidate')
        self.assertEqual(item['miss_streak'], 1)
        # 不完整视图不允许进入生命周期，因此不能贡献第二次缺席
        self.store.start_run('rx', ['root'], dict(depth=8, nodes=100))
        self.store.finish_run('rx', 'incomplete', False, [dict(reason='http_error')])
        self.assertEqual(self.store.get_lifecycle('/2')['state'], 'candidate')
        # 第二次独立完整视图缺席 -> 撤下
        self.store.start_run('r3', ['root'], dict(depth=8, nodes=100))
        self.store.finish_run('r3', 'completed', True, [])
        self.store.apply_complete_view('r3', present_v3)
        item = self.store.get_lifecycle('/2')
        self.assertEqual(item['state'], 'withdrawn')
        self.assertEqual(item['miss_streak'], 2)
        self.assertEqual(self.store.get_lifecycle('/3')['state'], 'candidate')
        # 重新出现则复活
        self.store.start_run('r4', ['root'], dict(depth=8, nodes=100))
        self.store.finish_run('r4', 'completed', True, [])
        self.store.revive_if_seen('r4', {'/1', '/2'})
        self.store.apply_complete_view('r4', {'/1', '/2'})
        self.assertEqual(self.store.get_lifecycle('/2')['state'], 'active')


class GzipAndClaimTests(unittest.TestCase):
    def test_gzip_detection_by_magic_and_header(self):
        import gzip
        body, compressed = decode_body(gzip.compress(b'<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"/>'), True)
        self.assertTrue(compressed)
        self.assertIn(b'urlset', body)
        body, compressed = decode_body(gzip.compress(b'x'), False)  # 即使头没声明也按魔数解压
        self.assertTrue(compressed)

    def test_future_lastmod_is_never_watermark(self):
        claim = parse_claimed_time('2099-01-01T00:00:00Z')
        self.assertTrue(claim['parseable'])
        self.assertTrue(claim['in_future'])
        self.assertIsNone(parse_claimed_time(None))
        bad = parse_claimed_time('not-a-date')
        self.assertFalse(bad['parseable'])


class MockSiteAndCrawlerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.fixture = load_fixture(FIXTURE)
        self._db_seq = 0

    def tearDown(self):
        self.temporary.cleanup()

    def _store(self):
        # 每个用例（及需要独立库的步骤）使用独立文件，避免边表跨用例污染
        self._db_seq += 1
        return CrawlStore(Path(self.temporary.name) / f'crawl-{self._db_seq}.sqlite3')

    def test_full_g1_traversal_is_complete(self):
        with mock_site_server(self.fixture) as site:
            site.phase('g1')
            store = self._store()
            try:
                summary = Crawler(store, Fetcher(Client()), site.limits()).run(
                    'g1', [site.base + resource_path('root')])
            finally:
                store.close()
        self.assertTrue(summary['complete_view'], summary['incomplete_reasons'])
        self.assertEqual(leaf_paths(summary['leaves']), ['/1', '/2', '/3'])
        self.assertTrue(all(node['etag'] for node in summary['nodes']))
        self.assertTrue(all(node['digest'] for node in summary['nodes']))

    def test_rolling_window_is_incomplete_and_keeps_entries(self):
        with mock_site_server(self.fixture) as site:
            site.phase('g1', rolling=True)
            store = self._store()
            try:
                summary = Crawler(store, Fetcher(Client()), site.limits()).run(
                    'roll', [site.base + resource_path('root')])
                controller = RolloverController(store)
                decision = controller.adopt_view(summary)
            finally:
                store.close()
        self.assertFalse(summary['complete_view'])
        reasons = {item['reason'] for item in summary['incomplete_reasons']}
        self.assertIn('http_error', reasons)
        self.assertEqual(decision['action'], 'no_evidence')

    def test_cycle_reference_is_recorded_but_not_followed(self):
        with mock_site_server(self.fixture) as site:
            site.phase('g2', rolling=True)
            store = self._store()
            try:
                summary = Crawler(store, Fetcher(Client()), site.limits()).run(
                    'cycle', [site.base + resource_path('root')])
            finally:
                store.close()
        self.assertTrue(summary['complete_view'], summary['incomplete_reasons'])
        cyclic = [edge for edge in summary['edges'] if edge['cyclic']]
        self.assertTrue(cyclic)
        # 循环边的目标未被重复获取：root 只观察一次
        root_nodes = [n for n in summary['nodes']
                      if n['url'].endswith(resource_path('root'))]
        self.assertEqual(len(root_nodes), 1)

    def test_duplicate_url_is_kept_as_separate_reference(self):
        import gzip
        from collector.transport import replay_server
        xml = ('<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
               '<sitemap><loc>/child.xml</loc></sitemap>'
               '<sitemap><loc>/child.xml</loc></sitemap></sitemapindex>')
        child = ('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
                 '<url><loc>http://data/1</loc></url></urlset>')
        routes = {
            '/root.xml': [dict(body=gzip.compress(xml.encode()),
                               headers={'Content-Encoding': 'gzip', 'ETag': 'r'})],
            '/child.xml': [dict(body=gzip.compress(child.encode()),
                                headers={'Content-Encoding': 'gzip', 'ETag': 'c'})],
        }
        with replay_server(routes) as (base, calls):
            store = self._store()
            try:
                summary = Crawler(store, Fetcher(Client()), dict(depth=8, nodes=100)).run(
                    'dup', [base + '/root.xml'])
                edges = store.list_edges('dup')
            finally:
                store.close()
        self.assertTrue(summary['complete_view'])
        child_edges = [e for e in edges if e['child_url'] == base + '/child.xml']
        self.assertEqual(len(child_edges), 2)
        self.assertTrue(any(e['duplicate'] for e in child_edges))

    def test_node_and_depth_limits_make_view_incomplete(self):
        import gzip
        from collector.transport import replay_server
        root = ('<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
                '<sitemap><loc>http://x/a.xml</loc></sitemap>'
                '<sitemap><loc>http://x/b.xml</loc></sitemap></sitemapindex>')
        leaf = ('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
                '<url><loc>http://data/1</loc></url></urlset>')
        routes = {
            '/root.xml': [dict(body=gzip.compress(root.encode()),
                               headers={'Content-Encoding': 'gzip', 'ETag': 'r'})],
            '/a.xml': [dict(body=gzip.compress(leaf.encode()),
                            headers={'Content-Encoding': 'gzip', 'ETag': 'a'})],
            '/b.xml': [dict(body=gzip.compress(leaf.encode()),
                            headers={'Content-Encoding': 'gzip', 'ETag': 'b'})],
        }
        with replay_server(routes) as (base, calls):
            store = self._store()
            try:
                summary = Crawler(store, Fetcher(Client()),
                                  dict(depth=8, nodes=1)).run('limited', [base + '/root.xml'])
            finally:
                store.close()
        self.assertFalse(summary['complete_view'])
        self.assertEqual(summary['incomplete_reasons'][0]['reason'], 'node_limit')

    def test_parse_error_contributes_no_missing_evidence(self):
        from collector.transport import replay_server
        routes = {'/root.xml': [dict(body=b'not xml at all', headers={'ETag': 'r'})]}
        with replay_server(routes) as (base, calls):
            store = self._store()
            try:
                summary = Crawler(store, Fetcher(Client()),
                                  dict(depth=8, nodes=100)).run('bad', [base + '/root.xml'])
            finally:
                store.close()
        self.assertFalse(summary['complete_view'])
        self.assertEqual(summary['incomplete_reasons'][0]['reason'], 'parse_error')

    def test_resume_only_refetches_unfinished_nodes(self):
        with mock_site_server(self.fixture) as site:
            site.phase('g1')
            store = CrawlStore(Path(self.temporary.name) / 'resume.sqlite3')
            try:
                crawler = Crawler(store, Fetcher(Client()), site.limits())
                first = crawler.run('resume', [site.base + resource_path('root')])
                # 人为把 b.xml 的成功观察改回失败，制造未完成节点
                node = store.get_node('resume', site.base + resource_path('b.xml'))
                # 把 b.xml 的成功观察改回失败并清掉它登记的叶子，模拟遍历在该子图中断
                store.save_node('resume', dict(url=node['url'], depth=node['depth'],
                                               status='network_error', error='模拟中断'))
                store.connection.execute(
                    "DELETE FROM crawl_leaves WHERE run_id='resume' AND parent_url=?",
                    (node['url'],))
                pending_before = store.pending_targets('resume')
                self.assertEqual(pending_before, [site.base + resource_path('b.xml')])
                second = Crawler(store, Fetcher(Client()), site.limits()).run(
                    'resume', [site.base + resource_path('root')])
            finally:
                store.close()
        self.assertTrue(first['complete_view'])
        self.assertTrue(second['complete_view'])
        self.assertEqual(leaf_paths(second['leaves']), ['/1', '/2', '/3'])

    def test_conditional_request_reuses_entity_with_second_run(self):
        with mock_site_server(self.fixture) as site:
            site.phase('g1')
            store = self._store()
            try:
                first = Crawler(store, Fetcher(Client()), site.limits()).run(
                    'cond-1', [site.base + resource_path('root')])
                second = Crawler(store, Fetcher(Client()), site.limits()).run(
                    'cond-2', [site.base + resource_path('root')])
                nodes = store.list_nodes('cond-2')
            finally:
                store.close()
        self.assertTrue(first['complete_view'] and second['complete_view'])
        # 第二次所有节点都走 304 条件请求，仍然各自形成完整独立视图
        self.assertTrue(all(n['http_status'] == 304 for n in nodes))
        self.assertEqual(second['leaves'], first['leaves'])

    def test_run_lock_blocks_second_process(self):
        with mock_site_server(self.fixture) as site:
            site.phase('g1')
            store = self._store()
            try:
                store.acquire_lock('locked-run', 'other-process')
                with self.assertRaises(RunLocked):
                    Crawler(store, Fetcher(Client()), site.limits()).run(
                        'locked-run', [site.base + resource_path('root')])
            finally:
                store.close()

    def test_view_diff_and_reread_plan_across_generations(self):
        with mock_site_server(self.fixture) as site:
            store = self._store()
            try:
                site.phase('g1')
                Crawler(store, Fetcher(Client()), site.limits()).run(
                    'p1', [site.base + resource_path('root')])
                site.phase('g2')
                Crawler(store, Fetcher(Client()), site.limits()).run(
                    'p2', [site.base + resource_path('root')])
                controller = RolloverController(store)
                diff = controller.diff('p1', 'p2')
                plan = {item['url']: item['decision']
                        for item in controller.reread_plan('p1', 'p2')}
            finally:
                store.close()
        self.assertEqual(leaf_paths(diff['added']), ['/4'])
        self.assertEqual(leaf_paths(diff['removed']), ['/2', '/3'])
        self.assertEqual(leaf_paths(diff['retained']), ['/1'])
        decisions = set(plan.values())
        self.assertIn('reread_etag_changed', decisions)  # a.xml 换代
        self.assertTrue(any('dropped' in d for d in decisions))


class ScenarioTests(unittest.TestCase):
    def test_end_to_end_rollover_scenario(self):
        with tempfile.TemporaryDirectory() as directory:
            report = run_scenario(FIXTURE, str(Path(directory) / 'scenario.sqlite3'))
        views = {view['run_id']: view for view in report['views']}
        self.assertTrue(views['V1-g1-stable']['complete_view'])
        self.assertFalse(views['V2-rolling-incomplete']['complete_view'])
        self.assertTrue(views['V3-g2-complete']['complete_view'])
        self.assertTrue(views['V4-g2-independent']['complete_view'])
        # 不完整视图后 /2 /3 仍是保留状态
        after_v2 = report['decisions'][1]
        self.assertEqual(after_v2['action'], 'no_evidence')
        # V3 后仅候选，V4 独立完整视图后才撤下
        explanation = report['final_explanation']
        withdrawn = {urlsplit(item['url']).path for item in explanation['withdrawn']}
        self.assertEqual(withdrawn, {'/2', '/3'})
        retained = {urlsplit(item['url']).path for item in explanation['retained']}
        self.assertEqual(retained, {'/1', '/4'})
        # 撤下证据必须指向两个不同的独立完整视图
        for item in explanation['withdrawn']:
            missing_runs = [e['run'] for e in item['evidence']
                            if e['event'] == 'missing_in_complete_view']
            self.assertEqual(len(missing_runs), 2)
            self.assertEqual(len(set(missing_runs)), 2)
        # 中断恢复与循环引用专项
        self.assertTrue(report['special']['resume']['complete_view'])
        self.assertTrue(report['special']['cycle']['cyclic_edges'])
        self.assertEqual(
            report['special']['concurrent_cas']['stale_request_with_old_version'], 'stale')
        # 未来 lastmod 被识别但仅作声称值
        v3 = views['V3-g2-complete']
        self.assertTrue(v3['future_lastmod_claims'])
        self.assertIn('lastmod', v3['watermark_note'])


if __name__ == '__main__':
    unittest.main()

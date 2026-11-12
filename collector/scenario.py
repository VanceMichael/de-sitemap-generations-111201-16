"""基于 fixtures/sitemaps.json 的滚动换代全过程演示。

阶段设计对应本地地图滚动替换窗口：
  V1 g1 稳态        完整视图，/1 /2 /3 全部建立
  V2 g1 滚动窗口    b.xml 返回 503、旧索引仍在服务 -> 视图不完整，缺席条目全部保留
  V3 g2 新代际      完整视图只剩 /1 /4，/2 /3 首次在完整视图中缺席 -> 候选但保留
  V4 g2 再次完整    独立的第二次完整视图（条件请求 304 复用实体）-> /2 /3 才确认撤下
另含中断恢复与并发 CAS 两个专项演示。
"""
import json
import uuid

from .crawlstore import CrawlStore
from .crawler import Crawler, Fetcher
from .mocksite import load_fixture, mock_site_server, resource_path
from .rollover import RolloverController
from .transport import Client


class FlakyFetcher(Fetcher):
    """前 fail_on 次访问指定 URL 直接抛网络异常，用于演示中断恢复。"""

    def __init__(self, client, fail_url, fail_times):
        super().__init__(client)
        self.fail_url = fail_url
        self.remaining = fail_times
        self.attempts = 0

    def fetch(self, url, etag=None):
        self.attempts += 1
        if url == self.fail_url and self.remaining:
            self.remaining -= 1
            return dict(ok=False, network_error='ConnectionError', detail='模拟的网络中断')
        return super().fetch(url, etag)


def run_scenario(fixture_path, db_path):
    fixture = load_fixture(fixture_path)
    store = CrawlStore(db_path)
    report = {'fixture': fixture_path, 'views': [], 'decisions': [], 'diffs': [],
              'reread_plans': [], 'special': {}}
    try:
        with mock_site_server(fixture) as site:
            base = site.base
            root_url = base + resource_path('root')
            limits = site.limits()
            controller = RolloverController(store)

            def traverse(run_id):
                crawler = Crawler(store, Fetcher(Client()), limits)
                return crawler.run(run_id, [root_url])

            # V1：g1 稳态完整视图
            site.phase('g1', rolling=False)
            v1 = traverse('V1-g1-stable')
            report['views'].append(v1)
            report['decisions'].append(controller.adopt_view(v1))

            # V2：滚动替换窗口，b.xml 503，视图不完整
            site.phase('g1', rolling=True)
            v2 = traverse('V2-rolling-incomplete')
            report['views'].append(v2)
            report['decisions'].append(controller.adopt_view(v2))

            # V2-resume：同一棵树上的中断恢复（首次获取 b.xml 断网，随后续跑）
            site.phase('g1', rolling=False)
            flaky = FlakyFetcher(Client(), base + resource_path('b.xml'), fail_times=1)
            resuming = Crawler(store, flaky, limits)
            first_pass = resuming.run('V2b-resume', [root_url])
            resumed = resuming.run('V2b-resume', [root_url])
            report['special']['resume'] = {
                'run_id': 'V2b-resume',
                'interrupted_complete_view': first_pass['complete_view'],
                'interrupted_reasons': first_pass['incomplete_reasons'],
                'resumed_state': resumed['state'],
                'complete_view': resumed['complete_view'],
                'leaves': resumed['leaves'],
                'fetcher_calls': flaky.attempts,
                'note': '中断后按持久化边表只重取未完成节点，成功节点不重复获取',
            }

            # V3：g2 新代际完整视图（滚动事件全部关闭）
            site.phase('g2', rolling=False)
            v3 = traverse('V3-g2-complete')
            report['views'].append(v3)
            report['decisions'].append(controller.adopt_view(v3))
            report['reread_plans'].append({
                'from': 'V1-g1-stable', 'to': 'V3-g2-complete',
                'plan': controller.reread_plan('V1-g1-stable', 'V3-g2-complete'),
            })
            report['diffs'].append(controller.diff('V1-g1-stable', 'V3-g2-complete'))

            # V4：第二次独立完整视图（ETag 全部命中 304，仍独立完成遍历与判定）
            v4 = traverse('V4-g2-independent')
            report['views'].append(v4)
            report['decisions'].append(controller.adopt_view(v4))
            report['diffs'].append(controller.diff('V3-g2-complete', 'V4-g2-independent'))

            # 循环引用专项：滚动窗口中 c.xml 被替换为指向 root 的索引
            # （放在 V4 之后，避免窗口态 ETag 干扰条件请求演示）
            site.phase('g2', rolling=True)
            cyclic = traverse('V3b-cycle-detection')
            report['special']['cycle'] = {
                'run_id': 'V3b-cycle-detection',
                'complete_view': cyclic['complete_view'],
                'cyclic_edges': [edge for edge in cyclic['edges'] if edge['cyclic']],
                'note': '循环边被显式记录且不再展开，不构成不完整原因',
            }
            site.phase('g2', rolling=False)

            report['final_explanation'] = controller.explain()
            report['narrative'] = controller.narrative([
                dict(summary=v1), dict(summary=v2), dict(summary=v3), dict(summary=v4)])

        # 并发 CAS 专项：两个存储句柄模拟两个进程，旧版本号提交必须失败
        other = CrawlStore(db_path)
        try:
            target = resource_path('b.xml').lstrip('/')
            url = next(n['url'] for n in store.list_nodes('V1-g1-stable') if target in n['url'])
            current = store.get_subgraph(url)
            stale_result = other.complete_subgraph(
                url, 'stale-etag', 'stale-digest', 'old-process', current['version'] - 1)
            fresh_result = other.complete_subgraph(
                url, current['etag'], current['digest'], 'new-process', current['version'])
            report['special']['concurrent_cas'] = {
                'url': url,
                'stale_request_with_old_version': stale_result,
                'fresh_request_with_current_version': fresh_result,
                'winner_run': other.get_subgraph(url)['completed_run'],
                'note': '旧请求携带过期版本号无法覆盖子图完成状态',
            }
        finally:
            other.close()
        report['run_id'] = uuid.uuid4().hex
        return report
    finally:
        store.close()

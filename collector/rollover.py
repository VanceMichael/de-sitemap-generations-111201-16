"""滚动换代编排：视图差异、撤下确认与可解释说明。

撤下纪律集中在这里执行：
* 只有 complete_view=True 的遍历才能进入条目生命周期；
* 一次完整视图缺席只产生 candidate（保留、不撤下）；
* 连续两次可证明完整且相互独立的视图都缺席，才转为 withdrawn；
* “相互独立”要求两次遍历拥有不同 run_id、各自重新完成获取与判定；
  同一 run 内永远不会重复计数，任何不完整视图都不会写入缺失证据。
"""


class RolloverController:
    def __init__(self, store):
        self.store = store

    def adopt_view(self, summary):
        """把一次遍历结果应用到条目生命周期，返回本次决策说明。"""
        run_id = summary['run_id']
        present = set(summary['leaves'])
        if not summary['complete_view']:
            return {
                'run_id': run_id,
                'action': 'no_evidence',
                'why': '视图不可证明完整，缺席条目不产生任何缺失证据',
                'incomplete_reasons': summary['incomplete_reasons'],
                'leaves_present': sorted(present),
            }
        # 先让撤下后又回归的条目复活，再应用本视图的出现/缺席计数
        self.store.revive_if_seen(run_id, present)
        self.store.apply_complete_view(run_id, present)
        lifecycle = {item['url']: item for item in self.store.list_lifecycle()}
        candidates = sorted(url for url, item in lifecycle.items() if item['state'] == 'candidate')
        withdrawn = sorted(url for url, item in lifecycle.items() if item['state'] == 'withdrawn')
        return {
            'run_id': run_id,
            'action': 'complete_view_applied',
            'why': '可证明完整的独立视图，缺席条目按连续计数规则处理',
            'leaves_present': sorted(present),
            'new_candidates': candidates,
            'withdrawn': withdrawn,
        }

    def diff(self, earlier_run_id, later_run_id):
        return self.store.diff_views(earlier_run_id, later_run_id)

    def reread_plan(self, previous_run_id, current_run_id):
        return self.store.reread_plan(previous_run_id, current_run_id)

    def explain(self):
        """当前条目全景：保留中的、候选撤下（仍保留）、已撤下，各自带证据。"""
        active, candidates, withdrawn = [], [], []
        for item in self.store.list_lifecycle():
            entry = dict(url=item['url'], miss_streak=item['miss_streak'],
                         evidence=item['evidence'],
                         first_seen_run=item['first_seen_run'],
                         last_seen_run=item['last_seen_run'])
            if item['state'] == 'active':
                active.append(entry)
            elif item['state'] == 'candidate':
                candidates.append(entry)
            else:
                withdrawn.append(entry)
        return {
            'retained': sorted(active, key=lambda e: e['url']),
            'candidate_not_yet_removed': sorted(candidates, key=lambda e: e['url']),
            'withdrawn': sorted(withdrawn, key=lambda e: e['url']),
            'rule': '连续两次可证明完整且相互独立的视图均缺席才撤下；'
                    '不完整视图（限额/解析/网络失败）不贡献缺失证据',
        }

    def narrative(self, views):
        """根据一系列已执行视图生成“本地地图滚动换代”的条目保留/撤下说明。"""
        lines = []
        for view in views:
            run_id = view['summary']['run_id']
            if view['summary']['complete_view']:
                lines.append(
                    f"视图 {run_id}：完整可信，包含 {len(view['summary']['leaves'])} 个条目；"
                    f"本视图可作为出现或缺席证据。")
            else:
                reasons = '；'.join(
                    f"{r['reason']}({r.get('url', '')})" for r in view['summary']['incomplete_reasons'])
                lines.append(
                    f"视图 {run_id}：不完整（{reasons}），期间未扫到的条目一律保留，"
                    f"不产生缺失证据。")
        explain = self.explain()
        lines.append('最终判定：')
        lines.append('  保留中的条目: ' + (', '.join(e['url'] for e in explain['retained']) or '无'))
        lines.append('  候选但仍保留（仅一次完整视图缺席）: '
                     + (', '.join(e['url'] for e in explain['candidate_not_yet_removed']) or '无'))
        lines.append('  已撤下（两次独立完整视图均缺席）: '
                     + (', '.join(e['url'] for e in explain['withdrawn']) or '无'))
        return '\n'.join(lines)

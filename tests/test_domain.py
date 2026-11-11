import json
from pathlib import Path
import unittest

from collector.domain import *


class DomainTests(unittest.TestCase):
    def setUp(self):
        self.payload = json.loads(Path('fixtures/sample.json').read_text())

    def test_domain_behavior(self):
        raw = self.payload['xml'].encode()
        self.assertEqual(parse_sitemap(gzip.compress(raw), True)['kind'], 'urlset')
        self.assertEqual(len(collect(self.payload)), 1)
        with self.assertRaises(ValueError):
            parse_sitemap(b'<other/>')

    def test_deterministic_observations(self):
        self.assertEqual(collect(self.payload), collect(self.payload))

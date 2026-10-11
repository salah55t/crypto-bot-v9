# -*- coding: utf-8 -*-
"""
test_v9380.py — اختبارات V9.38.0: زر تصفير الإحصائيات في لوحة التحكم
  1) الإصدار + وجود المسار /api/stats/reset (POST) في خريطة المسارات
  2) الحراسة: بلا confirm → 400 | confirm خاطئ → 400 | DB ساقط → 500 | صفقات مفتوحة → 409
  3) المسار السعيد (FakeConn): أرشفة bot_state قبل الحذف + DELETE signals/notifications/
     bot_state(بمفاتيح محددة فقط) + commit — والأرشفة الأخيرة لا تُحذف
  4) تنظيف الذاكرة: صفقات مفتوحة/إشعارات/أقفال الحماية/حاكم المخاطر/تقاعد الاستراتيجيات
     + منع إعادة تحميل حالة قديمة (_risk_gov_loaded/_strategy_retire_loaded) + مسح Redis
  5) الواجهة: بطاقة منطقة الخطر + الزر + resetStats() مع مودال التأكيد المزدوج
     + تحديث البطاقات بعد النجاح + الإصدار V9.38.0 في HTML
  6) انحدار سريع: ما لم يُمس — 8 استراتيجيات حية والدوال المحذوفة سابقًا غائبة
"""
import sys, os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('LOG_LEVEL', 'ERROR')
os.environ.setdefault('BINANCE_API_KEY', 'test_key_dummy')
os.environ.setdefault('BINANCE_API_SECRET', 'test_secret_dummy')
os.environ.pop('DASHBOARD_USERNAME', None)
os.environ.pop('DASHBOARD_PASSWORD', None)

import vnz  # noqa: E402
vnz.logger.disabled = True
vnz.log_and_notify = lambda *a, **k: None
vnz.send_telegram_message = lambda *a, **k: None

PASS, FAIL = [], []


def check(name, cond, extra=''):
    (PASS if cond else FAIL).append(name)
    print(('✅ ' if cond else '❌ ') + name + (f' — {extra}' if (extra and not cond) else ''))


# ============ 1) الإصدار والمسار ============
print('═══ 1) الإصدار والمسار ═══')
check('الإصدار V9.38.0', vnz.APP_VERSION == 'V9.38.0', vnz.APP_VERSION)
check('HTML يحمل الإصدار V9.38.0', vnz.get_dashboard_html().find('V9.38.0') > 0)
rules = {str(r): r.methods for r in vnz.app.url_map.iter_rules()}
check('المسار /api/stats/reset مسجل', '/api/stats/reset' in rules, str(sorted(rules))[:200])
check('المسار يقبل POST فقط (مع HEAD/OPTIONS التلقائية)',
      '/api/stats/reset' in rules and 'POST' in rules['/api/stats/reset'] and 'GET' not in rules['/api/stats/reset'])

# ============ تجهيزات الاختبار ============
client = vnz.app.test_client()


class FakeCur:
    def __init__(self, parent):
        self.p = parent
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        norm = ' '.join(sql.split())
        self.p.calls.append(norm[:160])
        self.p.params_log.append(params)
        s = norm.upper()
        if s.startswith('SELECT COUNT'):
            self._one = self.p.count_row
        elif s.startswith('SELECT * FROM SIGNALS'):
            self._all = self.p.trades_rows
        elif s.startswith('SELECT * FROM NOTIFICATIONS'):
            self._all = []
        if s.startswith('DELETE') or s.startswith('INSERT'):
            self.rowcount = self.p.del_count

    def fetchone(self):
        return getattr(self, '_one', {'c': 0})

    def fetchall(self):
        return getattr(self, '_all', [])


class FakeConn:
    def __init__(self):
        self.calls = []
        self.params_log = []
        self.count_row = {'c': 0}
        self.trades_rows = []
        self.del_count = 1
        self.committed = False
        self.rolled_back = False

    def cursor(self, *a, **k):
        return FakeCur(self)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


class FakeRedis:
    def __init__(self):
        self.deleted = []

    def delete(self, *keys):
        self.deleted.extend(keys)
        return len(keys)


def prime_memory():
    vnz.open_signals_cache.clear()
    vnz.open_signals_cache['BTCUSDT'] = {'id': 1}
    vnz.notifications_cache.clear()
    vnz.notifications_cache.append({'type': 'T', 'message': 'old'})
    vnz._protection_locks.clear()
    vnz._protection_locks.append({'scope': 'global'})
    vnz._protection_close_log.clear()
    vnz._protection_close_log.append({'ts': 1.0, 'symbol': 'X', 'reason': 'stop_loss', 'profit_pct': -1.0})
    vnz._risk_gov_state.clear()
    vnz._risk_gov_state.update({'date': '2020-01-01', 'daily_realized': -5.0, 'global_peak': 9.9,
                                'global_profit_locked': True, 'global_hwm_locked': True, 'strat': {'S': {}}})
    vnz._risk_gov_loaded = False
    vnz._strategy_retire_state.clear()
    vnz._strategy_retire_state['EMA_RSI_Cross'] = {'suspended': True}
    vnz._strategy_retire_loaded = False


# ============ 2) الحراسة ============
print('═══ 2) الحراسة الأربعة ═══')
r = client.post('/api/stats/reset')
check('بلا confirm → 400', r.status_code == 400, str(r.status_code))
r = client.post('/api/stats/reset', json={'confirm': 'yes'})
check('confirm خاطئ → 400', r.status_code == 400, str(r.status_code))

real_check = vnz.check_db_connection
real_conn = vnz.conn
vnz.check_db_connection = lambda: False
vnz.conn = None
r = client.post('/api/stats/reset', json={'confirm': 'RESET'})
check('DB ساقط → 500', r.status_code == 500, str(r.status_code))
vnz.check_db_connection = lambda: True

fc = FakeConn()
fc.count_row = {'c': 2}
vnz.conn = fc
r = client.post('/api/stats/reset', json={'confirm': 'RESET'})
check('صفقتان مفتوحتان → 409', r.status_code == 409, f"{r.status_code} {r.get_json(silent=True)}")
check('رفض الحارس لم يحذف شيئًا (بلا DELETE)', not any(c.upper().startswith('DELETE') for c in fc.calls))
check('رفض الحارس لم يلتزم commit', not fc.committed)

# ============ 3) المسار السعيد ============
print('═══ 3) المسار السعيد — أرشفة ثم حذف ═══')
fc = FakeConn()
fc.trades_rows = [{'id': 1, 'symbol': 'NMRUSDT', 'profit_percentage': 0.8}]
vnz.conn = fc
fake_redis = FakeRedis()
real_redis = vnz.redis_client
vnz.redis_client = fake_redis
prime_memory()
r = client.post('/api/stats/reset', json={'confirm': 'RESET'})
body = r.get_json(silent=True) or {}
check('نجاح 200', r.status_code == 200, f"{r.status_code} {body}")
check('commit تم', fc.committed)
check('أرشفة bot_state قبل الحذف (INSERT precedes DELETE)',
      any(c.upper().startswith('INSERT INTO BOT_STATE') for c in fc.calls)
      and any(c.upper().startswith('DELETE FROM SIGNALS') for c in fc.calls)
      and fc.calls.index(next(c for c in fc.calls if c.upper().startswith('INSERT INTO BOT_STATE')))
      < fc.calls.index(next(c for c in fc.calls if c.upper().startswith('DELETE FROM SIGNALS'))))
insert_idx = next(i for i, c in enumerate(fc.calls) if c.upper().startswith('INSERT INTO BOT_STATE'))
insert_params = fc.params_log[insert_idx] or []
check('مفتاح الأرشفة stats_archive_* (كمعامل أول)',
      len(insert_params) >= 2 and str(insert_params[0]).startswith('stats_archive_'), str(insert_params)[:120])
check('حمولة الأرشفة JSONB تحتوي closed_trades',
      len(insert_params) >= 2 and 'closed_trades' in str(insert_params[1]))
check('أرشفة تحمل عدد الصفقات', body.get('archived_trades') == 1, str(body))
check('حذف signals', any(c.upper().startswith('DELETE FROM SIGNALS') for c in fc.calls))
check('حذف notifications', any(c.upper().startswith('DELETE FROM NOTIFICATIONS') for c in fc.calls))
state_del = next((c for c in fc.calls if c.upper().startswith('DELETE FROM BOT_STATE')), '')
check('حذف bot_state بمفاتيح محددة فقط (risk_governor/strategy_retirement)',
      'RISK_GOVERNOR' in state_del.upper() and 'STRATEGY_RETIREMENT' in state_del.upper(), state_del)
check('مفتاح الأرشفة محصّن من الحذف (ليس في قائمة الحذف)',
      'STATS_ARCHIVE' not in state_del.upper(), state_del)
check('مسح أسعار Redis', fake_redis.deleted == [vnz.REDIS_PRICES_HASH_NAME], str(fake_redis.deleted))
check('الاستجابة تحمل archive_key', str(body.get('archive_key', '')).startswith('stats_archive_'))

# ============ 4) تنظيف الذاكرة ============
print('═══ 4) تنظيف الذاكرة والأنظمة الحاكمة ═══')
check('كاش الصفقات المفتوحة أُفرغ', len(vnz.open_signals_cache) == 0)
check('كاش الإشعارات أُفرغ', len(vnz.notifications_cache) == 0)
check('أقفال الحماية أُفرغت', len(vnz._protection_locks) == 0)
check('سجل إغلاقات الحماية أُفرغ', len(vnz._protection_close_log) == 0)
check('حاكم المخاطر أعيد لوضع الولادة (صفر أخطاء/أقفال)',
      vnz._risk_gov_state.get('daily_realized') == 0.0 and vnz._risk_gov_state.get('global_peak') == 0.0
      and not vnz._risk_gov_state.get('global_profit_locked') and not vnz._risk_gov_state.get('global_hwm_locked')
      and vnz._risk_gov_state.get('strat') == {}, str(vnz._risk_gov_state)[:120])
check('حاكم المخاطر لن يعيد تحميل حالة قديمة', vnz._risk_gov_loaded is True)
check('تقاعد الاستراتيجيات أُفرغ', len(vnz._strategy_retire_state) == 0)
check('حاكم التقاعد لن يعيد تحميل حالة قديمة', vnz._strategy_retire_loaded is True)

# ============ 5) الواجهة ============
print('═══ 5) واجهة اللوحة ═══')
html = vnz.get_dashboard_html()
check('بطاقة منطقة الخطر موجودة', 'منطقة الخطر — تصفير الإحصائيات' in html)
check('زر التصفير يستدعي resetStats()', 'onclick="resetStats()"' in html)
check('resetStats() معرفة', 'function resetStats()' in html)
check('تأكيد مزدوج عبر showConfirmation', html.find('showConfirmation') < html.find('function resetStats()')
      and "showConfirmation('تصفير كل الإحصائيات'" in html)
check('النداء يرسل confirm=RESET', "confirm: 'RESET'" in html)
check('تحديث البطاقات بعد النجاح (stats/pnl/chart/market)',
      'updateStats(); updateStrategyPnl(); updatePnlChart(); updateMarketStatus();' in html)
check('حاوية تغذية راجعة reset-feedback', 'id="reset-feedback"' in html)
check('تحذير الصفقات المفتوحة في الشرح', 'لا يعمل أثناء وجود صفقات مفتوحة' in html)

# ============ 6) انحدار سريع — ما لم يُمس ============
print('═══ 6) انحدار سريع ═══')
vnz.check_db_connection = real_check
vnz.conn = real_conn
vnz.redis_client = real_redis
live_scan = {n for _, _, n in vnz._evidence_strategy_table()}
check('جدول المسح ما يزال 8 استراتيجيات', len(live_scan) == 8, str(sorted(live_scan)))
for fn in ('check_bb_stoch_strategy_enhanced', 'check_macd_ema_strategy', 'ft_entry_bandtastic'):
    check(f'دالة {fn} ما تزال محذوفة (V9.37)', not hasattr(vnz, fn))
check('مسارات سابقة سليمة (toggle/protections/pnl_history)',
      '/api/trading/toggle' in rules and '/api/protections' in rules and '/api/pnl_history' in rules)

print(f"\n═══ النتيجة: {len(PASS)} نجاح / {len(FAIL)} فشل ═══")
if FAIL:
    print('الفاشلة: ' + ' | '.join(FAIL))
    sys.exit(1)

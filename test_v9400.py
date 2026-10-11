# -*- coding: utf-8 -*-
"""
test_v9400.py — اختبارات V9.40.0: علاج "أرباح ضئيلة في الصعود + فرص ضائعة"
  1) الإصدار + سلّما الأرباح (الافتراضي والصاعد) موجودان بقيم صحيحة
  2) سلّم الصعود يعمل حسب الريم: STRONG_UPTREND → أدراج أوسع (قفل أدنى عند نفس القمة)
     وخارج الصعود السلّم الافتراضي المعاير كما هو | تحت أول درجة → None
  3) التهدئة حسب النتيجة: إغلاق رابح → 60د | خاسر → 240د | قفل أطول قائم لا يُقصَّر
  4) عرض كل الاستراتيجيات الحية (9) في /api/strategy_pnl حتى بلا صفقات — Elliott مرئي
  5) حفظ/استعادة مفتاح التداول: toggle يكتب bot_state | الاستعادة من DB | الافتراضي ورقي
     + تصفير الإحصائيات لا يحذف مفتاح trading_enabled (خارج القائمة البيضاء)
  6) الواجهة: شارة الوضع الورقي + إصدار V9.40.0
  7) انحدار: 9 استراتيجيات حية + Elliott_Impulse مسجل في الحلقة + المسارات الحية
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
        self.p.calls.append(norm[:170])
        self.p.params_log.append(params)
        s = norm.upper()
        if s.startswith('SELECT VALUE FROM BOT_STATE'):
            self._one = self.p.state_row
        elif s.startswith('SELECT'):
            self._all = self.p.select_rows
        if s.startswith('INSERT') or s.startswith('DELETE'):
            self.rowcount = 1

    def fetchone(self):
        return getattr(self, '_one', None)

    def fetchall(self):
        return getattr(self, '_all', [])


class FakeConn:
    def __init__(self):
        self.calls = []
        self.params_log = []
        self.state_row = None
        self.select_rows = []
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return FakeCur(self)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


# ============ 1) الإصدار والسلالم ============
print('═══ 1) الإصدار وسلالم قفل الأرباح ═══')
check('الإصدار V9.40.0', vnz.APP_VERSION == 'V9.40.0', vnz.APP_VERSION)
check('HTML يحمل الإصدار V9.40.0', vnz.get_dashboard_html().find('V9.40.0') > 0)
check('السلّم الافتراضي سليم (5 درجات تبدأ 0.75→0.30)',
      vnz.PROFIT_LOCK_LADDER[:1] == [(0.75, 0.30)] and len(vnz.PROFIT_LOCK_LADDER) == 5,
      str(vnz.PROFIT_LOCK_LADDER))
rally = vnz.PROFIT_LOCK_LADDER_RALLY
check('سلّم الصعود موجود بـ5 درجات', len(rally) == 5, str(rally))
check('سلّم الصعود: درجة أولى أوسع مساحة (قمة 0.75 قفل 0.20)',
      rally[0] == (0.75, 0.20), str(rally[0]))
check('سلّم الصعود: درجة ثانية عند قمة 1.50 (أبعد من 1.25 الافتراضي)',
      rally[1] == (1.50, 0.60), str(rally[1]))
check('سلّم الصعود: كل الأقفال ≥ الرسوم (لا قفل خاسر صافٍ)',
      all(lock >= 0.20 for _, lock in rally), str(rally))
rooms_rally = [round(pk - lk, 6) for pk, lk in rally]
rooms_def = [round(pk - lk, 6) for pk, lk in vnz.PROFIT_LOCK_LADDER]
check('مساحة التنفس من القمة أوسع في سلّم الصعود عند كل درجة',
      all(rr > rd for rr, rd in zip(rooms_rally, rooms_def)),
      f'rally={rooms_rally} def={rooms_def}')
check('تهدئة الإغلاق الرابح مفعّلة = 60د', vnz.PROTECTION_COOLDOWN_WIN_MIN == 60,
      str(vnz.PROTECTION_COOLDOWN_WIN_MIN))
check('تهدئة الخاسر تبقى 240د', vnz.PROTECTION_COOLDOWN_MIN == 240)


# ============ 2) السلّم حسب الريم ============
print('═══ 2) سلّم الصعود حسب الريم ═══')
vnz._market_is_rally = lambda: False
sl_def = vnz.profit_lock_ladder_stop(100.0, 101.0)   # قمة +1.0%
check('خارج الصعود: قمة 1.0% → قفل +0.30% (الافتراضي)', abs(sl_def - 100.30) < 1e-9, str(sl_def))
vnz._market_is_rally = lambda: True
sl_rally = vnz.profit_lock_ladder_stop(100.0, 101.0)
check('في صعود قوي: قمة 1.0% → قفل +0.20% فقط (مساحة أوسع)', abs(sl_rally - 100.20) < 1e-9, str(sl_rally))
check('في صعود قوي: قمة 1.0% يقف تحت درجة 1.50 → نفس الدرجة الأولى (لا قفز للدرجة الثانية)',
      sl_rally < sl_def)
sl_r2 = vnz.profit_lock_ladder_stop(100.0, 101.6)    # قمة +1.6% → درجة 1.50
check('في صعود قوي: قمة 1.6% → قفل +0.60%', abs(sl_r2 - 100.60) < 1e-9, str(sl_r2))
sl_r3 = vnz.profit_lock_ladder_stop(100.0, 102.6)    # قمة +2.6% → درجة 2.50
check('في صعود قوي: قمة 2.6% → قفل +1.30%', abs(sl_r3 - 101.30) < 1e-9, str(sl_r3))
sl_r5 = vnz.profit_lock_ladder_stop(100.0, 105.2)    # قمة +5.2% → درجة 5.00
check('في صعود قوي: قمة 5.2% → قفل +3.40%', abs(sl_r5 - 103.40) < 1e-9, str(sl_r5))
check('تحت أول درجة → None (كلا الوضعين)',
      vnz.profit_lock_ladder_stop(100.0, 100.5) is None)
vnz._market_is_rally = lambda: False
check('خارج الصعود: قمة 5.2% → قفل +3.00% (الافتراضي)',
      abs(vnz.profit_lock_ladder_stop(100.0, 105.2) - 103.0) < 1e-9)

# ============ 3) التهدئة حسب النتيجة ============
print('═══ 3) التهدئة حسب النتيجة ═══')
import time as _t


def _reset_protections():
    vnz._protection_close_log.clear()
    vnz._protection_locks.clear()


_reset_protections()
vnz.record_trade_close_for_protections('WINUSDT', 'stop_loss', 0.50)
lock_win = next((l for l in vnz._protection_locks if l['pair'] == 'WINUSDT'), None)
check('إغلاق رابح → تهدئة 60د فقط', lock_win is not None and lock_win['minutes'] == 60,
      str(lock_win))

_reset_protections()
vnz.record_trade_close_for_protections('LOSSUSDT', 'stop_loss', -2.10)
lock_loss = next((l for l in vnz._protection_locks if l['pair'] == 'LOSSUSDT'), None)
check('إغلاق خاسر → التهدئة الكاملة 240د', lock_loss is not None and lock_loss['minutes'] == 240,
      str(lock_loss))

_reset_protections()
vnz._add_protection_lock('pair', 'LONGUSDT', 240, 'قفل سابق أطول', 'LowProfitPairs')
vnz.record_trade_close_for_protections('LONGUSDT', 'stop_loss', 0.30)
lock_long = next((l for l in vnz._protection_locks if l['pair'] == 'LONGUSDT'), None)
check('قفل 240د قائم لا يُقصَّر بتهدئة رابح 60د',
      lock_long is not None and lock_long['minutes'] == 240, str(lock_long))

_reset_protections()
vnz.record_trade_close_for_protections('ZEROUSDT', 'stop_loss', 0.0)
lock_zero = next((l for l in vnz._protection_locks if l['pair'] == 'ZEROUSDT'), None)
check('إغلاق تعادل (0.0%) → المدة الكاملة 240د (الربح فقط يُقصّر)',
      lock_zero is not None and lock_zero['minutes'] == 240, str(lock_zero))
_reset_protections()

# ============ 4) جدول الأداء يعرض كل الاستراتيجيات ============
print('═══ 4) جدول أداء الاستراتيجيات — كل الحية مرئية ═══')
fake = FakeConn()
old_conn, old_check = vnz.conn, vnz.check_db_connection
vnz.conn = fake
vnz.check_db_connection = lambda: True
try:
    fake.select_rows = []          # لا صفقات مغلقة (بعد تصفير)
    resp = client.get('/api/strategy_pnl')
    data = resp.get_json()
    names = {r['strategy'] for r in data.get('strategies', [])}
    check('الاستجابة سليمة', resp.status_code == 200)
    check('كل الاستراتيجيات الحية الـ9 ظاهرة حتى بلا صفقات',
          len(data.get('strategies', [])) == 9, str(sorted(names)))
    check('Elliott_Impulse (الأخيرة) مرئية في الجدول', 'Elliott_Impulse' in names, str(sorted(names)))
    zero_rows = [r for r in data['strategies'] if r['n'] == 0]
    check('صفوف الصفر بحالة نشطة وبلا أرقام مضللة',
          all(r['status'] == 'active' and r['pf'] is None for r in zero_rows), str(zero_rows[:2]))
finally:
    vnz.conn, vnz.check_db_connection = old_conn, old_check

# ============ 5) حفظ/استعادة مفتاح التداول ============
print('═══ 5) حفظ مفتاح التداول عبر إعادة التشغيل ═══')
import inspect as _inspect
_reset_src = _inspect.getsource(vnz.api_stats_reset)
check('تصفير الإحصائيات لا يمس مفتاح trading_enabled (خارج قائمة الحذف)',
      'trading_enabled' not in _reset_src and 'risk_governor' in _reset_src)
fake2 = FakeConn()
vnz.conn = fake2
vnz.check_db_connection = lambda: True
old_flag = vnz.is_trading_enabled
try:
    vnz.is_trading_enabled = True
    fake2.calls.clear(); fake2.params_log.clear()
    vnz._persist_trading_flag()
    ins = [c for c in fake2.calls if c.lower().startswith('insert into bot_state')]
    check('toggle→ الحفظ يكتب INSERT في bot_state', len(ins) == 1, str(fake2.calls[:3]))
    pkeys = [p for p in fake2.params_log if p]
    check('مفتاح الحفظ = trading_enabled والقيمة JSON',
          len(pkeys) == 1 and pkeys[0][0] == 'trading_enabled' and '"enabled": true' in pkeys[0][1],
          str(pkeys))
    vnz.is_trading_enabled = False
    fake2.state_row = {'value': {'enabled': True}}
    vnz._restore_trading_flag_at_boot()
    check('الاستعادة: حالة محفوظة true → التداول مفعّل عند الإقلاع', vnz.is_trading_enabled is True)
    fake2.state_row = {'value': {'enabled': False}}
    vnz._restore_trading_flag_at_boot()
    check('الاستعادة: حالة محفوظة false → ورقي', vnz.is_trading_enabled is False)
    fake2.state_row = None
    vnz._restore_trading_flag_at_boot()
    check('الاستعادة: لا حالة محفوظة → يبقى الافتراضي الورقي', vnz.is_trading_enabled is False)
    # مسار toggle الحي يكتب الحالة
    fake2.calls.clear(); fake2.params_log.clear()
    before = vnz.is_trading_enabled
    r = client.post('/api/trading/toggle')
    check('مسار toggle يعمل', r.status_code == 200, str(r.status_code))
    check('toggle قلب الحالة فعليًا', vnz.is_trading_enabled == (not before))
    ins2 = [c for c in fake2.calls if c.lower().startswith('insert into bot_state')]
    check('toggle الحي حفظ الحالة في bot_state', len(ins2) == 1, str(fake2.calls[:4]))
finally:
    vnz.is_trading_enabled = old_flag
    vnz.conn, vnz.check_db_connection = old_conn, old_check

# ============ 6) الواجهة ============
print('═══ 6) الواجهة — شارة الوضع الورقي ═══')
html = vnz.get_dashboard_html()
check('شارة الوضع الورقي موجودة (paper-mode-banner)', 'paper-mode-banner' in html)
check('نص الشارة يوضح "وضع تجريبي ورقي"', 'وضع تجريبي ورقي' in html)
check('الـ JS يبدّل الشارة حسب is_trading_enabled',
      'paperBanner.style.display = data.is_trading_enabled' in html)

# ============ 7) انحدار ============
print('═══ 7) انحدار — ما لم يُمس ═══')
tbl = vnz._evidence_strategy_table()
check('9 استراتيجيات حية في جدول الأدلة', len(tbl) == 9, str([t[2] for t in tbl]))
check('Elliott_Impulse مسجل في حلقة الفحص',
      any(t[2] == 'Elliott_Impulse' for t in tbl))
check('دالة فحص Elliott موجودة', callable(vnz.check_elliott_impulse_strategy))
rules = {str(r) for r in vnz.app.url_map.iter_rules()}
for route in ('/api/trading/toggle', '/api/stats/reset', '/api/strategy_pnl',
              '/api/system_status', '/api/protections', '/health'):
    check(f'المسار {route} حي', route in rules)

print()
print(f'═══ النتيجة: {len(PASS)}/{len(PASS) + len(FAIL)} ═══')
if FAIL:
    print('❌ فشل:', FAIL)
    sys.exit(1)
print('✅ كل اختبارات V9.40.0 ناجحة')

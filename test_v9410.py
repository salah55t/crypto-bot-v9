# -*- coding: utf-8 -*-
"""
test_v9410.py — اختبارات V9.41.0: إصلاح "إستراتيجية اليوت لم تولد أي إشارة" + الخسائر
  1) الإصدار V9.41.0 في الكود والواجهة
  2) الإصلاح القاتل: Elliott_Impulse مُضاف إلى حلقة الفحص الرئيسية (strategies_to_check)
     — كان V9.39.0 سجّله في الترشيح والأدلة والعرض فقط → صفر فحص وصفر إشارة
  3) سلسلة البوابات لـ Elliott تعمل طرفًا لطرفًا: إشارة على موجات W4 مصطنعة + درجة
     مطابقة ≥ 70 في trend_up (وفي انتقالي كثانوي) + الرفض في الريم المعاكس
  4) أرضية SR 0.85→0.60 (فروع ضائعة في الصعود) وبقية الأرضيات لم تُمس
  5) تهدئة التوصيات بقيت 480د (مثبتة بالباك تيست V9.23.0 — فُحص خفضها ورُفض)
  6) انحدار V9.40.0: سلّم الصعود + تهدئة الرابح 60د + شارة الورقي + جدول 9 استراتيجيات
"""
import sys, os, inspect

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

import pandas as pd  # noqa: E402

PASS, FAIL = [], []


def check(name, cond, extra=''):
    (PASS if cond else FAIL).append(name)
    print(('✅ ' if cond else '❌ ') + name + (f' — {extra}' if (extra and not cond) else ''))


client = vnz.app.test_client()


# ===== باني موجات إليوت المصطنعة (نفس منهجية test_v9390) =====
def _seg(rows, start, end, bars):
    for i in range(bars):
        o = start + (end - start) * i / bars
        c = start + (end - start) * (i + 1) / bars
        rows.append((o, max(o, c) + 0.05, min(o, c) - 0.05, c, 1000.0))


def build_waves(w1=(100.0, 110.0, 20), w2=(110.0, 104.0, 15), w3=(104.0, 130.0, 25),
                w4=(130.0, 118.6, 15), tail=None):
    rows = []
    _seg(rows, *w1)
    _seg(rows, *w2)
    _seg(rows, *w3)
    _seg(rows, *w4)
    for o, c in (tail or [(118.6, 119.2), (119.2, 119.9), (119.9, 120.4)]):
        rows.append((o, max(o, c) + 0.05, min(o, c) - 0.05, c, 1000.0))
    df = pd.DataFrame(rows, columns=['open', 'high', 'low', 'close', 'volume'])
    df.name = 'TESTUSDT'
    return df


# ============ 1) الإصدار ============
print('═══ 1) الإصدار ═══')
check('الإصدار V9.41.0', vnz.APP_VERSION == 'V9.41.0', vnz.APP_VERSION)
check('HTML يحمل الإصدار V9.41.0', vnz.get_dashboard_html().find('V9.41.0') > 0)

# ============ 2) الإصلاح القاتل — Elliott في حلقة الفحص ============
print('═══ 2) Elliott_Impulse في حلقة الفحص الرئيسية ═══')
src = inspect.getsource(vnz.main_loop_enhanced)
check("حلقة الفحص تضيف ELLIOTT إلى strategies_to_check",
      "strategies_to_check.append(('ELLIOTT', check_elliott_impulse_strategy, 'Elliott_Impulse'))" in src)
append_sites = src.count('strategies_to_check.append')
check('مواضع الإضافة في الحلقة = 6 (4 أساسية + عائلة FT + Elliott)', append_sites == 6, str(append_sites))
check('الإضافة خاضعة لبوابة التعطيل _strategy_enabled',
      'if _strategy_enabled(\'ELLIOTT\'):' in src)
check('ELLIOTT غير معطلة افتراضيًا', 'ELLIOTT' not in vnz.DISABLED_STRATEGY_KEYS
      and vnz.ELLIOTT_ENABLED is True)
check('ELLIOTT ضمن المسموح في STRONG_UPTREND', 'ELLIOTT' in vnz.MARKET_STATE_STRATEGY_ALLOW['STRONG_UPTREND'])
check('ELLIOTT ضمن المسموح في UPTREND', 'ELLIOTT' in vnz.MARKET_STATE_STRATEGY_ALLOW['UPTREND'])
check('ELLIOTT في خريطة الترشيح STRATEGY_SETUP_SCANNERS', 'ELLIOTT' in vnz.STRATEGY_SETUP_SCANNERS)
check('Elliott_Impulse في جدول الأدلة (9 استراتيجيات)',
      any(n == 'Elliott_Impulse' for _, _, n in vnz._evidence_strategy_table()))
check('Elliott خارج بوابة ثبات الصاعد 480د (ليست في TREND_CONTINUATION_KEYS)',
      'ELLIOTT' not in vnz.TREND_CONTINUATION_KEYS)
check('دالة فحص Elliott قابلة للاستدعاء', callable(vnz.check_elliott_impulse_strategy))
check('مهلة زمنية لصفقات Elliott (300د في حاكم المخاطر)',
      vnz.STRATEGY_TIME_LIMIT_MIN.get('Elliott_Impulse') == 300,
      str(vnz.STRATEGY_TIME_LIMIT_MIN.get('Elliott_Impulse')))

# ============ 3) سلسلة البوابات طرفًا لطرفًا ============
print('═══ 3) سلسلة بوابات Elliott: إشارة حقيقية على موجات مصطنعة ═══')
df = build_waves()
ok_sig = vnz.check_elliott_impulse_strategy(df)
check('موجة W4 سليمة ⇒ إشارة Elliott True', ok_sig is True)

ri_prime = {'regime': 'trend_up', 'adx': 24.0, 'atr_pct': 1.2, 'roc': 2.0, 'struct_up': True}
fit_prime = vnz.score_strategy_pair_fit(ri_prime, 'Elliott_Impulse')
check('درجة المطابقة في trend_up ≥ 70 (مؤهلة لبناء دليل/إشارة)',
      fit_prime is not None and fit_prime >= 70.0, str(fit_prime))
ri_sec = {'regime': 'transitional', 'adx': 24.0, 'atr_pct': 1.2, 'roc': 2.0, 'struct_up': True}
fit_sec = vnz.score_strategy_pair_fit(ri_sec, 'Elliott_Impulse')
check('درجة المطابقة في انتقالي (ثانوي) ≥ 70 أيضًا',
      fit_sec is not None and fit_sec >= 70.0, str(fit_sec))
ri_bad = {'regime': 'range', 'adx': 24.0, 'atr_pct': 1.2, 'roc': 2.0}
check('الريم المعاكس (نطاق) ⇒ نقض قاسٍ 0.0',
      vnz.score_strategy_pair_fit(ri_bad, 'Elliott_Impulse') == 0.0)
# بوابة الجودة EQG: مواصفة Elliott موجودة بقيمها
eqg = vnz.ENTRY_QUALITY_PROFILES.get('Elliott_Impulse') or {}
check('مواصفة EQG لـ Elliott موجودة (min_excursion 0.60 / min_eqs 55)',
      eqg.get('min_excursion_pct') == 0.60 and eqg.get('min_eqs') == 55, str(eqg))
# فلتر الاستراتيجية الخاص: ADX أدنى 20
fl = vnz.STRATEGY_FILTER_PROFILES.get('Elliott_Impulse') or {}
check('فلتر Elliott: ADX أدنى 20 وتقلب 0.35-5.0%',
      fl.get('min_adx') == 20.0 and fl.get('min_atr_pct') == 0.35 and fl.get('max_atr_pct') == 5.0, str(fl))
# خريطة الريم: أساسي trend_up وثانوي انتقالي
rm = vnz.STRATEGY_PAIR_PROFILES.get('Elliott_Impulse') or {}
check('خريطة الريم: أساسي trend_up + ثانوي انتقالي',
      rm.get('regimes') == ('trend_up',) and rm.get('secondary_regimes') == ('transitional',), str(rm))

# ============ 4) أرضية SR المخفضة ============
print('═══ 4) أرضية SR 0.85→0.60 ═══')
sr = vnz.ENTRY_QUALITY_PROFILES.get('SR_Breakout_Enhanced') or {}
check('أرضية SR أصبحت 0.60%', sr.get('min_excursion_pct') == 0.60, str(sr.get('min_excursion_pct')))
check('أرضية EMA_RSI لم تتغير (0.60)',
      (vnz.ENTRY_QUALITY_PROFILES.get('EMA_RSI_Cross') or {}).get('min_excursion_pct') == 0.60)
check('أرضية Elliott لم تتغير (0.60)',
      (vnz.ENTRY_QUALITY_PROFILES.get('Elliott_Impulse') or {}).get('min_excursion_pct') == 0.60)
check('أرضية Bullish_Momentum لم تتغير (0.70)',
      (vnz.ENTRY_QUALITY_PROFILES.get('Bullish_Momentum') or {}).get('min_excursion_pct') == 0.70)
check('حد جودة SR (min_eqs 60) لم يتغير', sr.get('min_eqs') == 60)

# ============ 5) تهدئة التوصيات — قرار إبقاء 480د ============
print('═══ 5) تهدئة التوصيات (بقيت 480د) ═══')
check('RECOMMENDATION_COOLDOWN_MIN بقيت 480د (مثبتة بالباك تيست V9.23.0)',
      vnz.RECOMMENDATION_COOLDOWN_MIN == 480, str(vnz.RECOMMENDATION_COOLDOWN_MIN))

# ============ 6) انحدار V9.40.0 — ما لم يُمس ============
print('═══ 6) انحدار V9.40.0 ═══')
rally = vnz.PROFIT_LOCK_LADDER_RALLY
check('سلّم الصعود القوي سليم (5 درجات، الأولى 0.75→0.20)',
      len(rally) == 5 and rally[0] == (0.75, 0.20), str(rally))
check('تهدئة الإغلاق الرابح 60د قائمة', vnz.PROTECTION_COOLDOWN_WIN_MIN == 60)
check('تهدئة الخاسر 240د قائمة', vnz.PROTECTION_COOLDOWN_MIN == 240)
html = vnz.get_dashboard_html()
check('شارة الوضع الورقي قائمة (paper-mode-banner)', 'paper-mode-banner' in html)
check('سقف الوقف 4.0% لم يتغير', vnz.MAX_SL_DISTANCE_PCT == 4.0)
# /api/strategy_pnl يعرض كل الاستراتيجيات الـ9 حتى بلا صفقات (FakeConn بلا صفوف)
old_conn, old_check = vnz.conn, vnz.check_db_connection


class _FakeCur:
    def __init__(self, p): self.p = p; self.rowcount = 0
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, sql, params=None): self.p.calls.append(' '.join(sql.split())[:120])
    def fetchone(self): return None
    def fetchall(self): return []


class _FakeConn:
    def __init__(self): self.calls = []
    def cursor(self): return _FakeCur(self)
    def commit(self): pass
    def rollback(self): pass


try:
    fake = _FakeConn()
    vnz.conn, vnz.check_db_connection = fake, lambda: True
    r = client.get('/api/strategy_pnl')
    data = r.get_json()
    names = [s['strategy'] for s in data.get('strategies', [])]
    check('/api/strategy_pnl يعرض 9 استراتيجيات (Elliott مرئي حتى بصفقات n=0)',
          r.status_code == 200 and len(names) == 9 and 'Elliott_Impulse' in names,
          f'{r.status_code} {names}')
finally:
    vnz.conn, vnz.check_db_connection = old_conn, old_check

rules = {str(r) for r in vnz.app.url_map.iter_rules()}
for route in ('/health', '/api/strategy_pnl', '/api/rejection_summary',
              '/api/strategy_candidates', '/api/trading/toggle'):
    check(f'المسار {route} حي', route in rules)

print()
print(f'═══ النتيجة: {len(PASS)}/{len(PASS) + len(FAIL)} ═══')
if FAIL:
    print('❌ فشل:', FAIL)
    sys.exit(1)
print('✅ كل اختبارات V9.41.0 ناجحة')

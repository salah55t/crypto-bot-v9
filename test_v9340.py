# -*- coding: utf-8 -*-
"""
test_v9340.py — اختبارات V9.34.0: تنظيف OctoBot + تقاعد الاستراتيجيات + لوحة الأداء
  1) الإزالة البنيوية لـ BB_STOCH (المفاتيح المتقاعدة خارج كل بوابات الإنتاج)
  2) فلتر ارتداد القاع المحلي (اقتباس OctoBot DipAnalyser) لعائلة FT العكسية
  3) حاكم تقاعد الاستراتيجيات: إحصاء DB + شروط التعليق + الاستثناء + الاستمرارية
  4) نقاط النهاية /api/strategy_pnl و /api/pnl_history + لوحة الأداء (SVG)
"""
import sys, os, json
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('LOG_LEVEL', 'ERROR')
os.environ.setdefault('BINANCE_API_KEY', 'test_key_dummy')
os.environ.setdefault('BINANCE_API_SECRET', 'test_secret_dummy')

import vnz  # noqa: E402
vnz.logger.disabled = True
vnz.log_and_notify = lambda *a, **k: None
vnz.send_telegram_message = lambda *a, **k: None

PASS, FAIL = [], []


def check(name, cond, extra=''):
    (PASS if cond else FAIL).append(name)
    print(('✅ ' if cond else '❌ ') + name + (f' — {extra}' if (extra and not cond) else ''))


# ============ 1) الإصدار ============
print('═══ 1) الإصدار ═══')
check('الإصدار V9.34.0', vnz.APP_VERSION == 'V9.34.0', vnz.APP_VERSION)
check('مصدر الإصدار الوحيد', vnz.get_dashboard_html().find('V9.34.0') > 0)


# ============ 2) الإزالة البنيوية (المفاتيح المتقاعدة) ============
print('═══ 2) إزالة BB_STOCH — المفاتيح المتقاعدة ═══')
check('BB_STOCH في القائمة الافتراضية', 'BB_STOCH' in vnz.DISABLED_STRATEGY_KEYS)
check('المُحلل يقبل قائمة JSON', vnz._parse_disabled_strategies('["A_KEY","b_key"]') == ['A_KEY', 'B_KEY'])
check('المُحلل يقبل قاموس JSON (القيم الصائبة فقط)', vnz._parse_disabled_strategies('{"X_KEY": true, "Y_KEY": false}') == ['X_KEY'])
check('المُحلل يهمل التالف', vnz._parse_disabled_strategies('not-json') == [])

table = vnz._evidence_strategy_table()
table_keys = [k for k, _, _ in table]
check('المتقاعدة خارج جدول الأدلة', 'BB_STOCH' not in table_keys)
check('جدول الأدلة 12 استراتيجية', len(table_keys) == 12, str(len(table_keys)))
check('عائلة FT العكسية خمس استراتيجيات في الجدول',
      all(k in table_keys for k in vnz.FT_REVERSAL_KEYS))
check('الأصليات الباقية في الجدول',
      all(k in table_keys for k in ('MACD_EMA', 'EMA_RSI', 'PULLBACK', 'BB_SQUEEZE', 'BULLISH_MOMENTUM', 'SR_BREAKOUT')))
src = open('vnz.py', encoding='utf-8').read()
check('حلقة المسح تستبعد المتقاعدة', "_strategy_enabled('BB_STOCH')" in src and 'key not in DISABLED_STRATEGY_KEYS' in src)
check('حاكم التقاعد في بوابات حلقة المسح', 'strategy_retirement_blocked(name)' in src)
rules = {str(r) for r in vnz.app.url_map.iter_rules()}
check('/api/strategy_pnl موجود', '/api/strategy_pnl' in rules)
check('/api/pnl_history موجود', '/api/pnl_history' in rules)


# ============ 3) فلتر ارتداد القاع المحلي (اقتباس DipAnalyser) ============
print('═══ 3) تأكيد ارتداد القاع — عائلة FT العكسية ═══')


def _feats_for_bounce(seed=7):
    """إطار ميزات حقيقي عبر calculate_all_features ثم هندسة أدنى 7 شموع."""
    rng = np.random.default_rng(seed)
    n = 80
    closes = 100.0 + np.cumsum(rng.normal(0.02, 0.8, n))
    opens = np.roll(closes, 1); opens[0] = 100.0
    highs = np.maximum(opens, closes) + 0.15
    lows = np.minimum(opens, closes) - 0.15
    vols = np.full(n, 1000.0)
    df = pd.DataFrame({'open': opens, 'high': highs, 'low': lows, 'close': closes, 'volume': vols},
                      index=pd.date_range('2026-10-01', periods=n, freq='15min', tz='UTC'))
    feats = vnz.calculate_all_features(df, None)
    return feats


def _shape_lows(feats, lows7, close_last):
    """تضع أدنى 7 شموع (أقدم→أحدث) وإغلاق الشمعة الأخيرة يدويًا."""
    for j, lv in enumerate(lows7):
        feats.at[feats.index[-7 + j], 'low'] = float(lv)
    feats.at[feats.index[-1], 'close'] = float(close_last)
    feats.at[feats.index[-1], 'high'] = max(float(feats['high'].iloc[-1]), float(close_last) + 0.1)
    return feats


_orig_filter_flag = vnz.FT_REVERSAL_BOUNCE_FILTER
try:
    # أ) سكين ساقط: القاع عند آخر شمعة والإغلاق ملاصق له → رفض
    vnz._strategy_filter_stats.clear()
    feats = _shape_lows(_feats_for_bounce(7), [110, 109, 108, 107, 106, 105, 104], 104.0)
    ok = vnz.passes_strategy_prefilters(feats, 'FT_BbandRsi')
    broke = dict(vnz._strategy_filter_stats.get('FT_BbandRsi', {}))
    check('السكين النشط يُرفض', ok is False)
    check('الرفض بعلامة "القاع لم يتأكد بعد" (اقتباس DipAnalyser)',
          any('القاع لم يتأكد' in k for k in broke.keys()), str(broke.keys()))

    # ب) ارتداد مؤكد: القاع قبل شمعة واحدة والإغلاق ارتفع عنه → قبول
    feats2 = _shape_lows(_feats_for_bounce(7), [108, 109, 110, 109.5, 106, 105, 106.5], 106.9)
    ok2 = vnz.passes_strategy_prefilters(feats2, 'FT_BbandRsi')
    check('الارتداد المؤكد يقبل (قاع قبل شمعة + إغلاق فوقه)', ok2 is True)

    # ج) قاع قديم (قبل 3 شموع) → رفض حتى لو الإغلاق فوقه
    vnz._strategy_filter_stats.clear()
    feats3 = _shape_lows(_feats_for_bounce(7), [110, 105, 108, 108.5, 109, 109.2, 109.4], 109.6)
    ok3 = vnz.passes_strategy_prefilters(feats3, 'FT_CombinedBinHAndCluc')
    broke3 = dict(vnz._strategy_filter_stats.get('FT_CombinedBinHAndCluc', {}))
    check('القاع الأقدم من شمعتين يُرفض', ok3 is False and any('القاع لم يتأكد' in k for k in broke3.keys()),
          f'ok={ok3} labels={list(broke3.keys())}')

    # د) استراتيجية غير عكسية (BB_SQUEEZE — بلا قيود ADX/ATR أدنى) بنفس هندسة السكين → قبول
    feats4 = _shape_lows(_feats_for_bounce(7), [110, 109, 108, 107, 106, 105, 104], 104.0)
    ok4 = vnz.passes_strategy_prefilters(feats4, 'BB_Squeeze_Breakout')
    check('الفلتر خاص بالعكسية فقط (BB_SQUEEZE يمر بالسكين)', ok4 is True)

    # هـ) تعطيل العلم يعيد الوضع السابق
    vnz.FT_REVERSAL_BOUNCE_FILTER = False
    feats5 = _shape_lows(_feats_for_bounce(7), [110, 109, 108, 107, 106, 105, 104], 104.0)
    ok5 = vnz.passes_strategy_prefilters(feats5, 'FT_BbandRsi')
    check('FT_REVERSAL_BOUNCE_FILTER=false يعطّل الفلتر', ok5 is True)
finally:
    vnz.FT_REVERSAL_BOUNCE_FILTER = _orig_filter_flag
check('الافتراضي: الفلتر مفعّل', vnz.FT_REVERSAL_BOUNCE_FILTER is True)


# ============ 4) حاكم تقاعد الاستراتيجيات ============
print('═══ 4) حاكم تقاعد الاستراتيجيات ═══')


class _RetireCur:
    def __init__(self, store, rows):
        self._store, self._rows, self._row, self._result = store, rows, None, []

    def execute(self, q, *a):
        # نفس توقيع v9310: a = (params_tuple،) → a[0][1] هو الـ payload
        if 'FROM signals' in q:
            self._result = list(self._rows)
        elif 'SELECT value FROM bot_state' in q:
            self._row = {'value': self._store.get('saved')} if self._store.get('saved') else None
        elif 'INSERT INTO bot_state' in q:
            self._store['saved'] = json.loads(a[0][1])

    def fetchall(self): return self._result
    def fetchone(self): return self._row
    def __enter__(self): return self
    def __exit__(self, *a): return False


class _RetireConn:
    def __init__(self, store, rows): self._store, self._rows = store, rows
    def cursor(self): return _RetireCur(self._store, self._rows)
    def commit(self): pass


def _row(name, raw_pct):
    return {'strategy_name': name, 'profit_percentage': raw_pct, 'original_quantity': None,
            'entry_price': None, 'is_real_trade': False, 'closed_at': '2026-10-09T20:00:00+00:00'}


def _reset_retire():
    vnz._strategy_retire_state.clear()
    store['saved'] = None
    vnz._strategy_retire_loaded = False


store = {}
_orig_conn2, _orig_cdc2 = vnz.conn, vnz.check_db_connection
_orig_force = list(vnz.STRAT_RETIRE_FORCE_ACTIVE)
try:
    vnz.check_db_connection = lambda: True

    # أ) رياضيات الإحصاء (نفس محاسبة /api/stats: صافٍ = خام − 0.2%)
    weak_rows = ([_row('Weak_Strat', 1.2)] * 3) + ([_row('Weak_Strat', -3.0)] * 9)
    vnz.conn = _RetireConn(store, weak_rows)
    s = vnz._strategy_retire_db_stats()
    w = s.get('Weak_Strat', {})
    check('الإحصاء: عدد الصفقات', w.get('n') == 12, str(w))
    check('الإحصاء: PF صحيح (3.0/28.8)', abs(w.get('pf', 0) - 0.104) < 0.001, str(w.get('pf')))
    check('الإحصاء: التوقع −2.15%', abs(w.get('exp_pct', 0) - (-2.15)) < 0.001, str(w.get('exp_pct')))
    check('الإحصاء: الصافي بالدولار (−25.8% × 4$)', abs(w.get('net_usdt', 0) - (-1.032)) < 0.001, str(w.get('net_usdt')))

    # ب) شرط التعليق: n≥12 و PF<0.55 → تعليق + إشعار
    _reset_retire()
    vnz._strategy_retire_loaded = True
    vnz.strategy_retirement_refresh('close')
    st = vnz._strategy_retire_state.get('Weak_Strat', {})
    check('الضعيفة (n=12, PF=0.10) عُلّقت', st.get('suspended') is True, str(st))
    check('سبب التعليق موثق بالأرقام', 'PF=' in str(st.get('reason', '')) and 'EXP=' in str(st.get('reason', '')))
    check('الحفظ في bot_state كُتب', isinstance(store.get('saved'), dict) and
          store['saved'].get('strategies', {}).get('Weak_Strat', {}).get('suspended') is True)

    # ج) النشطة السليمة لا تُعلَّق
    good = ([_row('Fine_Strat', 1.2)] * 7) + ([_row('Fine_Strat', -1.0)] * 5)
    vnz.conn = _RetireConn(store, good)
    _reset_retire(); vnz._strategy_retire_loaded = True
    vnz.strategy_retirement_refresh('close')
    check('السليمة (PF=1.17, EXP=+0.08) بلا تعليق',
          vnz._strategy_retire_state.get('Fine_Strat', {}).get('suspended') is not True)

    # د) عينة قليلة (n<12) لا تُعلَّق وإن كانت التوقع سيئًا
    few = [_row('Few_Strat', -3.0)] * 5
    vnz.conn = _RetireConn(store, few)
    _reset_retire(); vnz._strategy_retire_loaded = True
    vnz.strategy_retirement_refresh('close')
    check('n=5 دون حد العينات → لا تعليق', 'Few_Strat' not in vnz._strategy_retire_state)

    # هـ) الاستثناء اليدوي (force active)
    bad = ([_row('Forced_Strat', 1.2)] * 3) + ([_row('Forced_Strat', -3.0)] * 9)
    vnz.conn = _RetireConn(store, bad)
    vnz.STRAT_RETIRE_FORCE_ACTIVE.append('Forced_Strat')
    _reset_retire(); vnz._strategy_retire_loaded = True
    vnz.strategy_retirement_refresh('close')
    check('المستثنى قسرًا لا يُعلَّق', 'Forced_Strat' not in vnz._strategy_retire_state)
    vnz.STRAT_RETIRE_FORCE_ACTIVE.remove('Forced_Strat')

    # و) الرفع عند تحسن الإحصاء فوق العتبات
    improving = ([_row('Improving_Strat', 2.2)] * 10) + ([_row('Improving_Strat', -1.0)] * 2)
    vnz.conn = _RetireConn(store, improving)
    _reset_retire()
    vnz._strategy_retire_state['Improving_Strat'] = {'suspended': True, 'n': 12, 'pf': 0.4, 'exp_pct': -1.0,
                                                     'reason': 'قديم', 'since': '2026-10-09', 'source': 'close'}
    vnz._strategy_retire_loaded = True
    vnz.strategy_retirement_refresh('close')
    check('تحسن الإحصاء يرفع التعليق', vnz._strategy_retire_state.get('Improving_Strat', {}).get('suspended') is False)

    # ز) الحاكم معطّل → لا شيء
    vnz.conn = _RetireConn(store, weak_rows)
    _reset_retire(); vnz._strategy_retire_loaded = True
    old_enabled = vnz.STRAT_RETIRE_ENABLED
    vnz.STRAT_RETIRE_ENABLED = False
    vnz.strategy_retirement_refresh('close')
    check('الحاكم معطّل → لا تعليقات', not vnz._strategy_retire_state)
    vnz.STRAT_RETIRE_ENABLED = old_enabled

    # ح) blocked(): فحص ذاكرة مع السبب + الاستعادة من DB
    vnz._strategy_retire_state['Weak_Strat'] = {'suspended': True, 'n': 12, 'pf': 0.10, 'exp_pct': -2.15,
                                                'reason': 'PF=0.10/EXP=-2.15% على 12 صفقة',
                                                'since': '2026-10-09', 'source': 'close'}
    vnz._strategy_retire_loaded = True
    blk, why = vnz.strategy_retirement_blocked('Weak_Strat')
    check('blocked يعلم بالسبب', blk is True and 'تقاعد' in why, why)
    blk2, _ = vnz.strategy_retirement_blocked('Fine_Strat')
    check('غير معلّقة تمر', blk2 is False)
    # استعادة من bot_state بعد "إعادة تشغيل" — نزرع الحالة المحفوظة مباشرة
    # (كل refresh يعيد بناء saved من صفقات DB الحالية، فالمحاكاة تزرع ما قبل إعادة التشغيل)
    store['saved'] = {'strategies': {'Weak_Strat': {'suspended': True, 'n': 12, 'pf': 0.10, 'exp_pct': -2.15,
                                                    'reason': 'PF=0.10/EXP=-2.15% على 12 صفقة',
                                                    'since': '2026-10-09', 'source': 'close'}}}
    vnz._strategy_retire_state.clear()
    vnz._strategy_retire_loaded = False
    vnz._strategy_retire_ensure_loaded()
    check('الاستعادة من bot_state تحفظ التعليق',
          vnz._strategy_retire_state.get('Weak_Strat', {}).get('suspended') is True)
finally:
    vnz.conn, vnz.check_db_connection = _orig_conn2, _orig_cdc2
    vnz.STRAT_RETIRE_FORCE_ACTIVE = _orig_force
    vnz._strategy_retire_state.clear()
    vnz._strategy_retire_loaded = False


# ============ 5) ربط register_realized_pnl + الحمايات ============
print('═══ 5) الربط بالحمايات والإشعارات ═══')
check('register_realized_pnl يستدعي إحصاء التقاعد عند الإغلاق',
      "strategy_retirement_refresh('close')" in src)
snap = vnz.get_active_protections_snapshot()
check('protections.config يحوي strategy_retirement', 'strategy_retirement' in snap.get('config', {}))
check('protections.config يحوي disabled_strategies',
      snap.get('config', {}).get('disabled_strategies', {}).get('keys') == list(vnz.DISABLED_STRATEGY_KEYS))
check('اللقطة تحمل العتبات', snap['config']['strategy_retirement']['min_n'] == vnz.STRAT_RETIRE_MIN_N)


# ============ 6) لوحة الأداء (اقتباسات OctoBot) ============
print('═══ 6) لوحة الأداء ═══')
html = vnz.get_dashboard_html()
check('تبويب أداء الاستراتيجيات موجود', 'pnlperf-tab' in html and "showTab('pnlperf'" in html)
check('دالة منحنى الأرباح SVG', 'function renderPnlChart' in html and 'function updatePnlChart' in html)
check('دالة جدول أداء الاستراتيجيات', 'function updateStrategyPnl' in html and 'strategy-pnl-table' in html)
check('شرارة بطاقة صافي الربح', 'net-spark' in html and '/api/pnl_history' in html)
check('شارة المتقاعدة في الإعدادات', 'متقاعدة' in html)
check('عنصر مفتاح BB+Stoch حُذف من الإعدادات', 'bb-stoch-strategy-toggle' not in html)
check('إشعارات ملوّنة بالمستوى', 'border-r-2' in html and 'STRATEGY_RETIREMENT' in html)
check('الاستطلاع الدوري للوحة الأداء', 'whenVisible(updatePnlChart' in html and 'whenVisible(updateStrategyPnl' in html)


# ============ الخلاصة ============
print('\n' + '=' * 50)
print(f'النتيجة: {len(PASS)} نجاح / {len(FAIL)} فشل')
if FAIL:
    print('الفاشلة:', FAIL)
    sys.exit(1)
print('✅ كل اختبارات V9.34.0 نجحت')

# -*- coding: utf-8 -*-
"""
test_v9420.py — اختبارات V9.42.0: المراجعة الاحترافية الشاملة (أكبر إصلاح وتنقية)
  1) الإصدار V9.42.0 في الكود والواجهة
  2) فيبوناتشي كامل لليوت: calculate_elliott_tp_sl (هدف 0.618×W3 من قاع W4 + وقف
     بنيوي P4−0.25×ATR + أرضية MIN_PROFIT + سقف الوقف) وسلكه في حلقة الفتح
  3) زيجزاج تكييفي: عتبة ATR%×1.6 المحدودة [0.35,1.2] — موجات صغيرة (تقلب 15م
     الحقيقي) تُكتشف بالتكييف بينما العتبة الثابتة 1.0% تفشل
  4) منطقة فيبوناتشي الذهبية للتراجع [0.15,0.618] من دفعة ≥2×ATR — ضد المطاردة
     وضد البنية المكسورة، مع تمرير هادئ عند تعذر القياس
  5) حجم المركز على المسافة الحقيقية: _risk_quantity + risk_stop_distance في
     محركات TP/SL الثلاثة (ديناميكي/FT/إليوت) + تمريره في حلقة الفتح
  6) حق النقض الانتقالي: الريم الانتقالي = 0 لعائلة الاستمرارية، قائم لعائلة
     الارتداد/الإليوت/الاختراق
  7) سقف مطاردة EMA_RSI 2.5→2.0×ATR + ثوابت النطاق الذهبي
  8) انحدار الإصدارات السابقة عبر تشغيل مجموعاتها
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


# ===== باني شموع مساعد =====
def _seg(rows, start, end, bars, wick=0.05):
    for i in range(bars):
        o = start + (end - start) * i / bars
        c = start + (end - start) * (i + 1) / bars
        rows.append((o, max(o, c) + wick, min(o, c) - wick, c, 1000.0))


def build_waves(w1=(100.0, 110.0, 20), w2=(110.0, 104.0, 15), w3=(104.0, 130.0, 25),
                w4=(130.0, 118.6, 15), tail=None, wick=0.05):
    rows = []
    _seg(rows, *w1, wick=wick)
    _seg(rows, *w2, wick=wick)
    _seg(rows, *w3, wick=wick)
    _seg(rows, *w4, wick=wick)
    for o, c in (tail or [(118.6, 119.2), (119.2, 119.9), (119.9, 120.4)]):
        rows.append((o, max(o, c) + wick, min(o, c) - wick, c, 1000.0))
    df = pd.DataFrame(rows, columns=['open', 'high', 'low', 'close', 'volume'])
    df.name = 'TESTUSDT'
    return df


# =====================================================================
# 1) الإصدار
# =====================================================================
print('\n=== 1) الإصدار V9.42.0 ===')
check('APP_VERSION == V9.42.0', vnz.APP_VERSION == 'V9.42.0', vnz.APP_VERSION)
h = client.get('/health')
check('/health يعرض V9.42.0', h.status_code == 200 and h.get_json().get('version') == 'V9.42.0',
      f'{h.status_code} {h.get_json() if h.status_code == 200 else ""}')

# =====================================================================
# 2) فيبوناتشي كامل لليوت — محرك TP/SL البنيوي
# =====================================================================
print('\n=== 2) calculate_elliott_tp_sl — فيبوناتشي الموجة 4 ===')
ev_big = {'p4': 118.6, 'w3': 26.0, 'w1': 10.0, 'atr': 0.5}
r = vnz.calculate_elliott_tp_sl(120.0, ev_big)
check('يعمل على بنية صالحة', r is not None)
if r:
    exp_tp = 118.6 + 0.618 * 26.0  # 134.668
    check('الهدف = امتداد 0.618×W3 من قاع W4', abs(r['target_price'] - exp_tp) < 1e-6, f"{r['target_price']} != {exp_tp}")
    check('الوقف البنيوي = P4 − 0.25×ATR', abs(r['stop_loss'] - (118.6 - 0.125)) < 1e-6, str(r['stop_loss']))
    check('المصدر ELLIOTT_FIB_W4', r['source'] == 'ELLIOTT_FIB_W4', str(r.get('source')))
    check('المسافة الحقيقية قبل السقف محمولة', abs(r['risk_stop_distance'] - (120.0 - 118.475)) < 1e-6, str(r.get('risk_stop_distance')))
    check('هدف فيبو 1.0×W3 موثق', abs(r['fib_tp2'] - 144.6) < 1e-6, str(r.get('fib_tp2')))
    check('امتداد 1.618×W1 موثق', abs(r['fib_ext_1618'] - (118.6 + 16.18)) < 1e-6, str(r.get('fib_ext_1618')))
    check('R:R محسوب موجب', r['rr_ratio'] > 1.0, str(r.get('rr_ratio')))
    check('أساس الوقف بنيوي', r.get('stop_basis') == 'W4_structural', str(r.get('stop_basis')))
# أرضية MIN_PROFIT: بنية ضئيلة
ev_thin = {'p4': 133.0, 'w3': 1.0, 'w1': 0.8, 'atr': 0.2}
r2 = vnz.calculate_elliott_tp_sl(134.0, ev_thin)
if r2:
    floor = 134.0 * (1.0 + vnz.MIN_PROFIT_PERCENT / 100.0)
    check('أرضية MIN_PROFIT تحمي البنيات الضئيلة', r2['target_price'] >= floor - 1e-6,
          f"{r2['target_price']} < {floor}")
# سقف الوقف 4%: ATR ضخم
ev_wide = {'p4': 100.0, 'w3': 50.0, 'w1': 30.0, 'atr': 200.0}
r3 = vnz.calculate_elliott_tp_sl(120.0, ev_wide)
if r3:
    cap = 120.0 * vnz.MAX_SL_DISTANCE_PCT / 100.0
    check('سقف الوقف 4% يطبق على الوقف البنيوي', abs((120.0 - r3['stop_loss']) - cap) < 1e-6,
          f"dist={120.0 - r3['stop_loss']} cap={cap}")
    check('المسافة الحقيقية تحفظ البعد قبل السقف', r3['risk_stop_distance'] > (120.0 - r3['stop_loss']),
          str(r3.get('risk_stop_distance')))
# مدخلات فاسدة
check('بنية ناقصة → None', vnz.calculate_elliott_tp_sl(120.0, {'p4': 0, 'w3': 0}) is None)

# =====================================================================
# 3) الزيجزاج التكييفي — موجات صغيرة بتقلب 15م الحقيقي
# =====================================================================
print('\n=== 3) زيجزاج تكييفي ATR%×1.6 ===')
rows = []
_seg(rows, 100.0, 100.02, 12, wick=0.02)          # تمهيد رائز
_seg(rows, 100.0, 100.8, 14, wick=0.02)           # W1 = 0.8%
_seg(rows, 100.8, 100.4, 10, wick=0.02)           # W2 تراجع 0.4%
_seg(rows, 100.4, 101.45, 14, wick=0.02)          # W3 = 1.05%
_seg(rows, 101.45, 100.95, 10, wick=0.02)         # W4 تراجع 0.476%
# ذيل صاعد ≥ عتبة التأكيد (0.35%) ويبقى الإغلاق داخل (P4، P3)
for o, c in [(100.95, 101.10), (101.10, 101.24), (101.24, 101.36)]:
    rows.append((o, max(o, c) + 0.02, min(o, c) - 0.02, c, 1000.0))
df_small = pd.DataFrame(rows, columns=['open', 'high', 'low', 'close', 'volume'])
df_small['atr'] = 0.15   # 0.15% من السعر → dev = max(0.35, 0.24) = 0.35%
df_small.name = 'SMALLUSDT'

ok_ad, ev_ad = vnz._elliott_w4_setup_info(df_small)
check('التكييف يكتشف موجات 15م الصغيرة', ok_ad, ev_ad.get('reason', ''))
if ok_ad:
    check('العتبة التكيفية = 0.35 (أرضية)', ev_ad.get('zz_dev_pct') == 0.35, str(ev_ad.get('zz_dev_pct')))
    check('قواعد إليوت موثقة (W3/W1)', ev_ad.get('w3_w1', 0) >= 1.0, str(ev_ad.get('w3_w1')))
    check('منطقة W4 داخل [0.236,0.5]', 0.236 <= ev_ad.get('w4_retrace_pct', 0) / 100.0 <= 0.5,
          str(ev_ad.get('w4_retrace_pct')))
    check('أهداف فيبو في الأدلة', ev_ad.get('fib_tp1') is not None and ev_ad.get('fib_tp2') is not None)
    check('الوقف البنيوي في الأدلة تحت P4', (ev_ad.get('struct_stop') or 0) < ev_ad.get('p4', 0))
check('العتبة الثابتة 1.0% تفشل على نفس الموجات (بروح الأساس)',
      not vnz._elliott_w4_setup_info(df_small.drop(columns=['atr']))[0],
      'المفروض: محاور غير كافية تحت عتبة 1.0%')
# الموجات الكبيرة (اختبارات v9390) ما زالت تعمل بلا atr (احتياط ثابت)
df_big = build_waves()
ok_big, ev_big2 = vnz._elliott_w4_setup_info(df_big)
check('الموجات الكبيرة تُكتشف بلا عمود ATR (سلوك قديم محفوظ)', ok_big, ev_big2.get('reason', ''))
check('بدون ATR: العتبة تساوي الثابتة', ev_big2.get('zz_dev_pct') == float(vnz.ELLIOTT_ZZ_DEVIATION_PCT)
      if ok_big else True, str(ev_big2.get('zz_dev_pct')))
def _with_time(df):
    """فهرس زمني 15د (يتطلبه حساب الميزات الزمنية)"""
    df.index = pd.date_range('2026-01-01', periods=len(df), freq='15min', tz='UTC')
    return df


df_big_f = vnz.calculate_all_features(_with_time(build_waves()), None)
ok_big_f, _ = vnz._elliott_w4_setup_info(df_big_f)
check('الموجات الكبيرة مع مؤشرات كاملة تُكتشف تكييفيًا', ok_big_f)

# =====================================================================
# 4) منطقة فيبوناتشي الذهبية للتراجع
# =====================================================================
print('\n=== 4) النطاق الذهبي للتراجع Pullback ===')


def _pb_rows(flat_n, rally_end, mid_closes, tail_closes, wick=0.01, atr=0.2):
    """باني هندسة التراجع: تمهيد مسطح → دفعة إلى rally_end → تصحيح mid_closes → شموع الذيل tail_closes.
    كل شمعة (o، h، l، c) صريحة — حتمية كاملة."""
    rows = []
    last = 100.0
    for _ in range(flat_n):
        rows.append((100.0, 100.01, 99.99, 100.0, 1000.0))
    # دفعة خطية إلى rally_end
    steps = 12
    for i in range(steps):
        o = 100.0 + (rally_end - 100.0) * i / steps
        c = 100.0 + (rally_end - 100.0) * (i + 1) / steps
        rows.append((o, max(o, c) + wick, min(o, c) - wick, c, 1000.0))
    last = rally_end
    for c in mid_closes:
        rows.append((last, max(last, c) + wick, min(last, c) - wick, c, 1000.0))
        last = c
    for o, c in tail_closes:
        rows.append((o, max(o, c) + wick, min(o, c) - wick, c, 1000.0))
        last = c
    df = pd.DataFrame(rows, columns=['open', 'high', 'low', 'close', 'volume'])
    df['atr'] = atr
    df.name = 'PBUSDT'
    return df


# القاعدة: دفعة إلى 105.0 ثم تصحيح إلى 104.54 ثم ارتداد بإغلاق 104.75
# swing_high=105.01، swing_low=104.54، الدفعة=0.47 (≥ 2×0.2)، التراجع ≈ 55%
_r_in = _pb_rows(32, 105.0, [104.8, 104.65, 104.55], [(104.55, 104.75)])
ok_in, ev_in = vnz._pullback_fib_zone_info(_r_in)
check('تراجع ~55% داخل النطاق الذهبي → قبول', ok_in, str(ev_in))
if ok_in:
    check('أرقام التراجع موثقة', 40.0 <= ev_in.get('retrace_pct', 0) <= 62.0, str(ev_in.get('retrace_pct')))
    check('قمة وقاع الدفعة موثقان', ev_in.get('swing_high') is not None and ev_in.get('swing_low') is not None)
# المطاردة: هبوط عميق ثم استرداد شبه كامل عند 104.98 (تراجع ~4%)
_r_chase = _pb_rows(32, 105.0, [104.55, 104.35], [(104.35, 104.70), (104.70, 104.98)])
ok_chase, ev_chase = vnz._pullback_fib_zone_info(_r_chase)
check('شراء عند القمة بعد استرداد كامل → رفض (مطاردة)', not ok_chase,
      str(ev_chase.get('retrace_pct') or ev_chase.get('skipped')))
# البنية المكسورة: استمرار الهبوط إلى 104.0 (تراجع ~99%)
_r_broke = _pb_rows(32, 105.0, [104.8, 104.4, 104.2], [(104.2, 104.0)])
ok_broke, ev_broke = vnz._pullback_fib_zone_info(_r_broke)
check('تراجع ~99% (بنية مكسورة) → رفض', not ok_broke, str(ev_broke.get('retrace_pct') or ev_broke))
rows_flat = []
for i in range(50):
    c = 100.0 + 0.03 * (i % 2)
    rows_flat.append((100.0, 100.04 if i % 2 == 0 else 100.01, 99.99 if i % 2 == 0 else 99.96, c, 1000.0))
df_flat = pd.DataFrame(rows_flat, columns=['open', 'high', 'low', 'close', 'volume'])
df_flat['atr'] = 0.5
df_flat.name = 'FLATUSDT'
ok_small, ev_small = vnz._pullback_fib_zone_info(df_flat)
check('دفعة بلا معنى (< 2×ATR) → تمرير هادئ', ok_small and ev_small.get('skipped') == 'no_meaningful_impulse',
      str(ev_small))
df_noatr = _r_in.drop(columns=['atr'])
ok_noatr, _ = vnz._pullback_fib_zone_info(df_noatr)
check('بلا ATR → تمرير هادئ (لا ختنق)', ok_noatr)


def _pb_integration(base_rows):
    df = base_rows.copy()
    df.name = 'PBUSDT'  # copy() لا يحمل السمة المخصصة
    n = len(df)
    df['ema_21'] = [float(df['close'].iloc[-1]) - 0.5] * n
    df['ema_50'] = [float(df['close'].iloc[-1]) - 1.5] * n
    macd, sig = [0.2] * n, [0.3] * n
    macd[-2], sig[-2] = 0.30, 0.35   # تحت الإشارة
    macd[-1], sig[-1] = 0.50, 0.40   # تقاطع صاعد
    df['macd'], df['macd_signal'] = macd, sig
    return df


rej_log = []
_orig_lr = vnz.log_rejection
vnz.log_rejection = lambda *a, **k: rej_log.append(a)
check('تقاطع MACD داخل النطاق الذهبي → إشارة Pullback', vnz.check_pullback_strategy(_pb_integration(_r_in)))
check('تقاطع MACد عند القمة → رفض فيبو', not vnz.check_pullback_strategy(_pb_integration(_r_chase)))
vnz.log_rejection = _orig_lr
check('الرفض موثق في سجل الرفضات', len(rej_log) >= 1 and 'فيبوناتشي' in str(rej_log[-1][1]), str(rej_log[-1:]))

# =====================================================================
# 5) حجم المركز على المسافة الحقيقية
# =====================================================================
print('\n=== 5) _risk_quantity + risk_stop_distance ===')
q1 = vnz._risk_quantity(8.5, 3.0)
q2 = vnz._risk_quantity(8.5, 6.0)
check('_risk_quantity: 8.5/3 = 2.8333', q1 is not None and abs(float(q1) - 8.5 / 3.0) < 1e-9, str(q1))
check('مضاعفة المسافة تنصف الكمية', q2 is not None and abs(float(q2) / float(q1) - 0.5) < 1e-9, str(q2))
check('مدخلات فاسدة → None', vnz._risk_quantity(8.5, 0) is None and vnz._risk_quantity(-1, 3) is None)
_dvol = _pb_rows(32, 105.0, [104.8], [(104.8, 104.6)])
_dvol['atr'] = 3.0   # تقلب مرتفع: 2.5×ATR يتجاوز سقف 4% → مقارنة المسافتين
d = vnz.calculate_dynamic_tp_sl(_dvol, 104.6)
check('الديناميكي يحمل risk_stop_distance', d is not None and d.get('risk_stop_distance') is not None, str(d))
if d:
    _entry = 104.6
    check('المسافة الحقيقية ≥ المسافة المقيدة', d['risk_stop_distance'] >= (_entry - d['stop_loss']) - 1e-6,
          f"{d['risk_stop_distance']} vs {_entry - d['stop_loss']}")
_ft = vnz.ft_initial_tp_sl(100.0, {'stoploss': -0.05, 'minimal_roi': {0: 0.05}, 'name': 'X'})
check('FT يحمل الكانوني 5% قبل السقف', abs(_ft['risk_stop_distance'] - 5.0) < 1e-6, str(_ft.get('risk_stop_distance')))
check('FT: الوقف المعروض مقيد بالسقف 4%', abs(_ft['stop_loss'] - 96.0) < 1e-6, str(_ft.get('stop_loss')))
_src_loop = inspect.getsource(vnz.main_loop_enhanced)
check('حلقة الفتح تمرر risk_stop_distance لحجم المركز', "new_signal.get('risk_stop_distance')" in _src_loop)
check('حلقة الفتح تستدعي محرك فيبو لليوت', 'calculate_elliott_tp_sl' in _src_loop)
# [جولة 2] محاكاة تدفق الفتح الحقيقي طرفًا لطرفًا: الكاشف bool → إعادة فحص البنية
# (لأن setup_ev فارغ: 'ELLIOTT': None) → دمج الأدلة → محرك فيبو يفتح بأهداف بنيوية
check('الكاشف يطلق إشارة Elliott على الموجات الصغيرة', vnz.check_elliott_impulse_strategy(df_small))
_ok_flow, _ev_flow = vnz._elliott_w4_setup_info(df_small)
merged_ev = {**_ev_flow, **{}}  # setup_ev فارغ تمامًا كما في الحلقة الحية
_tp_flow = vnz.calculate_elliott_tp_sl(float(df_small['close'].iloc[-1]), merged_ev) if _ok_flow else None
check('تدفق الفتح الكامل: أدلة الكاشف تغذي محرك فيبو',
      _tp_flow is not None and _tp_flow.get('source') == 'ELLIOTT_FIB_W4', str(_tp_flow))
check('الفتح يعيد فحص بنية اليوت على نفس الشموع', '_elliott_w4_setup_info(df_with_indicators)' in _src_loop)

# =====================================================================
# 6) حق النقض الانتقالي
# =====================================================================
print('\n=== 6) الريم الانتقالي: نقض الاستمرارية ===')
RI_TR = {'regime': 'transitional', 'er': 0.25, 'flips': 3, 'bbwp': 0.5, 'adx': 24.0,
         'atr_pct': 1.0, 'roc': 0.5, 'struct_up': True, 'ema50_dist_atr': 0.2}
for s in ('EMA_RSI_Cross', 'Pullback_MACD', 'Bullish_Momentum', 'FT_ADXMomentum'):
    sc = vnz.score_strategy_pair_fit(RI_TR, s)
    check(f'انتقالي → 0 لـ {s}', sc == 0.0, str(sc))
for s, lo in (('FT_CombinedBinHAndCluc', 15.0), ('Elliott_Impulse', 25.0), ('SR_Breakout_Enhanced', 15.0)):
    sc = vnz.score_strategy_pair_fit(RI_TR, s)
    check(f'انتقالي مقبول لـ {s} (≥{lo})', sc is not None and sc >= lo, str(sc))
RI_TU = dict(RI_TR, regime='trend_up')
sc_tu = vnz.score_strategy_pair_fit(RI_TU, 'EMA_RSI_Cross')
check('trend_up ما زال ممتلئًا لـ EMA_RSI', sc_tu is not None and sc_tu >= 40.0, str(sc_tu))

# =====================================================================
# 7) القيم والثوابت الاحترافية
# =====================================================================
print('\n=== 7) القيم المعيارية ===')
check('سقف مطاردة EMA_RSI = 2.0×ATR', vnz.ENTRY_QUALITY_PROFILES['EMA_RSI_Cross']['max_ext_atr'] == 2.0,
      str(vnz.ENTRY_QUALITY_PROFILES['EMA_RSI_Cross']['max_ext_atr']))
check('نطاق فيبو الذهبي [0.15, 0.618]', vnz.PULLBACK_FIB_ZONE_MIN == 0.15 and abs(vnz.PULLBACK_FIB_ZONE_MAX - 0.618) < 1e-9)
check('عتبة معنى الدفعة = 2×ATR', vnz.PULLBACK_FIB_MIN_IMPULSE_ATR == 2.0)
check('قيم المؤشرات المعيارية: RSI/ATR/ADX=14', vnz.RSI_PERIOD == 14 and vnz.ATR_PERIOD == 14 and vnz.ADX_PERIOD == 14)
check('لا استراتيجية تُحذف بلا بيانات (9 نشطة)', len(vnz.STRATEGY_PAIR_PROFILES) == 9,
      str(len(vnz.STRATEGY_PAIR_PROFILES)))
_src_pb = inspect.getsource(vnz.check_pullback_strategy)
check('Pullback يستدعي فاحص فيبو', '_pullback_fib_zone_info' in _src_pb)
_src_el = inspect.getsource(vnz._elliott_w4_setup_info)
check('عتبة اليوت تكييفية بالكود', 'atr_pct * 1.6' in _src_el)

# =====================================================================
# 8) انحدار سريع للنسخ السابقة
# =====================================================================
print('\n=== 8) انحدار V9.40/V9.41 ===')
check('سلّم الصعود V9.40 موجود', vnz.PROFIT_LOCK_LADDER_RALLY[:2] == [(0.75, 0.20), (1.50, 0.60)])
check('تهدئة الرابح 60د', vnz.PROTECTION_COOLDOWN_WIN_MIN == 60)
check('Elliott في حلقة الفحص (إصلاح V9.41 باقٍ)', "('ELLIOTT', check_elliott_impulse_strategy, 'Elliott_Impulse')" in _src_loop)
check('أرضية SR 0.60 باقية', vnz.ENTRY_QUALITY_PROFILES['SR_Breakout_Enhanced']['min_excursion_pct'] == 0.60)

print(f"\n{'=' * 60}\nالنتيجة: {len(PASS)} نجح / {len(FAIL)} فشل")
if FAIL:
    print('الفاشلة:')
    for f in FAIL:
        print('  ❌', f)
sys.exit(1 if FAIL else 0)

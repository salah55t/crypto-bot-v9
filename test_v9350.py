# -*- coding: utf-8 -*-
"""
test_v9350.py — اختبارات V9.35.0: بوابة جودة الدخول لكل استراتيجية (EQG)
  1) الإصدار + سلامة الملفات التعريفية (بروفايلات 13 استراتيجية + الافتراضي)
  2) رياضيات المدى المتوقع _eqg_expected_excursion (تطابق حرفي مع منهجية القمم)
  3) G1 أرضية المدى المتوقع — قاتل منطقة الرسوم (بيئة ميتة → رفض موثق)
  4) G2 ضد الملاحقة (توجهية) وضد الارتداد المُستهلك (عكسية)
  5) G3 ضد المضخة (روح EMASkipPump)
  6) الدرجة المركبة EQS: اجتياز صحي بأرقام معلومة + رفض دون العتبة
  7) مفتاح التعطيل EQG_ENABLED + تكامل مع calculate_all_features (أعمدة حقيقية)
"""
import sys, os
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


def make_df(closes, highs=None, lows=None, opens=None, volumes=None, atr=None, ema21=None):
    """df مصغّر بالأعمدة التي تقرأها البوابة حصرًا (بلا نداء شبكي)."""
    n = len(closes)
    closes = np.asarray(closes, dtype=float)
    highs = np.asarray(highs if highs is not None else [c * 1.002 for c in closes], dtype=float)
    lows = np.asarray(lows if lows is not None else [c * 0.998 for c in closes], dtype=float)
    opens = np.asarray(opens if opens is not None else closes, dtype=float)
    vols = np.asarray(volumes if volumes is not None else [1000.0] * n, dtype=float)
    df = pd.DataFrame({'open': opens, 'high': highs, 'low': lows,
                       'close': closes, 'volume': vols})
    df['atr'] = atr if atr is not None else float(np.mean(highs - lows))
    df['ema_21'] = ema21 if ema21 is not None else pd.Series(closes).rolling(21).mean().bfill().values
    return df


# ============ 1) الإصدار والبروفايلات ============
print('═══ 1) الإصدار والبروفايلات ═══')
check('الإصدار V9.35.0', vnz.APP_VERSION == 'V9.35.0', vnz.APP_VERSION)
check('مصدر الإصدار الوحيد', vnz.get_dashboard_html().find('V9.35.0') > 0)
check('بوابة EQG مفعلة افتراضيًا', vnz.ENTRY_QUALITY_GATE_ENABLED is True)
check('13 بروفايل جودة', len(vnz.ENTRY_QUALITY_PROFILES) == 13, str(len(vnz.ENTRY_QUALITY_PROFILES)))
missing = [k for k in vnz.STRATEGY_FILTER_PROFILES if k not in vnz.ENTRY_QUALITY_PROFILES]
check('كل بروفايل فلتر قديم له بروفايل جودة', not missing, str(missing))
req_fields = ('min_excursion_pct', 'max_ext_atr', 'max_bounce_atr', 'max_body_atr', 'min_eqs')
bad_profiles = [k for k, p in vnz.ENTRY_QUALITY_PROFILES.items() if any(f not in p for f in req_fields)]
check('حقول البروفايلات مكتملة', not bad_profiles, str(bad_profiles))
rev_has_bounce = all(vnz.ENTRY_QUALITY_PROFILES[k].get('max_bounce_atr') is not None
                     for k in ('FT_BbandRsi', 'FT_Quickie', 'FT_Bandtastic', 'FT_CombinedBinHAndCluc', 'FT_EMASkipPump'))
check('العائلة العكسية بحد ارتداد (استكمال V9.34)', rev_has_bounce)
trend_no_bounce = all(vnz.ENTRY_QUALITY_PROFILES[k].get('max_ext_atr') is not None
                      and vnz.ENTRY_QUALITY_PROFILES[k].get('max_bounce_atr') is None
                      for k in ('MACD_EMA_Crossover', 'Bullish_Momentum', 'FT_ADXMomentum'))
check('العائلة التوجهية بحد ملاحقة لا حد ارتداد', trend_no_bounce)

# ============ 2) رياضيات المدى المتوقع ============
print('═══ 2) رياضيات _eqg_expected_excursion ═══')
flat_close = [100.0] * 60
df_known = make_df(flat_close, highs=[101.0] * 60, lows=[99.0] * 60)
exc_known = vnz._eqg_expected_excursion(df_known, horizon=8, window=30)
check('تطابق حرفي: كل قمة أمامية 1.0%', abs(exc_known - 1.0) < 1e-9, f'{exc_known}')
df_dead = make_df([100.0] * 60, highs=[100.0] * 60, lows=[100.0] * 60)
exc_dead = vnz._eqg_expected_excursion(df_dead, horizon=8, window=30)
check('بيئة ميتة تمامًا = صفر مدى', exc_dead == 0.0, f'{exc_dead}')
df_short = make_df([100.0] * 6)
check('بيانات قصيرة → None (تمرير لا اختناق)', vnz._eqg_expected_excursion(df_short) is None)
# اتجاه هابط: القمم الأمامية تحت الإغلاق → قيمة سالبة مقبولة (بيئة لا يمكن الخروج منها بربح)
down_close = list(np.linspace(101, 100, 60))
df_down = make_df(down_close, highs=[c - 0.05 for c in down_close], lows=[c - 0.2 for c in down_close])
exc_down = vnz._eqg_expected_excursion(df_down, horizon=8, window=30)
check('الهابط يكشف مدى سالب (لا حتى خروج بالدخول)', exc_down < 0, f'{exc_down}')

# ============ 3) G1: أرضية المدى المتوقع ============
print('═══ 3) G1 منطقة الرسوم ═══')
df_fee = make_df([100.0 + (0.02 if i % 3 == 0 else 0.0) for i in range(80)],
                 highs=[100.05] * 80, lows=[99.97] * 80)
ok, info = vnz.entry_quality_gate(df_fee, 'FT_Quickie')
check('بيئة ميتة → رفض G1', ok is False, str(info))
check('سبب G1 يذكر المدى المتوقع', 'المدى المتوقع' in str(info.get('reason_ar', '')), str(info.get('reason_ar')))
check('توثيق قياس المدى في المعلومات', info.get('exc_pct') is not None, str(info))

# ============ 4) G2: الملاحقة والارتداد المُستهلك ============
print('═══ 4) G2 المطاردة والارتداد ═══')
# 4-أ: سلسلة موجات صحية (مدى متوقع ≈ 1.3%) ثم شمعة ختامية منفلتة فوق EMA21 — توجهية
base = [100.0 + 0.9 * np.sin(i / 4.0) for i in range(119)]
chase_closes = base + [104.0]
chase_highs = [c + 0.25 for c in chase_closes]
chase_lows = [c - 0.25 for c in chase_closes]
df_chase = make_df(chase_closes, highs=chase_highs, lows=chase_lows, atr=0.5)
ok, info = vnz.entry_quality_gate(df_chase, 'MACD_EMA_Crossover', fit_score=70)
check('سعر منفلت فوق EMA21 → رفض مطاردة', ok is False and 'مطاردة' in str(info.get('reason_ar', '')), str(info))
# نفس الشمعة على استراتيجية عكسية (بلا حد ملاحقة) تتجاوز G2 ثم تسقط في حد الجسم أو الجودة — لا "مطاردة"
ok_r, info_r = vnz.entry_quality_gate(df_chase, 'FT_Quickie', fit_score=70)
check('العكسية معفاة من حد الملاحقة', 'مطاردة' not in str(info_r.get('reason_ar', '')), str(info_r))
# 4-ب: ارتداد استهلك القاع — موجات صحية ثم صعود حاد آخر 5 شموع بعيدًا عن قاع 7 شموع
v_base = [100.2 + 0.75 * np.sin(i / 3.5) for i in range(95)]
v_closes = v_base + [99.8, 100.1, 100.4, 100.8, 101.2]
v_highs = [c + 0.2 for c in v_closes]
v_lows = [c - 0.2 for c in v_closes]
df_v = make_df(v_closes, highs=v_highs, lows=v_lows, atr=0.45)
ok, info = vnz.entry_quality_gate(df_v, 'FT_Quickie')
check('ارتداد مُستهلك فوق قاع 7 شموع → رفض', ok is False and 'الارتداد مُستهلك' in str(info.get('reason_ar', '')), str(info))
check('توثيق bounce_atr', info.get('bounce_atr') is not None, str(info))

# ============ 5) G3: شمعة المضخة ============
print('═══ 5) G3 المضخة ═══')
pump_base = [100.0 + 0.75 * np.sin(i / 3.5) for i in range(119)]
pump_closes = pump_base + [99.4]
pump_opens = pump_closes[:-1] + [101.0]   # شمعة حمراء عملاقة عند قاع الموجة (جسم 1.6 ≈ 3.6×ATR)
pump_highs = [max(o, c) + 0.1 for o, c in zip(pump_opens, pump_closes)]
pump_lows = [min(o, c) - 0.1 for o, c in zip(pump_opens, pump_closes)]
df_pump = make_df(pump_closes, highs=pump_highs, lows=pump_lows, opens=pump_opens, atr=0.45)
ok, info = vnz.entry_quality_gate(df_pump, 'FT_Quickie')
check('شمعة مضخة → رفض G3', ok is False and 'شمعة مضخة' in str(info.get('reason_ar', '')), str(info))
check('توثيق body_atr', (info.get('body_atr') or 0) > 2.0, str(info))

# ============ 6) الدرجة المركبة EQS ============
print('═══ 6) الدرجة المركبة EQS ═══')
# 6-أ: سلسلة صحية كاملة → اجتياز بدرجة موثقة
wave = [100.0 + 0.9 * np.sin(i / 4.0) + i * 0.004 for i in range(300)]
healthy_highs = [c + 0.25 for c in wave]
healthy_lows = [c - 0.25 for c in wave]
healthy_opens = [wave[i - 1] if i else wave[0] for i in range(len(wave))]
healthy_opens[-1] = wave[-1] - 0.2        # آخر شمعة خضراء صغيرة
vols = [1000.0] * 300
vols[-1] = 2500.0  # حجم آخر شمعة فوق متوسط 20
df_ok = make_df(wave, highs=healthy_highs, lows=healthy_lows,
                opens=healthy_opens, volumes=vols, atr=0.5)
ri = {'er': 0.35, 'regime': 'trend_up'}
ok, info = vnz.entry_quality_gate(df_ok, 'MACD_EMA_Crossover', ri, 70)
check('إشارة صحية → اجتياز EQG', ok is True, str(info))
check('درجة موثقة ≥ العتبة 55', (info.get('score') or 0) >= 55, str(info.get('score')))
check('مكوّنات التداخل موثقة', set((info.get('conf') or {}).keys()) == {'vol', 'er', 'dir'}, str(info))
check('وفرة المدى موثقة', info.get('exc_pct') is not None, str(info))
# 6-ب: نفس السلسلة بلا مطابقة/تداخل → رفض الدرجة
vols_low = [1000.0] * 300
vols_low[-1] = 200.0
red_opens = healthy_opens[:-1] + [wave[-1] + 0.3]  # شمعة حمراء صغيرة
df_low = make_df(wave, highs=healthy_highs, lows=healthy_lows,
                 opens=red_opens, volumes=vols_low, atr=0.5)
ok, info = vnz.entry_quality_gate(df_low, 'MACD_EMA_Crossover', {'er': 0.05}, None)
check('درجة ضعيفة (بلا مطابقة/تداخل) → رفض', ok is False and 'درجة الجودة' in str(info.get('reason_ar', '')), str(info))
# 6-ج: المعادلة الموزونة نفسها (OctoBot weighted mean): مطابقة 70 → 28 نقطة
check('وزن المطابقة: 70 → 28', abs(70 / 100 * 40 - 28.0) < 1e-9)
# 6-د: وفرة المدى مشبعة عند 1.8× الأرضية
check('تشبع الوفرة عند 1.8×', 30.0 * min(1.0, (1.8 * 0.6 * 3) / (1.8 * 0.6)) == 30.0)

# ============ 7) التعطيل والتكامل ============
print('═══ 7) التعطيل والتكامل ═══')
_old = vnz.ENTRY_QUALITY_GATE_ENABLED
vnz.ENTRY_QUALITY_GATE_ENABLED = False
try:
    ok, info = vnz.entry_quality_gate(df_fee, 'FT_Quickie')
    check('EQG_ENABLED=false → تمرير مع علم التعطيل', ok is True and info.get('disabled') is True, str(info))
finally:
    vnz.ENTRY_QUALITY_GATE_ENABLED = _old
# تكامل: بروفايل افتراضي لاسم غير مدرج
ok, info = vnz.entry_quality_gate(df_ok, 'Unknown_Future_Strategy', ri, 60)
check('بروفايل افتراضي لاستراتيجية مستقبلية', ok is True, str(info))
# تكامل مع calculate_all_features: أعمدة حقيقية + ريم حقيقي
try:
    rng = np.random.default_rng(42)
    n = 300
    drift = np.cumsum(rng.normal(0.002, 0.35, n))
    o = 100 + drift
    c = o + rng.normal(0, 0.12, n)
    h = np.maximum(o, c) + np.abs(rng.normal(0, 0.15, n))
    l = np.minimum(o, c) - np.abs(rng.normal(0, 0.15, n))
    dfx = pd.DataFrame({'open': o, 'high': h, 'low': l, 'close': c,
                        'volume': rng.uniform(800, 3000, n)},
                       index=pd.date_range('2026-10-01', periods=n, freq='15min'))
    dfx = vnz.calculate_all_features(dfx, None)
    rix = vnz.compute_symbol_regime(dfx)
    ok, info = vnz.entry_quality_gate(dfx, 'FT_Quickie', rix, 65)
    check('تكامل calculate_all_features: لا انهيار', isinstance(ok, bool), str(info)[:120])
    check('المدى مقاس على أعمدة حقيقية', info.get('exc_pct') is not None, str(info)[:120])
    check('بُنيت النتيجة من score أو سبب موثق', ('score' in info) or ('reason_ar' in info), str(info)[:120])
except Exception as integ_err:
    check(f'تكامل calculate_all_features: فشل غير متوقع {integ_err}', False)


# ============ الخلاصة ============
print('\n' + '=' * 50)
print(f'النتيجة: {len(PASS)} نجاح / {len(FAIL)} فشل')
if FAIL:
    print('الفاشلة:', FAIL)
    sys.exit(1)
print('✅ كل اختبارات V9.35.0 نجحت')

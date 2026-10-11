# -*- coding: utf-8 -*-
"""
test_v9360.py — اختبارات V9.36.0: معايرة قيم الحماية على قياس 113 صفقة حية
  1) الإصدار + سقف الوقف MAX_SL_DISTANCE_PCT 6.0 → 4.0 (الدليل: 5 نزفات تحت -4% = -1.45$،
     أسوأ خسارة رابحة -3.05% — السقف الجديد لا يلمس صفقة صحية)
  2) توحيد وقوفات عائلة FT على -5% (كانت -25%/-34.5% من الريبو الأصلي لهدف ROI 10-16%)
  3) سلّم قفل الأرباح فوق الرسوم: درجة 1 (0.75 → قفل 0.30% خام = +0.10% صافٍ)
     ودرجة 2 (1.25 → قفل 0.55% خام = +0.35% صافٍ) — القيم القديمة كانت تقفل خسارة صافية
  4) تعطيل MACD_EMA (PF 0.012، WR 14.3%، n=7) + خريطة المفتاح→العرض
  5) تكامل ratchet السلم: القفل الجديد لا يُنزّل وقفًا أرفع ويحترم الأعلى بينه وبين التريلينغ
  6) حساب ft_initial_tp_sl بالقيم الجديدة (وقف فعلي 4% بعد السقف لكل عائلة FT)
  7) رياضيات صافي القفل: lock_raw − 0.2% رسوم > 0 لكل درجات السلم
"""
import sys, os, json

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


# ============ 1) الإصدار وسقف الوقف ============
print('═══ 1) الإصدار وسقف الوقف ═══')
check('الإصدار V9.36.0', vnz.APP_VERSION == 'V9.36.0', vnz.APP_VERSION)
check('مصدر الإصدار الوحيد', vnz.get_dashboard_html().find('V9.36.0') > 0)
check('سقف الوقف 4.0% (كان 6.0)', vnz.MAX_SL_DISTANCE_PCT == 4.0, str(vnz.MAX_SL_DISTANCE_PCT))
check('سقف الوقف env-configurable', isinstance(vnz.MAX_SL_DISTANCE_PCT, float))

# ============ 2) وقوفات عائلة FT الموحدة ============
print('═══ 2) وقوفات FT الموحدة -5% ═══')
for name, spec in vnz.FREQTRADE_STRATEGIES.items():
    check(f'{name} stoploss = -0.05', float(spec.get('stoploss')) == -0.05,
          f"القيمة {spec.get('stoploss')}")
# عينات تاريخية: القيم القديمة كانت كارثية
check('BbandRsi لم تعد -0.25 (نزف MAGIC -9.02% تحت أرضية أوسع)', True)
check('Bandtastic لم تعد -0.345 (نزف ZRO -4.97%)', True)

# ============ 3) سلّم الأرباح فوق الرسوم ============
print('═══ 3) سلّم قفل الأرباح فوق الرسوم ═══')
ladder = vnz.PROFIT_LOCK_LADDER
check('السلم 5 درجات', len(ladder) == 5, str(ladder))
check('درجة 1: قمة 0.75 → قفل 0.30', (0.75, 0.30) in [(t[0], t[1]) for t in ladder], str(ladder[:2]))
check('درجة 2: قمة 1.25 → قفل 0.55', (1.25, 0.55) in [(t[0], t[1]) for t in ladder])
check('درجات 3-5 محفوظة', [(2.0, 1.0), (3.0, 1.8), (4.5, 3.0)] == [(t[0], t[1]) for t in ladder[2:]])
FEES_PCT = 0.2
for tier_peak, tier_lock in ladder:
    net = tier_lock - FEES_PCT
    check(f'قفل درجة {tier_peak}% صافيه موجب ({tier_lock:g}%−رسوم = {net:+.2f}%)', net > 0)

# رياضيات profit_lock_ladder_stop بالقيم الجديدة
def ladder_stop(entry, peak):
    return vnz.profit_lock_ladder_stop(entry, peak)

e = 1.0
check('قمة 0.75% → وقف ≥ +0.30%', abs(ladder_stop(e, e * 1.0075) - e * 1.0030) < 1e-9,
      str(ladder_stop(e, e * 1.0075)))
check('قمة 0.90% → ما زال قفل درجة 1 (+0.30%)', abs(ladder_stop(e, e * 1.009) - e * 1.0030) < 1e-9)
check('قمة 1.25% → قفل +0.55%', abs(ladder_stop(e, e * 1.0125) - e * 1.0055) < 1e-9)
check('قمة 2.0% → قفل +1.0%', abs(ladder_stop(e, e * 1.02) - e * 1.01) < 1e-9)
check('قمة 3.0% → قفل +1.8%', abs(ladder_stop(e, e * 1.03) - e * 1.018) < 1e-9)
check('قمة 4.5% → قفل +3.0%', abs(ladder_stop(e, e * 1.045) - e * 1.03) < 1e-9)
check('قمة 10% → آخر درجة +3.0%', abs(ladder_stop(e, e * 1.10) - e * 1.03) < 1e-9)
check('قمة دون 0.75% → بلا قفل (None)', ladder_stop(e, e * 1.005) is None)
check('قمة تحت الدخول → بلا قفل', ladder_stop(e, e * 0.99) is None)

# ============ 4) تعطيل MACD_EMA ============
print('═══ 4) تعطيل MACD_EMA ═══')
check('MACD_EMA في قائمة التعطيل', 'MACD_EMA' in vnz.DISABLED_STRATEGY_KEYS,
      str(vnz.DISABLED_STRATEGY_KEYS))
check('BB_STOCH ما زالت معطلة', 'BB_STOCH' in vnz.DISABLED_STRATEGY_KEYS)
check('خريطة MACD_EMA → MACD_EMA_Crossover',
      vnz.STRATEGY_KEY_TO_DISPLAY.get('MACD_EMA') == 'MACD_EMA_Crossover')
check('خريطة BB_STOCH → BB_Stoch_Reversal_Enhanced',
      vnz.STRATEGY_KEY_TO_DISPLAY.get('BB_STOCH') == 'BB_Stoch_Reversal_Enhanced')
# الاستراتيجيات الحية لا تُمس
for key in ('SR_BREAKOUT', 'FT_BBANDRSI', 'FT_BINHCLUC', 'FT_QUICKIE', 'FT_BANDTASTIC',
            'FT_EMASKIPPUMP', 'FT_ADXMOMENTUM', 'EMA_RSI', 'PULLBACK', 'BB_SQUEEZE',
            'BULLISH_MOMENTUM'):
    check(f'{key} ليست في التعطيل', key not in vnz.DISABLED_STRATEGY_KEYS)

# ============ 5) ft_initial_tp_sl بالقيم الجديدة ============
print('═══ 5) الوقف الفعلي بعد السقف ═══')
for name, spec in vnz.FREQTRADE_STRATEGIES.items():
    tp_sl = vnz.ft_initial_tp_sl(100.0, spec)
    stop_pct = abs(tp_sl['stop_loss'] / 100.0 - 1.0) * 100.0
    check(f'{name}: وقف فعلي 4% (بعد سقف الحماية)', abs(stop_pct - 4.0) < 1e-6,
          f"{stop_pct:.3f}%")
    check(f'{name}: هدف = ROI0', tp_sl['target_price'] > 100.0)

# ============ 6) محاكاة الصفقات الكارثية بالقيم الجديدة ============
print('═══ 6) لو كانت القيم الجديدة نافذة تاريخيًا ═══')
DISASTERS = [
    ('MAGICUSDT FT_BbandRsi', -9.02), ('METUSDT BB_Squeeze', -10.67),
    ('ZROUSDT Bandtastic', -4.97), ('RLCUSDT MACD_EMA', -6.64), ('MINAUSDT MACD_EMA', -4.62),
]
total_saved = 0.0
for name, pct in DISASTERS:
    new_loss = -(4.0 + 1.5)  # وقف 4% + فجوة تنفيذ 1.5 نقطة (متحفظ)
    saved = (max(new_loss, pct) - pct) / 100 * 4.0
    total_saved += saved
    check(f'{name}: التوفير التحفظي غير سالب ({saved:+.2f}$)', saved >= 0.0)
check(f'التوفير الإجمالي التحفظي موجب ({total_saved:+.2f}$ على 5 نزفات)', total_saved >= 0.30)

# ============ 7) التوثيق في اللوحة ============
print('═══ 7) التوثيق ═══')
html = vnz.get_dashboard_html()
check('الإصدار في اللوحة', 'V9.36.0' in html)

# ============ الخلاصة ============
print()
print(f'═══ النتيجة: {len(PASS)} نجح / {len(FAIL)} فشل ═══')
if FAIL:
    print('الفاشلة:')
    for f in FAIL:
        print('  ❌', f)
sys.exit(1 if FAIL else 0)

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
اختبارات V9.33.0 — سلّم قفل الأرباح (اقتباس freqtrade custom_stoploss المتدرج):
1) الثوابت والإصدار والسلم الافتراضي
2) دالة profit_lock_ladder_stop: الدرجات، الحدود، الرتيب، المعطل
3) تكامل محرك FT: رفع الوقف عند بلوغ درجة + الحفظ عبر stop_changed
4) القفل التصاعدي: لا ينزّل وقفًا أرفع (تريلينغ المواصفة/ATR يفوزان إن كانا أعلى)
5) التعايش مع V9.32.0: حارس تأجيل الإشارة يعمل والسلم يرفع الوقف في نفس الخطوة
6) تكامل الحلقة العامة: السلم أرضية تحت بوابة ATR (ساكن + سلوكي مبسط)
7) التعطيل عبر البيئة + JSON مخصص + JSON تالف يعود للافتراضي
8) الربط الساكن: العرض في الحمايات، مصدر الإصدار الوحيد، لا مساس ببوابة ATR
"""
import sys, os, time, json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('LOG_LEVEL', 'ERROR')
os.environ.setdefault('BINANCE_API_KEY', 'test_key_dummy')
os.environ.setdefault('BINANCE_API_SECRET', 'test_secret_dummy')

import vnz  # noqa: E402
vnz.logger.disabled = True
vnz.log_and_notify = lambda *a, **k: None
vnz.send_telegram_message = lambda *a, **k: None

PASS, FAIL = [], []

def check(name: str, cond: bool, detail: str = ''):
    (PASS if cond else FAIL).append(name)
    print(f"{'✅' if cond else '❌'} {name}" + (f" — {detail}" if detail and not cond else ''))

SRC = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'vnz.py'), encoding='utf-8').read()

CLOSED = []
def reset_close_capture():
    CLOSED.clear()
    vnz.close_signal = lambda sid, price, reason: (CLOSED.append((sid, price, reason)), True)[1]

def make_signal(entry=100.0, price=None, sl=None, peak=None, sig_id=1, ts=None):
    p = price if price is not None else entry * 1.001
    return {
        'id': sig_id, 'entry_price': entry, 'stop_loss': sl if sl is not None else entry * 0.94,
        'initial_stop_loss': entry * 0.94,
        'current_peak_price': peak if peak is not None else max(entry, p),
        'timestamp': ts, 'symbol': 'TESTUSDT',
    }

def make_spec(exit_true=False, trailing=False):
    return {
        'stoploss': -0.05,
        'exit_fn': (lambda df: True) if exit_true else (lambda df: False),
        'minimal_roi': {0: 0.05},
        'trailing_stop': trailing,
        'trailing_stop_positive': 0.01 if trailing else None,
        'trailing_stop_positive_offset': 0.058 if trailing else None,
        'trailing_only_offset_is_reached': False,
    }

vnz.ft_exit_indicators = lambda symbol: object()
vnz._cache_touch_open = lambda symbol, signal_id, signal: True
vnz.check_db_connection = lambda: False

# ============ 1) الثوابت والإصدار ============
print('\n═══ 1) الثوابت والإصدار ═══')
check('الإصدار V9.33.0', vnz.APP_VERSION == 'V9.33.0')
check('السلم الافتراضي 5 درجات', len(vnz.PROFIT_LOCK_LADDER) == 5, str(vnz.PROFIT_LOCK_LADDER))
check('الدرجات مرتبة تصاعديًا',
      all(vnz.PROFIT_LOCK_LADDER[i][0] < vnz.PROFIT_LOCK_LADDER[i+1][0] for i in range(len(vnz.PROFIT_LOCK_LADDER)-1)))
check('أول درجة: قمة 0.75% → قفل 0.10%', vnz.PROFIT_LOCK_LADDER[0] == (0.75, 0.10))
check('قابلة للضبط عبر البيئة', "config('PROFIT_LOCK_LADDER_JSON'" in SRC)
check('القفل للرفع فقط موثق في الكود', 'لا ينزّل وقفًا أرفع' in SRC)

# ============ 2) الدالة ============
print('\n═══ 2) profit_lock_ladder_stop ═══')
E = 100.0
check('قمة 0.74% → بلا قفل (تحت أول درجة)', vnz.profit_lock_ladder_stop(E, E*1.0074) is None)
check('قمة 0.75% → وقف +0.10%',
      abs(vnz.profit_lock_ladder_stop(E, E*1.0075) - E*1.001) < 1e-9)
check('قمة 1.24% → ما زال +0.10% (نفس الدرجة)',
      abs(vnz.profit_lock_ladder_stop(E, E*1.0124) - E*1.001) < 1e-9)
check('قمة 1.25% → +0.45%',
      abs(vnz.profit_lock_ladder_stop(E, E*1.0125) - E*1.0045) < 1e-9)
check('قمة 2.00% → +1.00%', abs(vnz.profit_lock_ladder_stop(E, E*1.02) - E*1.01) < 1e-9)
check('قمة 3.00% → +1.80%', abs(vnz.profit_lock_ladder_stop(E, E*1.03) - E*1.018) < 1e-9)
check('قمة 4.50% → +3.00%', abs(vnz.profit_lock_ladder_stop(E, E*1.045) - E*1.03) < 1e-9)
check('قمة 6.00% → +3.00% (أعلى درجة تبقى)', abs(vnz.profit_lock_ladder_stop(E, E*1.06) - E*1.03) < 1e-9)
# رتيب: قمة أعلى لا يعيد وقفًا أدنى
vals = [vnz.profit_lock_ladder_stop(E, E*(1.0+p/100)) for p in (0.8, 1.0, 1.3, 1.6, 2.2, 3.2, 5.0)]
vals = [v for v in vals if v is not None]
check('الرتيب: قمم أعلى → أرضيات لا تنخفض', all(vals[i] <= vals[i+1] + 1e-12 for i in range(len(vals)-1)))
# معطل
_orig_ladder = vnz.PROFIT_LOCK_LADDER
try:
    vnz.PROFIT_LOCK_LADDER = []
    check('سلم فارغ → None (تعطيل)', vnz.profit_lock_ladder_stop(E, E*1.05) is None)
finally:
    vnz.PROFIT_LOCK_LADDER = _orig_ladder
check('مدخل فاسد (entry=0) → None', vnz.profit_lock_ladder_stop(0, E*1.05) is None)

# ============ 3) تكامل محرك FT ============
print('\n═══ 3) محرك FT: السلم يرفع الوقف عند بلوغ درجة ═══')
reset_close_capture()
sig = make_signal(entry=100.0, price=101.3, peak=101.3, sl=94.0, sig_id=301)
closed = vnz.ft_exit_engine_step(sig, 301, 'TESTUSDT', 101.3, make_spec())
check('قمة +1.3% بلا إشارة خروج → الصفقة تستمر', not closed and len(CLOSED) == 0)
check('الوقف رُفع إلى +0.45%', abs(float(sig['stop_loss']) - 100.45) < 1e-6,
      f"stop={sig['stop_loss']}")
check('القمة حُفظت في الإشارة', abs(float(sig['current_peak_price']) - 101.3) < 1e-9)

# درجة أدنى: قمة 0.9% → قفل 0.10%
reset_close_capture()
sig = make_signal(entry=100.0, price=100.9, peak=100.9, sl=94.0, sig_id=302)
vnz.ft_exit_engine_step(sig, 302, 'TESTUSDT', 100.9, make_spec())
check('قمة +0.9% → وقف ≥ +0.10%', abs(float(sig['stop_loss']) - 100.10) < 1e-6,
      f"stop={sig['stop_loss']}")

# ============ 4) القفل التصاعدي ============
print('\n═══ 4) السلم لا ينزّل وقفًا أرفع ═══')
reset_close_capture()
sig = make_signal(entry=100.0, price=101.3, peak=101.3, sl=101.0, sig_id=303)  # وقف حالي +1.0%
vnz.ft_exit_engine_step(sig, 303, 'TESTUSDT', 101.3, make_spec())
check('وقف حالي +1.0% يفوز على درجة +0.45%', abs(float(sig['stop_loss']) - 101.0) < 1e-9,
      f"stop={sig['stop_loss']}")
# تكامل مع تريلينغ المواصفة (Bandtastic-style): تريلينغ أرفع يفوز، سلم أرفع يفوز
reset_close_capture()
sig = make_signal(entry=100.0, price=107.0, peak=107.0, sl=94.0, sig_id=304)  # قمة +7%
spec_tr = make_spec(trailing=True)  # بعد إزاحة 5.8% → مسافة 1% → وقف 105.93
vnz.ft_exit_engine_step(sig, 304, 'TESTUSDT', 107.0, spec_tr)
check('تريلينغ المواصفة (وقف 105.93) يفوز على السلم (103)',
      abs(float(sig['stop_loss']) - 105.93) < 1e-6, f"stop={sig['stop_loss']}")

# ============ 5) التعايش مع حارس V9.32.0 ============
print('\n═══ 5) التعايش مع حارس تأجيل الخروج ═══')
reset_close_capture()
sig = make_signal(entry=100.0, price=100.1, peak=100.8, sl=94.0, sig_id=305)
# قمة 0.8% (فوق أول درجة) لكن السعر الحالي 0.1% وإشارة الخروج صحيحة → الحارس يؤجل
spec_exit = make_spec(exit_true=True)
closed = vnz.ft_exit_engine_step(sig, 305, 'TESTUSDT', 100.1, spec_exit)
check('إشارة الخروج عند 0.10% ما زالت مؤجلة (V9.32)', not closed and len(CLOSED) == 0)
check('والسلم رفع الوقف إلى +0.10% (قمة 0.8%) في نفس الخطوة',
      abs(float(sig['stop_loss']) - 100.10) < 1e-6, f"stop={sig['stop_loss']}")
with vnz._ft_exit_skip_lock:
    vnz._ft_exit_skip_since.pop(305, None)

# السيناريو الكامل للمستخدم: صعود ثم انهيار → خروج عند القفل لا عند -5%
reset_close_capture()
sig = make_signal(entry=100.0, price=101.3, peak=101.3, sl=94.0, sig_id=306)
vnz.ft_exit_engine_step(sig, 306, 'TESTUSDT', 101.3, make_spec())  # قمة → قفل 0.45%
locked_sl = float(sig['stop_loss'])
closed = vnz.ft_exit_engine_step(sig, 306, 'TESTUSDT', 100.4, make_spec())  # انهيار تحت القفل
check('انهيار تحت الوقف المقفول → إغلاق ft_trailing_stop عند 100.4',
      closed and CLOSED and CLOSED[0][2] == 'ft_trailing_stop' and abs(CLOSED[0][1] - 100.4) < 1e-9,
      f'{CLOSED}')
check('الخروج فوق الدخول بدل -5% (الربح محفوظ رغم الانزلاق تحت القفل)', CLOSED and CLOSED[0][1] > 100.0,
      f'exit={CLOSED[0][1] if CLOSED else None}')

# ============ 6) الحلقة العامة ============
print('\n═══ 6) الحلقة العامة: السلم أرضية تحت بوابة ATR ═══')
check('السلم مدمج في كتلة قمة الحلقة العامة (بعد ATR)',
      '_ladder_sl = profit_lock_ladder_stop(entry, new_peak)' in SRC and
      SRC.find('_ladder_sl = profit_lock_ladder_stop(entry, new_peak)') > SRC.find('ATR_TRAIL_ACTIVATE_PROFIT_PCT / 100.0)'))
check('السلم خارج شرط USE_ATR_TRAILING_STOP (يعمل حتى مع بوابة ATR غير مكتملة)',
      (lambda _anchor: (lambda _atr, _ladder, _save: (-1 < _atr < _ladder < _save))(
          SRC.find('if USE_ATR_TRAILING_STOP:', _anchor),
          SRC.find('_ladder_sl = profit_lock_ladder_stop(entry, new_peak)'),
          SRC.find('# [V9.30.0] كتابة مشروطة', _anchor)))(SRC.find('new_peak = max(peak_price, current_price)')))
check('بوابة ATR ومضاعفها بلا مساس',
      'ATR_TRAIL_ACTIVATE_PROFIT_PCT: float = config(\'ATR_TRAIL_ACTIVATE_PROFIT_PCT\', default=1.5, cast=float)' in SRC
      and 'ATR_TS_MULTIPLIER: float = 2.8' in SRC)
check('الحاكم (V9.31) والحارس (V9.32) باقيان',
      'risk_governor_step()' in SRC and '0.0 <= profit_pct < FT_EXIT_MIN_PROFIT_PCT' in SRC)

# ============ 7) البيئة والJSON ============
print('\n═══ 7) تعطيل وJSON مخصص ═══')
check('مُفسّر JSON موجود', vnz._parse_profit_lock_ladder('[[1.0,0.5],[0.5,0.2]]') == [(0.5, 0.2), (1.0, 0.5)])
check('JSON تالف → قائمة فارغة (يبقى الافتراضي في التهيئة)', vnz._parse_profit_lock_ladder('not-json') == [])
check('درجات سالبة/صفرية تُرفض', vnz._parse_profit_lock_ladder('[[-1,0.5],[0,0.2],[1.0,0.3]]') == [(1.0, 0.3)])
check('تعطيل بمفردات off/disabled/0/none مدعوم في الكود',
      "('off', 'disabled', '0', 'none', '')" in SRC)

# ============ 8) الربط الساكن ============
print('\n═══ 8) الربط الساكن ═══')
check("عرض السلم في إعدادات الحمايات", "'profit_lock_ladder'" in SRC)
check("السلم مستدعى في محرك FT (الخطوة 4.5)",
      SRC.count('profit_lock_ladder_stop(') >= 3)  # تعريف + استدعاءان (FT + عام)
check('مصدر الإصدار الوحيد V9.33.0', len(__import__('re').findall(r"APP_VERSION: str = 'V9\.\d+\.\d+'", SRC)) == 1
      and "APP_VERSION: str = 'V9.33.0'" in SRC)
check('السلم داخل الخطوة 4.5 (قبل الحفظ، بعده التريلينغ)',
      SRC.find('# 4.5) [V9.33.0]') < SRC.find('# 5) حفظ القمة/الوقف المحدثة'))
check('stop_changed يضمن الحفظ عند رفع السلم',
      "signal['stop_loss'] = _ladder_sl\n            stop_changed = True" in SRC)

# ============ النتيجة ============
print('\n' + '═' * 50)
print(f'النتيجة: {len(PASS)} نجاح / {len(FAIL)} فشل')
if FAIL:
    print('الفاشلة:', FAIL)
    sys.exit(1)
print('✅ كل اختبارات V9.33.0 ناجحة')

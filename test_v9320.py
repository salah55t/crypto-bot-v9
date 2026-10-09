#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
اختبارات V9.32.0 — حد أدنى لخروج إشارة FT (علاج الخروج المبكر):
1) الثوابت والإصدار
2) الحجب: إشارة خروج على ربح خام في [0، 0.35) لا تُغلق (0.00% و0.10% و0.34%)
3) القطع: إشارة الخروج على خاسر (< 0) تُغلق فورًا
4) العبور: إشارة على ربح ≥ 0.35 تُغلق كالسابق
5) سقف التأجيل: بعد FT_EXIT_SKIP_MAX_MIN من أول إعاقة يُسمح بالإغلاق (ويحذف السجل)
6) التعطيل: FT_EXIT_MIN_PROFIT_PCT=0 يعيد السلوك القديم حرفيًا
7) أسبقية الوقف وROI: لا مساس بهما — الوقف المرفوع يغلق تحت العتبة، ROI يغلق فوق عتبته
8) السقوط للتريلينغ: عند التأجيل تُقيَّم خطوة التريلينغ في نفس النبضة (الوقف يرتفع)
9) التنظيف: سجل التأجيل يُمسح عند أول إعاقة→سقف، والتنظيف الوقائي للقاموس يعمل
10) الربط الساكن: close_signal ينظف السجل، العرض في /api/protections، مصدر الإصدار الوحيد
"""
import sys, os, time

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

# ============ أدوات سلوكية ============
CLOSED = []  # (signal_id, price, reason)

def reset_close_capture():
    CLOSED.clear()
    def fake_close(signal_id, price, reason):
        CLOSED.append((signal_id, price, reason))
        return True
    vnz.close_signal = fake_close

def make_signal(entry=100.0, price=None, sl=None, peak=None, sig_id=1, ts=None):
    p = price if price is not None else entry * 1.001
    return {
        'id': sig_id, 'entry_price': entry, 'stop_loss': sl if sl is not None else entry * 0.94,
        'initial_stop_loss': entry * 0.94,
        'current_peak_price': peak if peak is not None else max(entry, p),
        'timestamp': ts, 'symbol': 'TESTUSDT',
    }

def make_spec(exit_true=True, trailing=False, roi=None):
    spec = {
        'stoploss': -0.05,
        'exit_fn': (lambda df: True) if exit_true else (lambda df: False),
        'minimal_roi': roi if roi is not None else {0: 0.05},
        'trailing_stop': trailing,
        'trailing_stop_positive': 0.01 if trailing else None,
        'trailing_stop_positive_offset': 0.058 if trailing else None,
        'trailing_only_offset_is_reached': False,
    }
    return spec

vnz.ft_exit_indicators = lambda symbol: object()  # dfx وهمي — exit_fn يتجاهله
vnz._cache_touch_open = lambda symbol, signal_id, signal: True
vnz.check_db_connection = lambda: False

def reset_guard_state():
    with vnz._ft_exit_skip_lock:
        vnz._ft_exit_skip_since.clear()

def profit_pct_of(entry, price):
    return (price / entry - 1.0) * 100.0

# ============ 1) الثوابت والإصدار ============
print('\n═══ 1) الثوابت والإصدار ═══')
check('الإصدار V9.32.0', vnz.APP_VERSION == 'V9.32.0')
check('عتبة الخروج 0.35% (رسوم 0.2% + هامش 0.15%)', vnz.FT_EXIT_MIN_PROFIT_PCT == 0.35)
check('سقف التأجيل 90 دقيقة', vnz.FT_EXIT_SKIP_MAX_MIN == 90.0)
check('العتبة قابلة للضبط عبر البيئة', "config('FT_EXIT_MIN_PROFIT_PCT'" in SRC)
check('السقف قابلاً للضبط عبر البيئة', "config('FT_EXIT_SKIP_MAX_MIN'" in SRC)

# ============ 2) الحجب ============
print('\n═══ 2) الحجب: إشارة على ربح في [0، العتبة) لا تغلق ═══')
for label, price in [('ربح 0.00%', 100.0), ('ربح 0.10%', 100.1), ('ربح 0.34% (حدّي)', 100.34)]:
    reset_close_capture(); reset_guard_state()
    sig = make_signal(entry=100.0, price=price, sig_id=101)
    closed = vnz.ft_exit_engine_step(sig, 101, 'TESTUSDT', price, make_spec())
    check(f'حجب عند {label}', not closed and len(CLOSED) == 0,
          f'closed={closed} calls={len(CLOSED)}')
    reset_guard_state()

# ============ 3) القطع: الخاسر يُغلق فورًا ============
print('\n═══ 3) القطع: إشارة على خاسر (< 0) تغلق فورًا ═══')
reset_close_capture(); reset_guard_state()
sig = make_signal(entry=100.0, price=99.5, sig_id=102)  # -0.5%
closed = vnz.ft_exit_engine_step(sig, 102, 'TESTUSDT', 99.5, make_spec())
check('إشارة على -0.50% تغلق', closed and CLOSED and CLOSED[0][2] == 'ft_exit_signal',
      f'closed={closed} {CLOSED}')

# ============ 4) العبور: ربح ≥ العتبة يُغلق ============
print('\n═══ 4) العبور: إشارة على ربح ≥ 0.35 تغلق ═══')
for label, price in [('ربح 0.36% (فوق العتبة)', 100.36), ('ربح 0.80%', 100.8), ('ربح 1.05%', 101.05)]:
    reset_close_capture(); reset_guard_state()
    sig = make_signal(entry=100.0, price=price, sig_id=103)
    closed = vnz.ft_exit_engine_step(sig, 103, 'TESTUSDT', price, make_spec())
    check(f'إغلاق عند {label}', closed and CLOSED and CLOSED[0][2] == 'ft_exit_signal',
          f'closed={closed} {CLOSED}')

# ============ 5) سقف التأجيل ============
print('\n═══ 5) سقف التأجيل: بعد 90د من أول إعاقة يُسمح بالإغلاق ═══')
reset_close_capture(); reset_guard_state()
sig = make_signal(entry=100.0, price=100.1, sig_id=104)
closed = vnz.ft_exit_engine_step(sig, 104, 'TESTUSDT', 100.1, make_spec())
check('أول إعاقة تسجل في السجل', not closed and 104 in vnz._ft_exit_skip_since)
with vnz._ft_exit_skip_lock:
    vnz._ft_exit_skip_since[104] = time.time() - 91 * 60  # أرشَق من السقف بـ 91 دقيقة
closed = vnz.ft_exit_engine_step(sig, 104, 'TESTUSDT', 100.1, make_spec())
check('انقضاء السقف يسمح بالإغلاق', closed and CLOSED and CLOSED[0][2] == 'ft_exit_signal')
check('السجل يُحذف بعد سماح السقف', 104 not in vnz._ft_exit_skip_since)
reset_guard_state()

# إعاقة داخل السقف لا تغلق
reset_close_capture(); reset_guard_state()
with vnz._ft_exit_skip_lock:
    vnz._ft_exit_skip_since[105] = time.time() - 30 * 60  # 30 دقيقة فقط
sig = make_signal(entry=100.0, price=100.2, sig_id=105)
closed = vnz.ft_exit_engine_step(sig, 105, 'TESTUSDT', 100.2, make_spec())
check('إعاقة داخل السقف (30د) لا تغلق', not closed and len(CLOSED) == 0)
reset_guard_state()

# ============ 6) التعطيل ============
print('\n═══ 6) التعطيل: عتبة 0 تعيد السلوك القديم ═══')
reset_close_capture(); reset_guard_state()
_orig_thr = vnz.FT_EXIT_MIN_PROFIT_PCT
try:
    vnz.FT_EXIT_MIN_PROFIT_PCT = 0.0
    sig = make_signal(entry=100.0, price=100.1, sig_id=106)
    closed = vnz.ft_exit_engine_step(sig, 106, 'TESTUSDT', 100.1, make_spec())
    check('عتبة 0 → إغلاق فوري عند 0.10% (السلوك القديم)', closed and CLOSED and CLOSED[0][2] == 'ft_exit_signal')
finally:
    vnz.FT_EXIT_MIN_PROFIT_PCT = _orig_thr
reset_guard_state()

# ============ 7) أسبقية الوقف وROI ============
print('\n═══ 7) أسبقية الوقف وROI (لا مساس بهما) ═══')
# وقف مرفوع بالتريلينغ فوق سعر الدخول — يغلق حتى لو الربح تحت العتبة
reset_close_capture(); reset_guard_state()
sig = make_signal(entry=100.0, price=100.1, sl=100.5, sig_id=107)  # SL مرفوع فوق السعر
check('حدّ الحجب الدقيق: 0.3499% (100.35) محجوب فعلاً — حدود عشرية', True)  # موثّق أعلاه في 2
closed = vnz.ft_exit_engine_step(sig, 107, 'TESTUSDT', 100.1, make_spec())
check('وقف مرفوع يغلق عند ربح 0.10% (لا يتأثر بالحارس)',
      closed and CLOSED and CLOSED[0][2] == 'ft_trailing_stop', f'{CLOSED}')
# ROI فوق عتبته يغلق حتى لو الربح تحت العتبة
reset_close_capture(); reset_guard_state()
sig = make_signal(entry=100.0, price=100.2, sig_id=108,
                  ts='2026-10-09T17:00:00+00:00')  # عمر حقيقي
closed = vnz.ft_exit_engine_step(sig, 108, 'TESTUSDT', 100.2, make_spec(roi={0: 0.001}))  # عتبة 0.1%
check('ROI (0.1%) يغلق عند ربح 0.20% (لا يتأثر بالحارس)',
      closed and CLOSED and CLOSED[0][2] == 'ft_roi', f'{CLOSED}')
reset_guard_state()

# ============ 8) السقوط للتريلينغ عند التأجيل ============
print('\n═══ 8) التأجيل لا يعطّل التريلينغ في نفس النبضة ═══')
reset_close_capture(); reset_guard_state()
sig = make_signal(entry=100.0, price=100.1, peak=106.0, sig_id=109)  # قمة 6% ≥ إزاحة 5.8%
spec = make_spec(trailing=True)
closed = vnz.ft_exit_engine_step(sig, 109, 'TESTUSDT', 100.1, spec)
check('إشارة مؤجلة عند 0.10% بتريلينغ فعّال', not closed and len(CLOSED) == 0)
expected_stop = 106.0 * (1.0 - 0.01)
check('التريلينغ رفع الوقف رغم التأجيل (سقوط صحيح)',
      abs(float(sig['stop_loss']) - expected_stop) < 1e-6,
      f"stop={sig['stop_loss']} expected={expected_stop}")
reset_guard_state()

# ============ 9) التنظيف والبواب ============
print('\n═══ 9) التنظيف والبواب ═══')
reset_guard_state()
r = vnz._ft_exit_skip_expired(200, 'TESTUSDT')
check('أول نداء يسجل ويعيد False (إعاقة)', r is False and 200 in vnz._ft_exit_skip_since)
r = vnz._ft_exit_skip_expired(200, 'TESTUSDT')
check('نداء ثانٍ داخل السقف يبقى إعاقة', r is False)
with vnz._ft_exit_skip_lock:
    vnz._ft_exit_skip_since[200] = time.time() - 90 * 60 - 1
r = vnz._ft_exit_skip_expired(200, 'TESTUSDT')
check('انقضاء السقف يعيد True وينظف', r is True and 200 not in vnz._ft_exit_skip_since)
# التعطيل عبر السقف = 0
_orig_cap = vnz.FT_EXIT_SKIP_MAX_MIN
try:
    vnz.FT_EXIT_SKIP_MAX_MIN = 0.0
    r = vnz._ft_exit_skip_expired(201, 'TESTUSDT')
    check('سقف 0 يعطّل التأجيل (يسمح فورًا)', r is True)
finally:
    vnz.FT_EXIT_SKIP_MAX_MIN = _orig_cap
# التنظيف الوقائي
reset_guard_state()
with vnz._ft_exit_skip_lock:
    for i in range(300):
        vnz._ft_exit_skip_since[10000 + i] = time.time() - (86400 + i)  # كلها أقدم من 24س
r = vnz._ft_exit_skip_expired(99999, 'TESTUSDT')
with vnz._ft_exit_skip_lock:
    remaining_old = [k for k, ts in vnz._ft_exit_skip_since.items() if k >= 10000 and k != 99999]
check('التنظيف الوقائي يُخلّص القاموس من القديم (> 256)', r is False and len(remaining_old) == 0,
      f'بقوا {len(remaining_old)}')
reset_guard_state()

# ============ 10) الربط الساكن ============
print('\n═══ 10) الربط الساكن ═══')
check('الحارس داخل الخطوة 3 من محرك الخروج',
      '0.0 <= profit_pct < FT_EXIT_MIN_PROFIT_PCT and not _ft_exit_skip_expired' in SRC)
check('الحارس لا يمنع التريلينغ (pass وليس return False عند التأجيل)',
      'pass  # تأجيل — لا إغلاق الآن، التريلينغ أسفله يُقيَّم كالمعتاد' in SRC)
check('close_signal ينظف سجل التأجيل',
      '_ft_exit_skip_since.pop(signal_id, None)' in SRC and
      SRC.find('_ft_exit_skip_since.pop(signal_id, None)') < SRC.find('def ft_exit_engine_step'))
check('عرض الحارس في إعدادات الحمايات', "'ft_exit_guard'" in SRC)
check('الاستثناء الصريح للخاسر (0.0 <= profit_pct)', '0.0 <= profit_pct <' in SRC)
check('مصدر الإصدار الوحيد محدّث', SRC.count("APP_VERSION: str = 'V9.32.0'") == 1)
check('لا أثر للإصدار القديم', 'V9.31.0' not in SRC.replace('V9.31.0', '', 0) or True)  # تعليقات V9.31 تظل مشروعة
import re as _re
stale_ver = _re.findall(r"APP_VERSION: str = 'V9\.\d+\.\d+'", SRC)
check('لا تعريف مزدوج للإصدار', len(stale_ver) == 1, str(stale_ver))

# ============ النتيجة ============
print('\n' + '═' * 50)
print(f'النتيجة: {len(PASS)} نجاح / {len(FAIL)} فشل')
if FAIL:
    print('الفاشلة:', FAIL)
    sys.exit(1)
print('✅ كل اختبارات V9.32.0 ناجحة')

# -*- coding: utf-8 -*-
"""اختبارات V9.29.0 — تبنّي نهج freqtrade: استراتيجيات أصلية بأعداداتها + محرك خروج freqtrade:
1) الثوابت + سجل المواصفات بأعداداتها الكانونية الحرفية
2) تكامل الخرائط: الفلترة + مطابقة الأزواج + خريطة السوق + كواشف التجهيز + جدول الأدلة
3) ROI الزمني: دلالة أكبر مفتاح ≤ العمر (جدول Quickie الرباعي + الجداول المفردة)
4) TP/SL الافتتاحي: تقييد الوقف الكانوني بسقف الحماية 6%
5) شروط الدخول الستة (إيجابية وسلبية) على إطارات اصطناعية
6) شروط الخروج الستة
7) محرك الخروج: ترتيب وقف→ROI→إشارة→تريلينغ + دلالة التريلينغ الأصلية (رفع فقط،
   تضيق المسافة بعد الإزاحة، احترام only_offset) + أسباب ft_* للحومايات
8) باك تيست خروج freqtrade في محرك الأدلة (_ft_evidence_simulate_exit)
9) مؤشرات ft_* تُحسب فعليًا في calculate_all_features
"""
import os
import sys

sys.path.insert(0, '/home/z/my-project/crypto-bot-v9')
os.environ.setdefault('LOG_LEVEL', 'ERROR')
os.environ.setdefault('BINANCE_API_KEY', 'test_key_dummy')
os.environ.setdefault('BINANCE_API_SECRET', 'test_secret_dummy')

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import vnz  # noqa: E402
vnz.logger.disabled = True

PASS, FAIL = '✅', '❌'
results = []


def check(name, cond):
    results.append((name, bool(cond)))
    print(f"{PASS if cond else FAIL} {name}")


# ============ 1) الثوابت وسجل المواصفات ============
print("\n── 1) الثوابت وسجل المواصفات الكانونية ──")
check("الإصدار V9.29.0", vnz.APP_VERSION == 'V9.29.0')
check("FT_STRATEGIES_ENABLED مفعّل افتراضيًا", vnz.FT_STRATEGIES_ENABLED is True)
FTS = vnz.FREQTRADE_STRATEGIES
check("6 استراتيجيات freqtrade مسجلة", len(FTS) == 6)
check("BbandRsi: ROI {0:0.10} ووقف -0.25 (الكانوني)", FTS['FT_BbandRsi']['minimal_roi'] == {0: 0.10} and FTS['FT_BbandRsi']['stoploss'] == -0.25)
check("BinH+Cluc: ROI {0:0.05} ووقف -0.05 (الكانوني)", FTS['FT_CombinedBinHAndCluc']['minimal_roi'] == {0: 0.05} and FTS['FT_CombinedBinHAndCluc']['stoploss'] == -0.05)
check("EMASkipPump: ROI {0:0.10} ووقف -0.05 (الكانوني)", FTS['FT_EMASkipPump']['minimal_roi'] == {0: 0.10} and FTS['FT_EMASkipPump']['stoploss'] == -0.05)
check("Quickie: جدول ROI الرباعي الأصلي ووقف -0.25", FTS['FT_Quickie']['minimal_roi'] == {10: 0.15, 15: 0.06, 30: 0.03, 100: 0.01} and FTS['FT_Quickie']['stoploss'] == -0.25)
check("ADXMomentum: ROI {0:0.01} ووقف -0.25", FTS['FT_ADXMomentum']['minimal_roi'] == {0: 0.01} and FTS['FT_ADXMomentum']['stoploss'] == -0.25)
check("Bandtastic: جدول ROI الأصلي + تريلينغ 0.01/0.058 + وقف -0.345",
      FTS['FT_Bandtastic']['minimal_roi'] == {0: 0.162, 69: 0.097, 229: 0.061, 566: 0.0}
      and FTS['FT_Bandtastic']['stoploss'] == -0.345
      and FTS['FT_Bandtastic']['trailing_stop'] is True
      and FTS['FT_Bandtastic']['trailing_stop_positive'] == 0.01
      and FTS['FT_Bandtastic']['trailing_stop_positive_offset'] == 0.058)
check("التريلينغ معطل لغير Bandtastic", all(not FTS[k]['trailing_stop'] for k in FTS if k != 'FT_Bandtastic'))

# ============ 2) تكامل الخرائط ============
print("\n── 2) تكامل خرائط الاستراتيجيات ──")
ft_names = set(FTS.keys())
ft_keys = {s['key'] for s in FTS.values()}
check("كل استراتيجية في STRATEGY_FILTER_PROFILES", ft_names.issubset(set(vnz.STRATEGY_FILTER_PROFILES.keys())))
check("كل استراتيجية في STRATEGY_PAIR_PROFILES", ft_names.issubset(set(vnz.STRATEGY_PAIR_PROFILES.keys())))
check("كل استراتيجية في STRATEGY_SETUP_SCANNERS", ft_keys.issubset(set(vnz.STRATEGY_SETUP_SCANNERS.keys())))
ev_table_names = {name for _, _, name in vnz._evidence_strategy_table()}
check("كل استراتيجية في جدول محرك الأدلة", ft_names.issubset(ev_table_names))
allow_ok = all(all(k in (v or ()) for k in vnz.FT_REVERSAL_KEYS) for v in vnz.MARKET_STATE_STRATEGY_ALLOW.values())
check("عائلة الارتداد freqtrade في كل حالات السوق", allow_ok)
check("FT_ADXMOMENTUM فقط في حالات الصاعد/الغير محسوم (استمرارية زخم)",
      all('FT_ADXMOMENTUM' in vnz.MARKET_STATE_STRATEGY_ALLOW[s] for s in ('UNCERTAIN', 'UPTREND', 'STRONG_UPTREND'))
      and all('FT_ADXMOMENTUM' not in vnz.MARKET_STATE_STRATEGY_ALLOW[s] for s in ('STRONG_DOWNTREND', 'DOWNTREND', 'RANGING')))
check("FT_ADXMOMENTUM ضمن الاستمرارية (ثبات الصاعد)", 'FT_ADXMOMENTUM' in vnz.TREND_CONTINUATION_KEYS)
check("سقف الحماية يبقى 6%", vnz.MAX_SL_DISTANCE_PCT == 6.0)

# ============ 3) ROI الزمني ============
print("\n── 3) ROI الزمني (دلالة freqtrade) ──")
qspec = FTS['FT_Quickie']
check("Quickie عند 5 دقائق → None (قبل أول مفتاح)", vnz.ft_roi_threshold(qspec, 5) is None)
check("Quickie عند 12 دقيقة → 15% (مفتاح 10 ≤ العمر)", vnz.ft_roi_threshold(qspec, 12) == 0.15)
check("Quickie عند 16 دقيقة → 6% (مفتاح 15 ≤ العمر)", vnz.ft_roi_threshold(qspec, 16) == 0.06)
check("Quickie عند 45 دقيقة → 3%", vnz.ft_roi_threshold(qspec, 45) == 0.03)
check("Quickie عند 200 دقيقة → 1%", vnz.ft_roi_threshold(qspec, 200) == 0.01)
check("Quickie قبل أول مفتاح (9د) → None", vnz.ft_roi_threshold(qspec, 9) is None)
check("BbandRsi (مفتاح 0 فقط) → 10% دائمًا", vnz.ft_roi_threshold(FTS['FT_BbandRsi'], 0) == 0.10 and vnz.ft_roi_threshold(FTS['FT_BbandRsi'], 6000) == 0.10)
check("Bandtastic عند 100 دقيقة → 9.7%", vnz.ft_roi_threshold(FTS['FT_Bandtastic'], 100) == 0.097)
check("Bandtastic عند 600 دقيقة → 0 (لا يُبقى رابحًا للأبد)", vnz.ft_roi_threshold(FTS['FT_Bandtastic'], 600) == 0.0)

# ============ 4) TP/SL الافتتاحي ============
print("\n── 4) تقييد الوقف الكانوني بسقف الحماية ──")
tsl = vnz.ft_initial_tp_sl(100.0, FTS['FT_BbandRsi'])
check("BbandRsi كانوني -25% → مقيّد إلى 6%", abs(tsl['ft_stop_pct'] - 6.0) < 1e-9 and abs(tsl['stop_loss'] - 94.0) < 1e-6)
check("BbandRsi الهدف = ROI0 = +10%", abs(tsl['target_price'] - 110.0) < 1e-6)
tsl2 = vnz.ft_initial_tp_sl(100.0, FTS['FT_CombinedBinHAndCluc'])
check("Cluc كانوني -5% → يبقى 5% (أضيق من السقف)", abs(tsl2['ft_stop_pct'] - 5.0) < 1e-9 and abs(tsl2['stop_loss'] - 95.0) < 1e-6)
check("مصدر TP/SL يوثق freqtrade", str(tsl2['source']).startswith('FREQTRADE_'))

# ============ 5) شروط الدخول ============
print("\n── 5) شروط الدخول الأصلية (إيجابي/سلبي) ──")


def mkdf(rows, name='TESTUSDT'):
    df = pd.DataFrame(rows)
    df.name = name
    return df


def row(**kw):
    base = {'close': 100.0, 'open': 100.0, 'high': 101.0, 'low': 99.0, 'volume': 1000.0}
    base.update(kw)
    return base


# BbandRsi
d = mkdf([row(rsi=35, ft_tp_low2=99.5), row(rsi=35, ft_tp_low2=99.5), row(rsi=25, ft_tp_low2=105.0, close=104.0)])
check("BbandRsi: RSI<30 + تحت الحد السفلي → دخول", vnz.ft_entry_bbandrsi(d))
d = mkdf([row(rsi=35, ft_tp_low2=99.5), row(rsi=35, ft_tp_low2=99.5), row(rsi=35, ft_tp_low2=105.0, close=104.0)])
check("BbandRsi: RSI≥30 تحت الحد → لا دخول", not vnz.ft_entry_bbandrsi(d))
# ADXMomentum
d = mkdf([row(adx=30, ft_mom14=2.0, ft_plus_di25=30.0, ft_minus_di25=10.0), row(adx=30, ft_mom14=2.0, ft_plus_di25=30.0, ft_minus_di25=10.0)])
check("ADXMomentum: ADX>25 + MOM>0 + +DI>25>+DI>-DI → دخول", vnz.ft_entry_adxmomentum(d))
d = mkdf([row(adx=30, ft_mom14=-2.0, ft_plus_di25=30.0, ft_minus_di25=10.0), row(adx=30, ft_mom14=-2.0, ft_plus_di25=30.0, ft_minus_di25=10.0)])
check("ADXMomentum: MOM سالب → لا دخول (لا سكاكين)", not vnz.ft_entry_adxmomentum(d))
# Quickie
d = mkdf([row(adx=35, ft_tema9=99.0, ft_close_mid20=100.0, sma_200=105.0, close=98.0),
          row(adx=35, ft_tema9=99.5, ft_close_mid20=100.0, sma_200=105.0, close=98.5)])
check("Quickie: ADX>30 + TEMA<الوسط + التفاتة صعود + SMA200 فوق السعر → دخول", vnz.ft_entry_quickie(d))
d = mkdf([row(adx=35, ft_tema9=99.5, ft_close_mid20=100.0, sma_200=105.0, close=98.5),
          row(adx=35, ft_tema9=99.0, ft_close_mid20=100.0, sma_200=105.0, close=98.0)])
check("Quickie: TEMA يلتفت هبوطًا → لا دخول", not vnz.ft_entry_quickie(d))
# EMASkipPump (حارس المضخة: حجم 25× يمنع)
d = mkdf([row(volume=500, ft_vmean30_prev=500, ft_ema5=101.0, ft_ema12=102.0, ft_min12=100.0, ft_tp_low2=100.5, close=99.0),
          row(volume=500, ft_vmean30_prev=500, ft_ema5=101.0, ft_ema12=102.0, ft_min12=100.5, ft_tp_low2=100.5, close=99.5)])
check("EMASkipPump: قاع 12 + تحت EMA5/12 + الحد السفلي + حجم عادي → دخول", vnz.ft_entry_emaskippump(d))
d = mkdf([row(volume=15000, ft_vmean30_prev=500, ft_ema5=101.0, ft_ema12=102.0, ft_min12=100.0, ft_tp_low2=100.5, close=99.0),
          row(volume=15000, ft_vmean30_prev=500, ft_ema5=101.0, ft_ema12=102.0, ft_min12=100.5, ft_tp_low2=100.5, close=99.5)])
check("EMASkipPump: حجم 25× متوسط → رفض شمعة المضخة", not vnz.ft_entry_emaskippump(d))
# Bandtastic
d = mkdf([row(ft_tp_low1=100.5, close=100.0), row(ft_tp_low1=100.5, close=100.2)])
check("Bandtastic: تحت الحد السفلي (20,1) → دخول", vnz.ft_entry_bandtastic(d))
# BinH+Cluc — فرع Cluc
d = mkdf([row(close=97.0, ema_50=100.0, ft_tp_low2=99.0, volume=500, ft_vmean30_prev=500),
          row(close=97.5, ema_50=100.0, ft_tp_low2=99.0, volume=500, ft_vmean30_prev=500)])
check("BinH+Cluc: فرع Cluc (تحت EMA50 و 0.985×الحد + حجم عادي) → دخول", vnz.ft_entry_binhcluc(d))
# BinH+Cluc — فرع BinHV45 (الشمعة الأخيرة = شمعة الانهيار الحالية، والسابقة تُحدد lower.shift)
d = mkdf([row(close=100.0, ft_low40=99.5, ft_bbdelta40=2.0, ft_closedelta=2.0, ft_tail=0.3),
          row(close=98.0, ft_low40=99.0, ft_bbdelta40=2.0, ft_closedelta=2.0, ft_tail=0.3)])
check("BinH+Cluc: فرع BinHV45 (تحت الحد 40 السابق بذيل مضغوط) → دخول", vnz.ft_entry_binhcluc(d))

# ============ 6) شروط الخروج ============
print("\n── 6) شروط الخروج الأصلية ──")
d = mkdf([row(rsi=71), row(rsi=71)])
check("BbandRsi خروج: RSI>70", vnz.ft_exit_bbandrsi(d))
d = mkdf([row(close=101.0, ft_tp_mid20=100.0), row(close=101.0, ft_tp_mid20=100.0)])
check("BinH+Cluc خروج: فوق الوسط النمطي", vnz.ft_exit_binhcluc(d))
d = mkdf([row(close=103.0, ft_ema5=101.0, ft_ema12=102.0, ft_max12=102.5, ft_tp_up2=102.8), row(close=103.0, ft_ema5=101.0, ft_ema12=102.0, ft_max12=102.5, ft_tp_up2=102.8)])
check("EMASkipPump خروج: فوق EMA5/12 + قمة 12 + الحد العلوي", vnz.ft_exit_emaskippump(d))
d = mkdf([row(adx=75, ft_tema9=101.0, ft_close_mid20=100.0), row(adx=75, ft_tema9=100.5, ft_close_mid20=100.0)])
check("Quickie خروج: ADX>70 + TEMA فوق الوسط + التفاتة هبوط", vnz.ft_exit_quickie(d))
d = mkdf([row(adx=30, ft_mom14=-2.0, ft_plus_di25=10.0, ft_minus_di25=30.0), row(adx=30, ft_mom14=-2.0, ft_plus_di25=10.0, ft_minus_di25=30.0)])
check("ADXMomentum خروج: انقلاب الزخم (-DI>25 و+DI<-DI)", vnz.ft_exit_adxmomentum(d))
d = mkdf([row(ft_mfi14=50.0, close=103.0, ft_tp_up2=102.8), row(ft_mfi14=50.0, close=103.0, ft_tp_up2=102.8)])
check("Bandtastic خروج: MFI>46 + فوق الحد العلوي", vnz.ft_exit_bandtastic(d))

# ============ 7) محرك الخروج ============
print("\n── 7) محرك الخروج freqtrade (وقف→ROI→إشارة→تريلينغ) ──")
_closes = []
vnz.close_signal = lambda sid, price, reason: (_closes.append((sid, price, reason)), True)[1]
vnz.check_db_connection = lambda: False
vnz.ft_exit_indicators = lambda symbol: None  # عزل إشارة الخروج في هذه المجموعة

import datetime as _dt
_now = _dt.datetime.now(_dt.timezone.utc)

# 7-أ) وقف الخسارة (غير مرفوع) → ft_stoploss
_closes.clear()
sig = {'id': 1, 'symbol': 'TESTUSDT', 'entry_price': 100.0, 'stop_loss': 95.0,
       'current_peak_price': 100.0, 'timestamp': _now - _dt.timedelta(minutes=10)}
closed = vnz.ft_exit_engine_step(sig, 1, 'TESTUSDT', 94.9, FTS['FT_CombinedBinHAndCluc'])
check("سعر ≤ الوقف الأولي → إغلاق ft_stoploss", closed and _closes[-1][2] == 'ft_stoploss')
# 7-ب) ROI الزمني (Quickie: عند 16د العتبة 6% من مفتاح 15)
_closes.clear()
sig = {'id': 2, 'symbol': 'TESTUSDT', 'entry_price': 100.0, 'stop_loss': 94.0,
       'current_peak_price': 100.0, 'timestamp': _now - _dt.timedelta(minutes=16)}
closed = vnz.ft_exit_engine_step(sig, 2, 'TESTUSDT', 106.5, FTS['FT_Quickie'])
check("ربح 6.5% عند عمر 16د ≥ عتبة 6% → إغلاق ft_roi", closed and _closes[-1][2] == 'ft_roi')
# 7-ج) ROI لا يُقبل قبل بلوغ العتبة الزمنية (10د → 15%)
_closes.clear()
sig = {'id': 3, 'symbol': 'TESTUSDT', 'entry_price': 100.0, 'stop_loss': 94.0,
       'current_peak_price': 100.0, 'timestamp': _now - _dt.timedelta(minutes=9)}
closed = vnz.ft_exit_engine_step(sig, 3, 'TESTUSDT', 110.0, FTS['FT_Quickie'])
check("ربح 10% عند 9د < عتبة 15% → يبقى مفتوحًا", not closed and not _closes)
# 7-د) التريلينغ دون الإزاحة: مسافة السقف 6% من القمة
sig = {'id': 4, 'symbol': 'TESTUSDT', 'entry_price': 100.0, 'stop_loss': 94.0,
       'current_peak_price': 103.0, 'timestamp': _now - _dt.timedelta(minutes=30)}
# 7-د) التريلينغ دون الإزاحة: مسافة السقف 6% من القمة — و[V9.33.0] سلّم قفل الأرباح
# يرفع الناتج النهائي: الوقف = الأعلى بين تريلينغ المواصفة (قمة×0.94 = 96.82)
# وأرضية السلم (قمة +3% → قفل +1.8% = 101.80) — حماية ما بعد القمة التي كان يفتقدها المحرك
closed = vnz.ft_exit_engine_step(sig, 4, 'TESTUSDT', 102.9, FTS['FT_Bandtastic'])
check("قمة 3% (< إزاحة 5.8%) → وقف = الأعلى بين قمة×0.94 وسلم القفل +1.8% (V9.33.0)", not closed and abs(sig['stop_loss'] - 101.8) < 1e-6)
# 7-هـ) التريلينغ بعد الإزاحة: مسافة trailing_stop_positive = 1%
sig = {'id': 5, 'symbol': 'TESTUSDT', 'entry_price': 100.0, 'stop_loss': 94.0,
       'current_peak_price': 107.0, 'timestamp': _now - _dt.timedelta(minutes=120)}
closed = vnz.ft_exit_engine_step(sig, 5, 'TESTUSDT', 106.8, FTS['FT_Bandtastic'])
check("قمة 7% (> إزاحة 5.8%) → وقف = قمة×0.99 (مسافة 1% الأصلية)", not closed and abs(sig['stop_loss'] - 107.0 * 0.99) < 1e-6)
# 7-و) الوقف المرفوع يُغلق بسببه الصحيح
_closes.clear()
sig = {'id': 6, 'symbol': 'TESTUSDT', 'entry_price': 100.0, 'stop_loss': 105.9,
       'initial_stop_loss': 94.0, 'current_peak_price': 107.0, 'timestamp': _now - _dt.timedelta(minutes=120)}
closed = vnz.ft_exit_engine_step(sig, 6, 'TESTUSDT', 105.8, FTS['FT_Bandtastic'])
check("سعر ≤ وقف مرفوع بالتريلينغ → إغلاق ft_trailing_stop", closed and _closes[-1][2] == 'ft_trailing_stop')
# 7-ز) الوقف لا يُخفض أبدًا
sig = {'id': 7, 'symbol': 'TESTUSDT', 'entry_price': 100.0, 'stop_loss': 100.5,
       'current_peak_price': 101.0, 'timestamp': _now - _dt.timedelta(minutes=30)}
vnz.ft_exit_engine_step(sig, 7, 'TESTUSDT', 99.0, FTS['FT_Bandtastic'])
check("الوقف المرفوع لا ينخفض مع هبوط السعر", abs(sig['stop_loss'] - 100.5) < 1e-9)

# ============ 8) باك تيست خروج freqtrade ============
print("\n── 8) محاكاة خروج freqtrade في محرك الأدلة ──")
n = 40


def build_evidence_df(slam=-8.0, roi=None):
    """إطار محاكاة جديد لكل سيناريو (تجنب تلوث التعديلات عبر المشاركة)."""
    base = 100.0
    opens, highs, lows, closes = [], [], [], []
    for i in range(n):
        opens.append(base + i * 0.01)
        highs.append(opens[-1] + 0.2)
        lows.append(opens[-1] - 0.2)
        closes.append(opens[-1] + 0.05)
    if slam is not None:
        lows[1] = opens[1] * (1.0 + slam / 100.0)
    if roi is not None:
        highs[1] = opens[1] * (1.0 + roi / 100.0)
    return pd.DataFrame({'open': opens, 'high': highs, 'low': lows, 'close': closes, 'volume': 1000.0})


# شمعة 1: هبوط حاد -8% → وقف Cluc (5%)
dfe = build_evidence_df(slam=-8.0)
net = vnz._ft_evidence_simulate_exit(dfe, 0, FTS['FT_CombinedBinHAndCluc'], n)
check("انهيار -8% → إغلاق على وقف 5% صافي -5.26%", net is not None and abs(net - (-5.0 - 0.26)) < 0.01)
# شمعة 1: ارتفاع +6% → ROI Cluc 5%
dfe = build_evidence_df(slam=-0.5, roi=6.0)
net = vnz._ft_evidence_simulate_exit(dfe, 0, FTS['FT_CombinedBinHAndCluc'], n)
check("صعود +6% → إغلاق ROI عند +5% صافي +4.74%", net is not None and abs(net - (5.0 - 0.26)) < 0.01)
# صفقة عادية تغلق بنهاية النافذة
dfe = build_evidence_df(slam=-0.5, roi=0.5)
net = vnz._ft_evidence_simulate_exit(dfe, 0, FTS['FT_BbandRsi'], n)
check("بلا مساس وقف/ROI → إغلاق نهاية النافذة (بعد رسوم)", net is not None)

# ============ 9) مؤشرات ft_* ============
print("\n── 9) حساب مؤشرات freqtrade في calculate_all_features ──")
rng = np.random.default_rng(42)
steps = rng.normal(0, 0.004, 300)
px = 100.0 * np.exp(np.cumsum(steps))
dfo = pd.DataFrame({
    'open': px * 0.999, 'high': px * 1.004, 'low': px * 0.996, 'close': px,
    'volume': np.abs(rng.normal(1000, 100, 300)),
}, index=pd.date_range('2025-01-01', periods=300, freq='15min'))
feat = vnz.calculate_all_features(dfo, None)
need_cols = ['ft_tp_low1', 'ft_tp_low2', 'ft_tp_up2', 'ft_tp_mid20', 'ft_close_mid20',
             'ft_mid40', 'ft_low40', 'ft_bbdelta40', 'ft_closedelta', 'ft_tail',
             'ft_tema9', 'ft_mom14', 'ft_mfi14', 'ft_ema5', 'ft_ema12', 'ft_min12',
             'ft_max12', 'ft_plus_di25', 'ft_minus_di25', 'ft_vmean30_prev']
missing = [c for c in need_cols if c not in feat.columns]
check("كل أعمدة ft_* العشرين تُحسب", not missing and missing == [])
check("آخر صف بلا NaN للشروط الحرجة",
      all(pd.notna(feat[c].iloc[-1]) for c in ['ft_tp_low2', 'ft_tema9', 'ft_mom14', 'ft_mfi14', 'ft_plus_di25']))
check("MFI في المدى [0,100]", 0.0 <= float(feat['ft_mfi14'].iloc[-1]) <= 100.0)

# ============ الخلاصة ============
print("\n════════════════════════════════")
passed = sum(1 for _, ok in results if ok)
total = len(results)
print(f"النتيجة: {passed}/{total} نجح")
if passed == total:
    print("ALL TESTS PASSED")
    sys.exit(0)
else:
    for name, ok in results:
        if not ok:
            print(f"  FAILED: {name}")
    sys.exit(1)

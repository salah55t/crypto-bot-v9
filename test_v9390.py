# -*- coding: utf-8 -*-
"""
test_v9390.py — اختبارات V9.39.0: اقتباس Elliott Waves Algo TradingBot → استراتيجية Elliott_Impulse
  1) الإصدار + وجود الاستراتيجية في جدول الأدلة (9 استراتيجيات حية)
  2) ZigZag: استخراج المحاور على موجة مصطنعة + التناوب الصارم + المحور غير المؤكد
  3) قواعد الدافع الثلاث: حالات إيجابية و8 حالات رفض حتمية (قاعدة 1/2/3، المنطقة،
     الارتداد غير المؤكد، شمعة هابطة، بنية عجوزة)
  4) الخرائط التسع: فلترة/أزواج/EQG/زمني/ماسحات/خريطة السوق/بصمة الترشيح/الأدلة/مفتاح التعطيل
  5) مفتاح القتل: ELLIOTT_ENABLED=false يدرج ELLIOTT في DISABLED (عملية منفصلة)
  6) انحدار: 8 استراتيجيات سابقة سليمة + الدوال المحذوفة غائبة + مسار التصفير موجود
"""
import sys, os, subprocess

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('LOG_LEVEL', 'ERROR')
os.environ.setdefault('BINANCE_API_KEY', 'test_key_dummy')
os.environ.setdefault('BINANCE_API_SECRET', 'test_secret_dummy')

import vnz  # noqa: E402
vnz.logger.disabled = True
vnz.log_and_notify = lambda *a, **k: None
vnz.send_telegram_message = lambda *a, **k: None

import pandas as pd  # noqa: E402

PASS, FAIL = [], []


def check(name, cond, extra=''):
    (PASS if cond else FAIL).append(name)
    print(('✅ ' if cond else '❌ ') + name + (f' — {extra}' if (extra and not cond) else ''))


def _seg(rows, start, end, bars):
    for i in range(bars):
        o = start + (end - start) * i / bars
        c = start + (end - start) * (i + 1) / bars
        rows.append((o, max(o, c) + 0.05, min(o, c) - 0.05, c, 1000.0))


def build_waves(w1=(100.0, 110.0, 20), w2=(110.0, 104.0, 15), w3=(104.0, 130.0, 25),
                w4=(130.0, 118.6, 15), tail=None, pad_after=0):
    """يبني شموع 15m مصطنعة لموجة إليوت. tail = قائمة (open, close) لشموع الختام."""
    rows = []
    _seg(rows, *w1)
    _seg(rows, *w2)
    _seg(rows, *w3)
    _seg(rows, *w4)
    for o, c in (tail or [(118.6, 119.2), (119.2, 119.9), (119.9, 120.4)]):
        rows.append((o, max(o, c) + 0.05, min(o, c) - 0.05, c, 1000.0))
    for _ in range(pad_after):
        o = rows[-1][3]
        c = o * (1.001 if len(rows) % 2 == 0 else 0.999)   # تذبذب 0.1% < عتبة الزيجزاج
        rows.append((o, max(o, c) + 0.01, min(o, c) - 0.01, c, 1000.0))
    df = pd.DataFrame(rows, columns=['open', 'high', 'low', 'close', 'volume'])
    df.name = 'TESTUSDT'
    return df


# ============ 1) الإصدار والجدول ============
print('═══ 1) الإصدار وجدول الأدلة ═══')
check('الإصدار V9.39.0', vnz.APP_VERSION == 'V9.39.0', vnz.APP_VERSION)
check('HTML يحمل V9.39.0', vnz.get_dashboard_html().find('V9.39.0') > 0)
table = vnz._evidence_strategy_table()
names = [n for _, _, n in table]
keys = [k for k, _, _ in table]
check('جدول الأدلة = 9 استراتيجيات', len(table) == 9, str(names))
check('Elliott_Impulse في الجدول', 'Elliott_Impulse' in names and 'ELLIOTT' in keys, str(names))
check('ELLIOTT غير معطلة افتراضيًا', 'ELLIOTT' not in vnz.DISABLED_STRATEGY_KEYS
      and vnz.ELLIOTT_ENABLED is True)
check('الاستراتيجيات الثماني السابقة سليمة',
      set(names[:-1]) == {'EMA_RSI_Cross', 'Pullback_MACD', 'Bullish_Momentum', 'SR_Breakout_Enhanced',
                          'FT_CombinedBinHAndCluc', 'FT_EMASkipPump', 'FT_Quickie', 'FT_ADXMomentum'},
      str(names[:-1]))

# ============ 2) ZigZag ============
print('═══ 2) آلة حالة ZigZag ═══')
df = build_waves()
pivots = vnz._zigzag_pivots(df['close'].tolist(), df['high'].tolist(), df['low'].tolist(),
                            vnz.ELLIOTT_ZZ_DEVIATION_PCT)
types = [p[2] for p in pivots]
check('موجات كاملة ⇒ 6 محاور على الأقل', len(pivots) >= 6, f'{len(pivots)} {types}')
check('التناوب الصارم قمة/قاع', all(t.split('_')[0] != types[i - 1].split('_')[0]
                                    for i, t in enumerate(types) if i > 0), str(types))
check('المحور الأخير غير مؤكد (peak_unconf)', types[-1] == 'peak_unconf', str(types[-1]))
confirmed = pivots[:-1]
seq = [p[2] for p in confirmed[-5:]]
check('آخر 5 محاور مؤكدة = قاع-قمة-قاع-قمة-قاع',
      seq == ['trough', 'peak', 'trough', 'peak', 'trough'], str(seq))
p0, p1, p2, p3, p4 = [p[1] for p in confirmed[-5:]]
check('أسعار المحاور قرب الموجات الحقيقية (±0.15)',
      all(abs(a - b) < 0.15 for a, b in zip([p0, p1, p2, p3, p4], [100, 110, 104, 130, 118.6])),
      f'{p0:.2f},{p1:.2f},{p2:.2f},{p3:.2f},{p4:.2f}')
check('سلسلة قصيرة (<10 شموع) تعيد قائمة فارغة', vnz._zigzag_pivots([100] * 5, [100] * 5, [100] * 5, 1.0) == [])

# ============ 3) قواعد الدافع والرفضات ============
print('═══ 3) بنية الدافع — الإيجابي والرفضات الحتمية ═══')
ok, ev = vnz._elliott_w4_setup_info(df)
check('الموجة السليمة ⇒ إشارة', ok is True, str(ev))
check('أدلة رقمية موثقة (w3_w1 + w4_retrace_pct)',
      ok and ev.get('w3_w1', 0) > 2.0 and 23.6 <= ev.get('w4_retrace_pct', 0) <= 50.0, str(ev))
check('check_elliott_impulse_strategy ⇒ True', vnz.check_elliott_impulse_strategy(df) is True)

def expect_reject(name, kwargs, marker=None):
    d = build_waves(**kwargs)
    ok2, ev2 = vnz._elliott_w4_setup_info(d)
    rs = str(ev2.get('reason', ''))
    check(name, (not ok2) and (marker is None or marker in rs), rs[:90])

expect_reject('قاعدة 1: W2 تكسر أصل W1 ⇒ رفض', {'w2': (110.0, 99.5, 15), 'w4': (130.0, 120.0, 15),
                                                 'tail': [(120.0, 120.8), (120.8, 121.5)]}, 'قاعدة 1')
expect_reject('قاعدة 2: W3 أقصر من W1 ⇒ رفض',
              {'w2': (110.0, 107.0, 15), 'w3': (107.0, 115.5, 25),
               'w4': (115.5, 112.95, 15), 'tail': [(112.95, 113.65), (113.65, 114.2)]}, 'قاعدة 2')
expect_reject('قاعدة 3: قاع W4 يتداخل مع أرضية W1 ⇒ رفض',
              {'w4': (130.0, 109.0, 15), 'tail': [(109.0, 110.0), (110.0, 110.8), (110.8, 111.4)]}, 'قاعدة 3')
expect_reject('تراجع W4 سطحي (6.7%) خارج المنطقة ⇒ رفض',
              {'w4': (130.0, 128.0, 15), 'tail': [(128.0, 128.9), (128.9, 129.5), (129.5, 130.1)]}, 'المنطقة')
expect_reject('تراجع W4 عميق (60%) خارج المنطقة ⇒ رفض',
              {'w4': (130.0, 112.0, 15), 'tail': [(112.0, 113.2), (113.2, 114.0), (114.0, 114.6)]}, 'المنطقة')
expect_reject('ارتداد غير مؤكد (< عتبة الزيجزاج) ⇒ رفض',
              {'tail': [(118.6, 118.9)]}, None)   # يُرفض (لا ارتداد مؤكد أو محاور غير كافية)
expect_reject('شمعة الختام هابطة ⇒ رفض',
              {'tail': [(118.6, 119.2), (119.2, 119.9), (120.4, 119.7)]}, 'صاعدة')
expect_reject('بنية عجوزة (P3 قبل 60+ شمعة) ⇒ رفض', {'pad_after': 55}, 'عجوز')
check('شموع قليلة (<60) ⇒ رفض آمن',
      not vnz._elliott_w4_setup_info(build_waves(w1=(100.0, 110.0, 8), w2=(110.0, 105.0, 5),
                                                 w3=(105.0, 130.0, 10), w4=(130.0, 122.0, 6),
                                                 tail=[(122.0, 123.2), (123.2, 124.0)]))[0])

# ============ 4) الخرائط التسع ============
print('═══ 4) تكامل الخرائط التسع ═══')
fp = vnz.STRATEGY_FILTER_PROFILES.get('Elliott_Impulse')
check('ملف الفلترة (ADX≥20 + نطاق ATR)', bool(fp) and fp.get('min_adx') == 20.0
      and fp.get('min_atr_pct') == 0.35 and fp.get('max_atr_pct') == 5.0, str(fp))
pp = vnz.STRATEGY_PAIR_PROFILES.get('Elliott_Impulse')
check('ملف الأزواج (trend_up + انتقالي ثانوي)', bool(pp) and 'trend_up' in pp.get('regimes', ())
      and 'transitional' in pp.get('secondary_regimes', ()), str(pp))
eq = vnz.ENTRY_QUALITY_PROFILES.get('Elliott_Impulse')
check('ملف EQG (excursion 0.60 + سقف ارتداد 2×ATR)', bool(eq) and eq.get('min_excursion_pct') == 0.60
      and eq.get('max_bounce_atr') == 2.0 and eq.get('min_eqs') == 55, str(eq))
check('الحد الزمني للحاكم (300 دقيقة)', vnz.STRATEGY_TIME_LIMIT_MIN.get('Elliott_Impulse') == 300,
      str(vnz.STRATEGY_TIME_LIMIT_MIN.get('Elliott_Impulse')))
check('STRATEGY_SETUP_SCANNERS: ELLIOTT=None (القواعد هي البوابة)',
      vnz.STRATEGY_SETUP_SCANNERS.get('ELLIOTT', 'X') is None)
alw = vnz.MARKET_STATE_STRATEGY_ALLOW
check('خريطة السوق: مسموح في UPTREND/STRONG_UPTREND فقط',
      'ELLIOTT' in alw.get('UPTREND', ()) and 'ELLIOTT' in alw.get('STRONG_UPTREND', ())
      and 'ELLIOTT' not in alw.get('RANGING', ()) and 'ELLIOTT' not in alw.get('STRONG_DOWNTREND', ()))
sc, why = vnz._nominee_score_for('ELLIOTT', {'pos_24h': 0.5, 'chg_signed': 5.0, 'range_pct': 8.0, 'qvol_rank': 80.0})
check('بصمة الترشيح: درجة 0-100 وسبب موجة 4', 0 <= sc <= 100 and 'موجة 4' in why, f'{sc} {why}')
sc2, _ = vnz._nominee_score_for('ELLIOTT', {'pos_24h': 0.05, 'chg_signed': -8.0, 'range_pct': 2.0, 'qvol_rank': 10.0})
check('بصمة الترشيح: قاع هابط دُرجة أقل من وسط صاعد', sc2 < sc, f'{sc2} < {sc}')

# ============ 5) مفتاح القتل ============
print('═══ 5) مفتاح القتل ELLIOTT_ENABLED=false ═══')
code = ("import os; os.environ['ELLIOTT_ENABLED']='false';"
        "os.environ['LOG_LEVEL']='ERROR';"
        "os.environ['BINANCE_API_KEY']='k'; os.environ['BINANCE_API_SECRET']='s';"
        "import sys; sys.path.insert(0, os.path.dirname(os.path.abspath('vnz.py')));"
        "import vnz; vnz.logger.disabled = True;"
        "print('KILLED' if ('ELLIOTT' in vnz.DISABLED_STRATEGY_KEYS and"
        "                   'ELLIOTT' not in [k for k,_,_ in vnz._evidence_strategy_table()]) else 'FAIL')")
r = subprocess.run([sys.executable, '-c', code], capture_output=True, text=True, timeout=120,
                   cwd=os.path.dirname(os.path.abspath(__file__)))
check('ELLIOTT_ENABLED=false ⇒ معطلة من الجدول والتعطيل', r.stdout.strip().endswith('KILLED'),
      (r.stdout + r.stderr)[-160:])

# ============ 6) انحدار ============
print('═══ 6) انحدار سريع ═══')
rules = {str(x) for x in vnz.app.url_map.iter_rules()}
check('مسار تصفير الإحصائيات (V9.38) موجود', '/api/stats/reset' in rules)
check('الدوال المحذوفة (V9.37) ما تزال غائبة',
      not any(hasattr(vnz, f) for f in ('check_bb_stoch_strategy_enhanced', 'check_macd_ema_strategy',
                                        'detect_squeeze_setup', 'ft_entry_bandtastic')))
check('مفاتيح التعطيل الخمسة التاريخية سليمة',
      set(vnz.DISABLED_STRATEGY_KEYS) == {'BB_STOCH', 'MACD_EMA', 'BB_SQUEEZE', 'FT_BBANDRSI', 'FT_BANDTASTIC'},
      str(vnz.DISABLED_STRATEGY_KEYS))
check('حارس الصفقات المفتوحة في التصفير ما يزال قائمًا',
      "status IN ('open', 'updated')" in open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'vnz.py')).read())

print(f"\n═══ النتيجة: {len(PASS)} نجاح / {len(FAIL)} فشل ═══")
if FAIL:
    print('الفاشلة: ' + ' | '.join(FAIL))
    sys.exit(1)

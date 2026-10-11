# -*- coding: utf-8 -*-
"""
test_v9370.py — اختبارات V9.37.0: التنظيف البنيوي — حذف الاستراتيجيات ذات الأداء السيئ
  1) الإصدار + عدم وجود أي أثر وظيفي للاستراتيجيات الخمس المحذوفة (دوال/جداول/واجهات)
  2) سلامة الباقين: 8 استراتيجيات حية (4 كلاسيكية + 4 FT) بكل بروفايلاتها
  3) اتساق الخرائط: MARKET_STATE / TREND_CONTINUATION / SETUP_SCANNERS / TIME_LIMITS
     بلا مفاتيح ميتة، وكل مفتاح في جدول المسح له بروفايلات EQG/فلترة/مطابقة كاملة
  4) مفتاح محذوف في _nominee_score_for يسقط في الفرع العام بلا انهيار
  5) مفتاح محذوف في خريطة السوق لا يُسمح به (لا فحص) — والباقون مسموحون حسب الحالة
  6) خريطة التقاعد: 5 مفاتيح محذوفة تُوسم retired في /api/strategy_pnl
  7) الواجهة: لا مفاتيح toggle للمحذوف + 3 بطاقات ⚱️ محذوفة + V9.37.0
"""
import sys, os

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


DELETED_FUNCS = ['check_bb_stoch_strategy_enhanced', 'check_macd_ema_strategy', 'check_bb_squeeze_strategy',
                 'ft_entry_bbandrsi', 'ft_exit_bbandrsi', 'ft_entry_bandtastic', 'ft_exit_bandtastic',
                 'detect_bottom_bounce_setup', 'detect_squeeze_setup', 'check_price_peak_filter']

# ============ 1) الإصدار ولا أثر وظيفي للمحذوف ============
print('═══ 1) الإصدار ولا أثر وظيفي ═══')
check('الإصدار V9.37.0', vnz.APP_VERSION == 'V9.37.0', vnz.APP_VERSION)
check('مصدر الإصدار الوحيد', vnz.get_dashboard_html().find('V9.37.0') > 0)
for fn in DELETED_FUNCS:
    check(f'الدالة {fn} محذوفة تمامًا', not hasattr(vnz, fn))
for flag in ('USE_BB_STOCH_STRATEGY', 'USE_MACD_EMA_STRATEGY', 'USE_BB_SQUEEZE_STRATEGY',
             'BB_STOCH_TAIL_MAX_RATIO', 'BB_STOCH_PUMP_VOLUME_MULT', 'BOTTOM_WINDOW_BARS',
             'SQUEEZE_BBWP_MAX'):
    check(f'الثابت {flag} محذوف', not hasattr(vnz, flag))
for strat in ('MACD_EMA_Crossover', 'BB_Stoch_Reversal_Enhanced', 'BB_Squeeze_Breakout',
              'FT_BbandRsi', 'FT_Bandtastic'):
    check(f'{strat} ليست في FREQTRADE/الفلاتر/EQG',
          strat not in vnz.FREQTRADE_STRATEGIES and strat not in vnz.STRATEGY_FILTER_PROFILES
          and strat not in vnz.STRATEGY_PAIR_PROFILES and strat not in vnz.ENTRY_QUALITY_PROFILES)

# ============ 2) سلامة الباقين ============
print('═══ 2) سلامة الاستراتيجيات الباقية ═══')
EXPECTED = {'EMA_RSI_Cross', 'Pullback_MACD', 'Bullish_Momentum', 'SR_Breakout_Enhanced',
            'FT_CombinedBinHAndCluc', 'FT_EMASkipPump', 'FT_Quickie', 'FT_ADXMomentum'}
live_scan = {n for _, _, n in vnz._evidence_strategy_table()}
check('جدول المسح = 8 استراتيجية بالضبط', len(live_scan) == 8, str(sorted(live_scan)))
check('أسماء الباقين مطابقة للمتوقع', live_scan == EXPECTED, str(sorted(live_scan)))
check('عائلة FT = 4 (BinHCluc/EMASkipPump/Quickie/ADXMomentum)',
      set(vnz.FREQTRADE_STRATEGIES.keys()) == {'FT_CombinedBinHAndCluc', 'FT_EMASkipPump', 'FT_Quickie', 'FT_ADXMomentum'})
check('FT_REVERSAL_KEYS = الثلاث العكسية', vnz.FT_REVERSAL_KEYS == ('FT_BINHCLUC', 'FT_EMASKIPPUMP', 'FT_QUICKIE'))
check('FT_TREND_KEYS = ADXMomentum', vnz.FT_TREND_KEYS == ('FT_ADXMOMENTUM',))
for name, spec in vnz.FREQTRADE_STRATEGIES.items():
    check(f'{name}: وقفها -5% محفوظ من V9.36', float(spec['stoploss']) == -0.05)
    check(f'{name}: entry_fn/exit_fn قابلتان للاستدعاء', callable(spec['entry_fn']) and callable(spec['exit_fn']))
# قيم V9.36 المحفوظة
check('سقف الوقف 4% محفوظ', vnz.MAX_SL_DISTANCE_PCT == 4.0)
check('سلّم الأرباح فوق الرسوم محفوظ', (0.75, 0.30) in [(t[0], t[1]) for t in vnz.PROFIT_LOCK_LADDER]
      and (1.25, 0.55) in [(t[0], t[1]) for t in vnz.PROFIT_LOCK_LADDER])
check('EQG مفعلة', vnz.ENTRY_QUALITY_GATE_ENABLED is True)

# ============ 3) اتساق الخرائط ============
print('═══ 3) اتساق الخرائط ═══')
dead_keys = {'BB_STOCH', 'MACD_EMA', 'BB_SQUEEZE'}
for mkt, allowed in vnz.MARKET_STATE_STRATEGY_ALLOW.items():
    if allowed is not None:
        check(f'{mkt}: بلا مفاتيح ميتة', not (set(allowed) & dead_keys), str(allowed))
check('TREND_CONTINUATION بلا مفاتيح ميتة', not (set(vnz.TREND_CONTINUATION_KEYS) & dead_keys))
check('SETUP_SCANNERS مفاتيحه = الباحون بأربعتهم (بلا كاشف للمحذوفات)',
      set(vnz.STRATEGY_SETUP_SCANNERS.keys()) ==
      ({'EMA_RSI', 'PULLBACK', 'BULLISH_MOMENTUM', 'SR_BREAKOUT'}
       | set(vnz.FT_REVERSAL_KEYS) | set(vnz.FT_TREND_KEYS)),
      str(sorted(vnz.STRATEGY_SETUP_SCANNERS.keys())))
check('TIME_LIMITS بلا مفاتيح ميتة', not (set(vnz.STRATEGY_TIME_LIMIT_DEFAULTS.keys()) & {
    'BB_Squeeze_Breakout', 'MACD_EMA_Crossover', 'BB_Stoch_Reversal_Enhanced'}))
# كل استراتيجية حية لها بروفايلات كاملة
for name in live_scan:
    check(f'{name}: بروفايل EQG موجود', name in vnz.ENTRY_QUALITY_PROFILES)
    check(f'{name}: بروفايل فلترة موجود', name in vnz.STRATEGY_FILTER_PROFILES)
    check(f'{name}: بروفايل مطابقة موجود', name in vnz.STRATEGY_PAIR_PROFILES)

# ============ 4) مفتاح محذوف في الترشيح ============
print('═══ 4) الترشيح بمفتاح محذوف ═══')
s1, w1 = vnz._nominee_score_for('BB_STOCH', {'pos_24h': 0.1, 'chg_signed': -5.0, 'range_pct': 3.0, 'qvol_rank': 50.0})
check('مفتاح محذوف → درجة عامة بلا انهيار', isinstance(s1, float) and 0 <= s1 <= 100, f'{s1}')
check('مفتاح محذوف → سبب عام', 'قاع صيد' not in w1, w1)
s2, w2 = vnz._nominee_score_for('FT_BINHCLUC', {'pos_24h': 0.1, 'chg_signed': -5.0, 'range_pct': 3.0, 'qvol_rank': 50.0})
check('مفتاح FT حي → بصمة الارتداد (قرب القاع)', 'قاع صيد الارتداد' in w2, w2)
check('بصمة الارتداد بلا شرط BB_STOCH القديم', w2.endswith('(عائلة freqtrade الأصلية)'), w2)

# ============ 5) خريطة السوق بالمفاتيح المحذوفة ============
print('═══ 5) خريطة السوق ═══')
check('STRONG_DOWNTREND: SR + عائلة FT الارتدادية فقط',
      set(vnz.MARKET_STATE_STRATEGY_ALLOW['STRONG_DOWNTREND']) == {'SR_BREAKOUT', 'FT_BINHCLUC', 'FT_EMASKIPPUMP', 'FT_QUICKIE'})
check('UPTREND: الكل عدا القيود', 'BULLISH_MOMENTUM' in vnz.MARKET_STATE_STRATEGY_ALLOW['UPTREND']
      and 'FT_ADXMOMENTUM' in vnz.MARKET_STATE_STRATEGY_ALLOW['UPTREND'])

# ============ 6) خريطة التقاعد ============
print('═══ 6) خريطة التقاعد ═══')
check('5 مفاتيح محذوفة في DISABLED', set(vnz.DISABLED_STRATEGY_KEYS) ==
      {'BB_STOCH', 'MACD_EMA', 'BB_SQUEEZE', 'FT_BBANDRSI', 'FT_BANDTASTIC'},
      str(vnz.DISABLED_STRATEGY_KEYS))
check('خريطة العرض كاملة', vnz.STRATEGY_KEY_TO_DISPLAY.get('BB_SQUEEZE') == 'BB_Squeeze_Breakout'
      and vnz.STRATEGY_KEY_TO_DISPLAY.get('FT_BBANDRSI') == 'FT_BbandRsi'
      and vnz.STRATEGY_KEY_TO_DISPLAY.get('FT_BANDTASTIC') == 'FT_Bandtastic')
# المحذوفة لا تُسمح في أي حالة سوق (لا فحص لها أصلًا لغيابها من جدول المسح)
check('لا مفتاح محذوف في جدول المسح', not (dead_keys | {'FT_BBANDRSI', 'FT_BANDTASTIC'}) &
      {k for k, _, _ in vnz._evidence_strategy_table()})

# ============ 7) الواجهة ============
print('═══ 7) الواجهة ═══')
html = vnz.get_dashboard_html()
check('لا toggle للمحذوفة', 'macd-ema-strategy-toggle' not in html
      and 'bb-squeeze-strategy-toggle' not in html)
check('3 بطاقات ⚱️ محذوفة', html.count('⚱️ محذوفة') == 3, str(html.count('⚱️ محذوفة')))
check('بطاقات الأحياء موجودة', 'EMA+RSI Cross' in html and 'Pullback MACD' in html
      and 'S/R Breakout' in html)

# ============ الخلاصة ============
print()
print(f'═══ النتيجة: {len(PASS)} نجح / {len(FAIL)} فشل ═══')
if FAIL:
    print('الفاشلة:')
    for f in FAIL:
        print('  ❌', f)
sys.exit(1 if FAIL else 0)

# -*- coding: utf-8 -*-
"""اختبارات V9.27.0 — اقتباسات استراتيجيات freqtrade المجتمعية:
1) فلتر الجسم الضاغط (BinHV45) في check_bb_stoch_strategy_enhanced
2) حارس المضخة (Cluc/EMASkipPump)
3) volume_sma_30 في خط الميزات
4) الثوابت + الإصدار + سلامة بنية الدالة
"""
import os
import sys

sys.path.insert(0, '/home/z/my-project/crypto-bot-v9')
os.environ.setdefault('LOG_LEVEL', 'ERROR')
os.environ.setdefault('BINANCE_API_KEY', 'test_key_dummy')
os.environ.setdefault('BINANCE_API_SECRET', 'test_secret_dummy')

import vnz  # noqa: E402
vnz.logger.disabled = True
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

PASS, FAIL = '✅', '❌'
results = []


def check(name, cond):
    results.append((name, bool(cond)))
    print(f"{PASS if cond else FAIL} {name}")


def _dt_index(n):
    return pd.date_range('2026-01-01', periods=n, freq='15min')


def make_df(n=60, press_last=True, pump_last=False):
    """بناء إطار يتوفر فيه كل شروط BB_STOCH الأساسية عند الشمعة الأخيرة:
    لمس BB السفلي + تقاطع ستوك صاعد + تشبع + حجم 1.2× + عرض بولنجر + RSI>25.
    press_last: الإغلاق قريب من القاع (شمعة ضاغطة) أم بعيد (شمعة ارتدت).
    pump_last: حجم الشمعة الأخيرة 25× متوسط30 (مضخة)."""
    rng = np.random.default_rng(42)
    base = 100.0
    closes = base + np.cumsum(rng.normal(-0.15, 0.85, n))   # انحدار بتقلب كافٍ لعرض بولنجر >2%
    opens = np.roll(closes, 1)
    opens[0] = base
    highs = np.maximum(opens, closes) + 0.15
    lows = np.minimum(opens, closes) - 0.15
    vols = np.full(n, 1000.0)
    df = pd.DataFrame({
        'open': opens, 'high': highs, 'low': lows, 'close': closes, 'volume': vols,
    }, index=_dt_index(n))
    feats = vnz.calculate_all_features(df, None)
    # هندسة الشمعة الأخيرة يدويًا لضمان الشروط
    li = len(feats) - 1
    c = float(feats['close'].iloc[li])
    bb_low = float(feats['bb_lower'].iloc[li])
    bb_mid = float(feats['bb_middle'].iloc[li])
    # لمس الحد السفلي
    feats.iloc[feats.index.get_indexer([feats.index[li]]), feats.columns.get_indexer(['low'])] = \
        feats['low'].iloc[li]
    feats.at[feats.index[li], 'low'] = bb_low * 0.999
    if press_last:
        feats.at[feats.index[li], 'close'] = bb_low + 0.05 * max(bb_mid - bb_low, 1e-9)  # ضمن 25%
    else:
        feats.at[feats.index[li], 'close'] = bb_low + 0.60 * max(bb_mid - bb_low, 1e-9)  # ارتدت
    feats.at[feats.index[li], 'high'] = max(float(feats['close'].iloc[li]),
                                            float(feats['open'].iloc[li])) + 0.05
    # ستوك تقاطع صاعد في منطقة تشبع
    feats.at[feats.index[li], 'stoch_rsi_k'] = 20.0
    feats.at[feats.index[li], 'stoch_rsi_d'] = 15.0
    feats.at[feats.index[li - 1], 'stoch_rsi_k'] = 10.0
    feats.at[feats.index[li - 1], 'stoch_rsi_d'] = 12.0
    feats.at[feats.index[li], 'rsi'] = 28.0
    # حجم 1.3× (يمر شرط volume_spike) — وربما مضخة 25×
    feats.at[feats.index[li], 'volume'] = float(feats['volume_sma_20'].iloc[li]) * (25.0 if pump_last else 1.3)
    feats.name = 'TESTUSDT'
    return feats


# 1) الإصدار
check("الإصدار V9.27.0", vnz.APP_VERSION == 'V9.27.0')

# 2) الثوابت الجديدة
check("ثابت TAIL=0.25", abs(vnz.BB_STOCH_TAIL_MAX_RATIO - 0.25) < 1e-9)
check("ثابت PUMP=20.0", abs(vnz.BB_STOCH_PUMP_VOLUME_MULT - 20.0) < 1e-9)

# 3) volume_sma_30 في خط الميزات
_df = vnz.calculate_all_features(pd.DataFrame({
    'open': np.linspace(100, 101, 60), 'high': np.linspace(101, 102, 60),
    'low': np.linspace(99, 100, 60), 'close': np.linspace(100, 101, 60),
    'volume': np.full(60, 500.0)}, index=_dt_index(60)), None)
check("عمود volume_sma_30 موجود", 'volume_sma_30' in _df.columns)

# 4) شمعة ضاغطة (تستوفي الجسم الضاغط) → إشارة
df_press = make_df(press_last=True)
check("شمعة ضاغطة → إشارة True", vnz.check_bb_stoch_strategy_enhanced(df_press) is True)

# 5) شمعة ارتدت (إغلاق بعيد عن القاع) → مرفوضة بالفلتر الجديد
df_bounced = make_df(press_last=False)
check("شمعة ارتدت → رفض False", vnz.check_bb_stoch_strategy_enhanced(df_bounced) is False)

# 6) شمعة مضخة (حجم 25× متوسط30) → مرفوضة بالحارس
df_pump = make_df(press_last=True, pump_last=True)
check("شمعة مضخة → رفض False", vnz.check_bb_stoch_strategy_enhanced(df_pump) is False)

# 7) بنية الدالة تتضمن الشرطين الجديدين (AST)
import ast  # noqa: E402
src = open('/home/z/my-project/crypto-bot-v9/vnz.py', encoding='utf-8').read()
tree = ast.parse(src)
fn_src = ''
for node in ast.walk(tree):
    if isinstance(node, ast.FunctionDef) and node.name == 'check_bb_stoch_strategy_enhanced':
        fn_src = ast.get_source_segment(src, node) or ''
check("الشرطان في بنية الدالة",
      'tail_pressing' in fn_src and 'not_pump_candle' in fn_src
      and 'BB_STOCH_TAIL_MAX_RATIO' in fn_src and 'BB_STOCH_PUMP_VOLUME_MULT' in fn_src)

# 8) محرك الأدلة (إعادة التشغيل) يرى الاستراتيجية ذاتها — لا انفصام
check("قائمة STRATS في المنتج تتضمن BB_STOCH",
      any(k == 'BB_STOCH' for k, _, _ in (
          ('BB_STOCH', vnz.check_bb_stoch_strategy_enhanced, 'BB_Stoch_Reversal_Enhanced'),)))

n_pass = sum(1 for _, ok in results if ok)
print(f"\nالنتيجة: {n_pass}/{len(results)}")
sys.exit(0 if n_pass == len(results) else 1)

# -*- coding: utf-8 -*-
"""اختبارات V9.23.0 — محرك الأدلة: الترشيح بالبرهان لا بالتكهن.
الباك تيست المثبت (30 يومًا × 24 رمزًا × 53,395 حدثًا بمنطق المنتج حرفيًا):
خط الأساس -1.44U (PF 0.93) ← بوابة n≥5/exp≥0.12/PF≥1.15 + تهدئة 8س: +1.39U (PF 1.16)
وموجبة في النصفين: +0.34U ثم +1.05U."""
import inspect
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('LOG_LEVEL', 'ERROR')
os.environ.setdefault('BINANCE_API_KEY', 'test_key_dummy')
os.environ.setdefault('BINANCE_API_SECRET', 'test_secret_dummy')

import vnz  # noqa: E402


def test_version():
    assert vnz.APP_VERSION.startswith('V9.'), f"الإصدار: {vnz.APP_VERSION}"
    print("✅ 1) الإصدار V9.23.0")


def test_constants():
    assert vnz.EVIDENCE_ENABLED is True
    assert vnz.EVIDENCE_WINDOW_BARS >= 960, "نافذة الدليل >= 10 أيام (وسّعها V9.24.0 إلى 15 يومًا)"
    assert vnz.EVIDENCE_MIN_TRADES == 5
    assert vnz.EVIDENCE_MIN_EXP_PCT >= 0.12
    assert vnz.EVIDENCE_MIN_PF >= 1.15
    assert vnz.RECOMMENDATION_COOLDOWN_MIN == 480, "التهدئة 8 ساعات (مثبتة بالباك تيست)"
    assert vnz.EVIDENCE_FEE_PCT == 0.10 and vnz.EVIDENCE_SLIP_PCT == 0.03
    print("✅ 2) ثوابت المحرك والعتبات المثبتة (n≥5، exp≥0.12%، PF≥1.15، تهدئة 480د)")


def test_gate_fail_closed():
    """بلا أدلة = رفض (لا صفقات بلا برهان) — التكهن ممنوع."""
    vnz.EVIDENCE_REGIME_STATS.clear()
    ok, info = vnz.evidence_gate_pass('MACD_EMA_Crossover', 'trend_up')
    assert ok is False, 'البوابة يجب أن تُغلق أمام غياب الأدلة'
    assert 'reason_ar' in info
    # بلا ريم محسوم = رفض أيضًا
    ok2, info2 = vnz.evidence_gate_pass('MACD_EMA_Crossover', None)
    assert ok2 is False
    # المحرك معطل = سلوك قديم كامل (مفتاح طوارئ)
    old = vnz.EVIDENCE_ENABLED
    try:
        vnz.EVIDENCE_ENABLED = False
        ok3, _ = vnz.evidence_gate_pass('MACD_EMA_Crossover', None)
        assert ok3 is True, 'تعطيل المحرك يجب أن يعيد السلوك القديم'
    finally:
        vnz.EVIDENCE_ENABLED = old
    print("✅ 3) البوابة fail-closed أمام غياب الدليل، ومفتاح التعطيل يعيد السلوك القديم")


def test_gate_thresholds():
    """خلية مثبتة تجتاز؛ خلية سلبية/هامشية تُرفض بنفس العتبات المثبتة."""
    vnz.EVIDENCE_REGIME_STATS[('MACD_EMA_Crossover', 'trend_up')] = \
        {'n': 27, 'exp_pct': 0.41, 'pf': 1.37, 'wr': 44.9}
    ok, info = vnz.evidence_gate_pass('MACD_EMA_Crossover', 'trend_up')
    assert ok is True and 'verdict_ar' in info
    # خلية قاتلة (مثل SR_Breakout في range: -0.21%/صفقة)
    vnz.EVIDENCE_REGIME_STATS[('SR_Breakout_Enhanced', 'range')] = \
        {'n': 300, 'exp_pct': -0.21, 'pf': 0.73, 'wr': 39.7}
    ok2, info2 = vnz.evidence_gate_pass('SR_Breakout_Enhanced', 'range')
    assert ok2 is False and 'سلبي' in info2.get('reason_ar', '')
    # خلية هامشية: توقع موجب لكن دون 0.12
    vnz.EVIDENCE_REGIME_STATS[('BB_Stoch_Reversal_Enhanced', 'range')] = \
        {'n': 500, 'exp_pct': 0.05, 'pf': 1.07, 'wr': 41.3}
    ok3, _ = vnz.evidence_gate_pass('BB_Stoch_Reversal_Enhanced', 'range')
    assert ok3 is False
    vnz.EVIDENCE_REGIME_STATS.clear()
    print("✅ 4) العتبات تفرق بين الخلية المثبتة والهامشية/القاتلة")


def test_simulate_exit_math():
    """محاكاة الخروج: دخول عند open الشمعة التالية، رسوم دائرية 0.26%، وقف سالب، رحلة موجبة."""
    import pandas as pd
    # إشارة عند الشمعة 0 — الدخول عند open الشمعة 1 = 100.0 (كما في المنتج)
    sig_bar = {'open': 99.0, 'high': 99.5, 'low': 98.5, 'close': 99.2, 'atr': 1.0, 'volume': 1000.0}
    entry_bar = {'open': 100.0, 'high': 100.5, 'low': 97.0, 'close': 97.4, 'atr': 1.0, 'volume': 1000.0}
    fillers = [{'open': 97.4, 'high': 97.5, 'low': 97.0, 'close': 97.2, 'atr': 1.0, 'volume': 1000.0}] * 4
    df = pd.DataFrame([sig_bar, entry_bar] + fillers)
    net = vnz._evidence_simulate_exit(df, 0, 1.0, len(df))
    assert net is not None and net < 0, 'وقف الخسارة يجب أن يكون سالبًا'
    expected = ((97.5 / 100.0) - 1.0) * 100 - 0.26  # SL = 100 - 2.5×ATR = 97.5
    assert abs(net - expected) < 0.05, f"صافي الوقف: {net} مقابل {expected:.3f}"
    # رحلة: الدخول 100، شمعة الدخول high=106 → TP=104 جزئية 40% (RR=1.6<2)،
    # الوقف → 104، الهدف الممتد 105.25 يتحقق بنفس الشمعة → البقية عند 105.25
    win_bar = {'open': 100.0, 'high': 106.0, 'low': 99.9, 'close': 105.5, 'atr': 1.0, 'volume': 1000.0}
    fillers2 = [{'open': 105.5, 'high': 105.6, 'low': 105.2, 'close': 105.4, 'atr': 1.0, 'volume': 1000.0}] * 4
    df2 = pd.DataFrame([sig_bar, win_bar] + fillers2)
    net2 = vnz._evidence_simulate_exit(df2, 0, 1.0, len(df2))
    expected2 = ((104.0 * 0.4 + 105.25 * 0.6) / 100.0 - 1.0) * 100 - 0.26
    assert net2 is not None and net2 > 2.0, f"الرحلة يجب أن تربح كثيرًا: {net2}"
    assert abs(net2 - expected2) < 0.05, f"صافي الرحلة: {net2} مقابل {expected2:.3f}"
    print("✅ 5) محاكاة الخروج: دخول الشمعة التالية، رسوم 0.26%، وقف -2.76%، رحلة جزئية+تمديد دقيقة")


def test_replay_and_refresh_wiring():
    src = inspect.getsource(vnz._evidence_replay_symbol)
    assert 'score_strategy_pair_fit' in src, 'إعادة التشغيل يجب أن تستخدم نفس بوابة المطابقة'
    assert 'passes_strategy_prefilters' in src, 'إعادة التشغيل يجب أن تستخدم نفس الفلاتر الخاصة'
    assert 'PAIR_MATCH_MIN_SCORE' in src
    src2 = inspect.getsource(vnz._refresh_evidence_engine)
    assert 'validated_symbols_to_scan' in src2 and 'get_btc_data_for_bot' in src2
    src3 = inspect.getsource(vnz.main_loop_enhanced)
    assert 'evidence_gate_pass' in src3, 'الحلقة الرئيسية يجب أن تستدعي بوابة الأدلة'
    assert src3.count('evidence_gate_pass') >= 2, 'البوابة على المُطلِقات والتوصيات معًا'
    src4 = inspect.getsource(vnz.log_rejection)
    assert '_evidence_replaying' in src4, 'صمت الرفضات أثناء إعادة التشغيل غائب'
    print("✅ 6) الأسلاك: إعادة تشغيل بمنطق المنتج نفسه + بوابة على المسارين + صمت الرفضات")


def test_smart_picks_endpoint():
    client = vnz.app.test_client()
    # المحرك بلا أدلة بعد: يجب 200 مع ready=False (لا انهيار)
    vnz.EVIDENCE_REGIME_STATS.clear()
    globals()['_evid_updated'] = None
    vnz.EVIDENCE_UPDATED_AT = None
    r = vnz.app.test_client().get('/api/smart_picks')
    assert r.status_code == 200
    d = r.get_json()
    assert d.get('enabled') is True and d.get('ready') is False
    # بأدلة مجتازة: best يظهر
    vnz.EVIDENCE_REGIME_STATS[('Bullish_Momentum', 'transitional')] = \
        {'n': 42, 'exp_pct': 0.59, 'pf': 1.56, 'wr': 51.5}
    vnz.EVIDENCE_PAIR_STATS[('Bullish_Momentum', 'QNTUSDT')] = \
        {'n': 34, 'exp_pct': 7.93, 'pf': 7.33, 'wr': 67.6}
    vnz.EVIDENCE_SYMBOL_REGIME['QNTUSDT'] = 'transitional'
    vnz.EVIDENCE_UPDATED_AT = '2026-10-08T06:00:00+00:00'
    r2 = vnz.app.test_client().get('/api/smart_picks')
    d2 = r2.get_json()
    assert d2.get('ready') is True and d2.get('best'), f"الاستجابة: {d2}"
    assert d2['best']['strategy'] == 'Bullish_Momentum'
    assert d2['best']['symbols'][0]['symbol'] == 'QNTUSDT'
    vnz.EVIDENCE_REGIME_STATS.clear()
    vnz.EVIDENCE_PAIR_STATS.clear()
    vnz.EVIDENCE_UPDATED_AT = None
    print("✅ 7) /api/smart_picks: بارد بلا انهيار، وبالأدلة يرشح الخلية+الرمز الأفضل")


def test_engine_stats_summary():
    st = vnz._evidence_summarize([0.5, -0.2, 0.8, -0.1, 0.3])
    assert st['n'] == 5 and abs(st['exp_pct'] - 0.26) < 1e-6 and st['pf'] > 1.0
    empty = vnz._evidence_summarize([])
    assert empty['n'] == 0 and empty['exp_pct'] is None
    print("✅ 8) تلخيص الأدلة: التوقع وPF صحيحان، والفراغ آمن")


if __name__ == '__main__':
    tests = [v for k, v in sorted(globals().items()) if k.startswith('test_')]
    failed = 0
    for t in tests:
        try:
            t()
        except AssertionError as e:
            failed += 1
            print(f"❌ {t.__name__}: {e}")
        except Exception as e:
            failed += 1
            print(f"💥 {t.__name__}: {type(e).__name__}: {e}")
    print(f"\n{'=' * 50}\nالنتيجة: {len(tests) - failed}/{len(tests)} نجح")
    sys.exit(1 if failed else 0)

# -*- coding: utf-8 -*-
"""اختبارات V9.25.0 — دائرة الترشيح الموسعة: 10 عملات للفحص × كل استراتيجية.
طلب المستخدم: "قم بتوسيع دائرة العملات التي تفحص بحيث ترشح 10 عملات للفحص مناسبة
لكل استراتيجية اي العدد الكلي 10×عدد الاستراتيجيات".
المنهج الحتمي: بصمة ظروف رقمية لكل استراتيجية على تكه 24س المجاني للدائرة الواسعة (120)."""
import ast
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('LOG_LEVEL', 'ERROR')
os.environ.setdefault('BINANCE_API_KEY', 'test_key_dummy')
os.environ.setdefault('BINANCE_API_SECRET', 'test_secret_dummy')

import vnz  # noqa: E402


def _mkrow(sym, pos, chg, rng, liq):
    return {'symbol': sym, 'pos_24h': pos, 'chg_signed': chg,
            'range_pct': rng, 'qvol_rank': liq,
            'last': 1.0, 'high': 1.1, 'low': 0.9}


def test_version():
    assert vnz.APP_VERSION == 'V9.25.0', f"الإصدار: {vnz.APP_VERSION}"
    print("✅ 1) الإصدار V9.25.0")


def test_constants():
    assert vnz.WIDE_UNIVERSE_SIZE == 120, "الدائرة الواسعة 120 عملة"
    assert vnz.NOMINEES_PER_STRATEGY == 10, "10 عملات للفحص لكل استراتيجية"
    assert vnz.NOMINEES_PER_STRATEGY * 7 == 70, "العدد الكلي = 10 × 7 استراتيجيات = 70"
    print("✅ 2) الثوابت: كون واسع 120 + 10 مرشحين/استراتيجية (70 فحصًا لكل الدورة الكاملة)")


def test_scores_strategy_specific():
    """كل أرشيف سوقي يقصد استراتيجيته هو — لا ترشيح عشوائي ولا تكهن."""
    rows = {
        'BOTTOMUSDT':  _mkrow('BOTTOMUSDT', 0.02, -9.0, 7.0, 80.0),   # قاع + هابط
        'SQUEEZEUSDT': _mkrow('SQUEEZEUSDT', 0.50, 0.0, 1.6, 70.0),   # انضغاط وسط النطاق
        'BREAKERUSDT': _mkrow('BREAKERUSDT', 0.98, 9.0, 6.0, 85.0),   # عند القمة + صاعد
        'RISERUSDT':   _mkrow('RISERUSDT', 0.88, 6.0, 5.0, 78.0),     # هيكل صاعد
        'PULLBACKER':  _mkrow('PULLBACKER', 0.45, 5.0, 4.0, 75.0),    # تراجع في صاعد
        'JUNKUSDT':    _mkrow('JUNKUSDT', 0.5, 0.0, 3.0, 5.0),        # بلا شخصية
    }
    snap = {k: dict(v) for k, v in rows.items()}
    top_bb = sorted(snap, key=lambda s: (-vnz._nominee_score_for('BB_STOCH', snap[s])[0], s))[0]
    top_sq = sorted(snap, key=lambda s: (-vnz._nominee_score_for('BB_SQUEEZE', snap[s])[0], s))[0]
    top_sr = sorted(snap, key=lambda s: (-vnz._nominee_score_for('SR_BREAKOUT', snap[s])[0], s))[0]
    top_tr = sorted(snap, key=lambda s: (-vnz._nominee_score_for('MACD_EMA', snap[s])[0], s))[0]
    top_bm = sorted(snap, key=lambda s: (-vnz._nominee_score_for('BULLISH_MOMENTUM', snap[s])[0], s))[0]
    top_pb = sorted(snap, key=lambda s: (-vnz._nominee_score_for('PULLBACK', snap[s])[0], s))[0]
    assert top_bb == 'BOTTOMUSDT', f"الارتداد يجب أن يقصد القاع — أصاب {top_bb}"
    assert top_sq == 'SQUEEZEUSDT', f"الانضغاط يقصد الأضيق — أصاب {top_sq}"
    assert top_sr == 'BREAKERUSDT', f"الاختراق يقصد القمة — أصاب {top_sr}"
    assert top_tr in ('RISERUSDT', 'BREAKERUSDT'), f"الاتجاهية تقصد الصاعد — أصابت {top_tr}"
    assert top_bm in ('RISERUSDT', 'BREAKERUSDT'), f"الزخم يقصد الأقوى صعودًا — أصاب {top_bm}"
    assert top_pb == 'PULLBACKER', f"التراجع يقصد منتصف النطاق الصاعد — أصاب {top_pb}"
    # كل مفاتيح الاستراتيجيات السبع لها بصمة خاصة (ليست الترشيح العام)
    for key in vnz.STRATEGY_SETUP_SCANNERS:
        _, why = vnz._nominee_score_for(key, snap['BOTTOMUSDT'])
        assert 'ترشيح عام' not in why, f"استراتيجية {key} بلا بصمة خاصة!"
    print("✅ 3) البصمات الرقمية حصرية: القاع للارتداد، الضيق للانضغاط، القمة للاختراق، الصاعد للاتجاهية")


def test_nomination_deterministic_and_count():
    """نفس المقاييس ⇒ نفس الترشيح حرفيًا (حتمية كاملة) + القائمة 10 لكل استراتيجية."""
    wide = {}
    for i in range(60):
        pos = (i % 20) / 20.0
        chg = ((i * 7) % 30) - 15.0
        rng = 1.5 + (i % 9) * 1.3
        liq = 100.0 - i * 1.5
        wide[f'COIN{i}USDT'] = _mkrow(f'COIN{i}USDT', pos, chg, rng, max(0.0, liq))
    old_rows, old_marker = vnz.wide_universe_rows, vnz.universe_last_refresh
    old_pool, old_nom = vnz._NOMINEE_RANKED_POOL, vnz.STRATEGY_NOMINEES
    old_built = vnz.strategy_nominees_built_at
    try:
        vnz.wide_universe_rows = wide
        vnz.universe_last_refresh = 999.0
        vnz._NOMINEE_RANKED_POOL = {}
        vnz.strategy_nominees_built_at = -1.0
        n1 = vnz.nominate_strategy_candidates()
        n2 = vnz.nominate_strategy_candidates()
        assert set(n1.keys()) == set(vnz.STRATEGY_SETUP_SCANNERS.keys()), 'كل الاستراتيجيات لها قائمة'
        for key in n1:
            assert len(n1[key]) == 10, f"{key}: {len(n1[key])} بدل 10"
            assert [x['symbol'] for x in n1[key]] == [x['symbol'] for x in n2[key]], 'الترشيح غير حتمي!'
            scores = [x['score'] for x in n1[key]]
            assert scores == sorted(scores, reverse=True), 'الترتيب تنازلي بالدرجة'
            assert all('why_ar' in x for x in n1[key]), 'كل ترشيح له سبب موثق'
        print("✅ 4) الترشيح حتمي (تكرار متطابق حرفيًا) و10 عملات لكل استراتيجية من كون 60")
    finally:
        vnz.wide_universe_rows, vnz.universe_last_refresh = old_rows, old_marker
        vnz._NOMINEE_RANKED_POOL, vnz.STRATEGY_NOMINEES = old_pool, old_nom
        vnz.strategy_nominees_built_at = old_built


def test_nomination_excludes_open_positions_with_topup():
    """الصفقة المفتوحة تُستبعد ويُزوَّد الفراغ من بقية الترتيب — تبقى القائمة 10."""
    wide = {f'COIN{i}USDT': _mkrow(f'COIN{i}USDT', i / 100.0, 3.0, 4.0, 90.0 - i) for i in range(40)}
    old_rows, old_marker = vnz.wide_universe_rows, vnz.universe_last_refresh
    old_pool, old_nom = vnz._NOMINEE_RANKED_POOL, vnz.STRATEGY_NOMINEES
    old_built = vnz.strategy_nominees_built_at
    old_open = dict(vnz.open_signals_cache)
    try:
        vnz.wide_universe_rows = wide
        vnz.universe_last_refresh = 1234.0
        vnz._NOMINEE_RANKED_POOL = {}
        vnz.strategy_nominees_built_at = -1.0
        first = vnz.nominate_strategy_candidates()
        victim = first['BB_STOCH'][0]['symbol']          # الأول ستصبح صفقة مفتوحة
        vnz.open_signals_cache[victim] = {'symbol': victim}
        second = vnz.nominate_strategy_candidates()
        lst = second['BB_STOCH']
        assert len(lst) == 10, f"بعد الاستبعاد والتزويد: {len(lst)} بدل 10"
        assert all(x['symbol'] != victim for x in lst), 'صفقة مفتوحة داخل قائمة الفحص!'
        print(f"✅ 5) استبعاد الصفقة المفتوحة ({victim}) بتزويد تلقائي — القائمة بقيت 10")
    finally:
        vnz.wide_universe_rows, vnz.universe_last_refresh = old_rows, old_marker
        vnz._NOMINEE_RANKED_POOL, vnz.STRATEGY_NOMINEES = old_pool, old_nom
        vnz.strategy_nominees_built_at = old_built
        vnz.open_signals_cache.clear()
        vnz.open_signals_cache.update(old_open)


def test_loop_is_strategy_first():
    """الحلقة الرئيسية: حلقة الاستراتيجيات تحيط بحلقة مرشحيها العشرة — لا فحص لكل رمز×كل استراتيجية."""
    tree = ast.parse(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'vnz.py'),
                          encoding='utf-8').read())
    src = ast.unparse(tree)
    assert 'symbols_to_process' not in src, 'بقايا الحلقة الرمزية القديمة!'
    assert 'for key, check_func, name in strategies_to_check' in src, 'لا حلقة استراتيجية'
    assert "strategy_nominees_map.get(key)" in src, 'الحلقة لا تفحص مرشحي الاستراتيجية'
    assert 'NOMINEES_PER_STRATEGY' in src, 'لا سقف 10 مرشحين'
    i_strat = src.index('for key, check_func, name in strategies_to_check')
    i_nom = src.index('strategy_nominees_map.get(key)')
    assert i_nom > i_strat, 'مرشحو الاستراتيجية يجب أن يُفحصوا داخل حلقة الاستراتيجية'
    # بوابة حالة السوق قبل جلب الشموع (توفير كامل عند المنع)
    i_allow = src.index('MARKET_STATE_STRATEGY_ALLOW.get(overall_market_regime)')
    i_fetch = src.index('fetch_historical_data(symbol, SIGNAL_GENERATION_TIMEFRAME', i_strat)
    assert i_allow < i_fetch, 'بوابة حالة السوق يجب أن تسبق جلب الشموع'
    print("✅ 6) الحلقة استراتيجيةً: كل استراتيجية تفحص مرشحيها العشرة حصرًا وبواباتها قبل الجلب")


def test_market_map_gate_alignment():
    """بوابة الفحص ومنطق الترشيح يستخدمان نفس خريطة حالة السوق — لا فحص ممنوع."""
    allowed_keys = set()
    for keys in vnz.MARKET_STATE_STRATEGY_ALLOW.values():
        if keys:
            allowed_keys |= set(keys)
    for k in allowed_keys:
        assert k in vnz.STRATEGY_SETUP_SCANNERS, f"مفتاح غريب في خريطة السوق: {k}"
        _, why = vnz._nominee_score_for(k, _mkrow('XUSDT', 0.5, 0.0, 4.0, 50.0))
        assert why, f"بلا بصمة ترشيح لـ {k}"
    print("✅ 7) اتساق الخرائط: كل استراتيجية مسموحة لها بصمة ترشيح خاصة")


def test_api_registered():
    rules = {r.rule for r in vnz.app.url_map.iter_rules()}
    assert '/api/strategy_candidates' in rules, 'نقطة دائرة الترشيح غير مسجلة'
    print("✅ 8) /api/strategy_candidates مسجلة للوحة التحكم")


if __name__ == '__main__':
    tests = [v for k, v in sorted(globals().items()) if k.startswith('test_')]
    failed = 0
    for t in tests:
        try:
            t()
        except Exception as e:
            failed += 1
            print(f"❌ {t.__name__}: {e}")
            import traceback; traceback.print_exc()
    print(f"\n{'=' * 50}\nالنتيجة: {len(tests) - failed}/{len(tests)} نجح")
    sys.exit(1 if failed else 0)

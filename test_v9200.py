# -*- coding: utf-8 -*-
"""اختبارات V9.20.0 — طبقة البيانات متعددة المصادر (Bybit/OKX/Gate + باينانس احتياطًا).
تُجرى بلا شبكة: كل نداءات HTTP موكّاة عبر _get_json أو طرق المزودين."""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('LOG_LEVEL', 'ERROR')
os.environ.setdefault('BINANCE_API_KEY', 'test_key_dummy')
os.environ.setdefault('BINANCE_API_SECRET', 'test_secret_dummy')

import vnz  # noqa: E402


def test_version_and_dashboard():
    assert vnz.APP_VERSION.startswith('V9.'), f"الإصدار: {vnz.APP_VERSION}"
    html = vnz.get_dashboard_html()
    assert 'df-chip' in html and 'updateDataFeed' in html
    print("✅ 1) الإصدار V9.20.0 + شارة مصدر البيانات في اللوحة")


def test_config_defaults():
    assert vnz.DATA_FEED_MODE == 'multi'
    names = [p.name for p in vnz.data_feed.providers]
    assert names == ['bybit', 'okx', 'gate'], f"ترتيب المزودين: {names}"
    assert vnz.data_feed._enabled() is True
    print("✅ 2) الإعدادات الافتراضية: multi + الترتيب bybit,okx,gate")


def test_symbol_conversion_and_intervals():
    b, o, g = vnz.BybitProvider(), vnz.OKXProvider(), vnz.GateProvider()
    assert b.to_symbol('BTCUSDT') == 'BTCUSDT'
    assert o.to_symbol('BTCUSDT') == 'BTC-USDT'
    assert g.to_symbol('BTCUSDT') == 'BTC_USDT'
    # رموز غير USDT مرفوضة في كل المزودين
    assert b.to_symbol('ETHBTC') is None and o.to_symbol('ETHBTC') is None
    # فريمات غير مدعومة: Gate لا يعرف 3m/2h — Bybit لا يعرف 8h
    assert not g.supports('BTCUSDT', '3m') and not g.supports('BTCUSDT', '2h')
    assert '8h' not in b.INTERVAL_MAP and '8h' in g.INTERVAL_MAP
    assert b.supports('BTCUSDT', '15m') and o.supports('BTCUSDT', '12h')
    print("✅ 3) تعليم الرموز (Bybit/OKX/Gate) وخريطة الفريمات المدعومة")


def test_lookback_to_candles():
    assert vnz.lookback_to_candles('800 hour', '4h') == 200
    assert vnz.lookback_to_candles('2 day', '1h') == 50   # أرضية 50 شمعة
    big = vnz.lookback_to_candles('496 hour', '15m')  # 1984 — [V9.22.0] السقف 1000→5000 (الترقيم الرجعي)
    assert big == 1984
    print("✅ 4) تحويل lookback إلى عدد شموع (بأرضية وسقف 5000 مع الترقيم)")


def test_bybit_kline_normalization():
    p = vnz.BybitProvider()
    payload = {'retCode': 0, 'result': {'list': [
        ['1704067200000', '42000.5', '42500.0', '41800.0', '42300.0', '123.45', '5200000.0'],
        ['1704066300000', '41900.0', '42100.0', '41500.0', '42000.0', '111.11', '4600000.0'],
    ]}}
    p._get_json = lambda path, params=None: payload
    rows = p.fetch_klines('BTCUSDT', '15m', limit=100)
    assert rows and len(rows) == 2 and len(rows[0]) == 12
    assert rows[0][0] == 1704066300000 and rows[1][0] == 1704067200000  # تصاعدي
    assert abs(rows[1][4] - 42300.0) < 1e-9          # إغلاق
    assert abs(rows[1][5] - 123.45) < 1e-9           # حجم
    assert abs(rows[1][7] - 5200000.0) < 1e-9        # حجم اقتباسي (turnover)
    assert rows[1][6] - rows[1][0] == 15 * 60 * 1000 - 1  # close_time
    print("✅ 5) تطبيع شموع Bybit إلى صيغة Binance (12 عمودًا تصاعديًا)")


def test_okx_kline_normalization_and_pagination():
    p = vnz.OKXProvider()
    recent = {'code': '0', 'data': [
        [str(1704074400000 - i * 90000), '100', '110', '95', '105', '10', '1000', '1050', '1']
        for i in range(300)]}  # 300 صف حديث بطوابع فريدة
    pages = {'n': 0}

    def fake(path, params=None):
        if path == '/api/v5/market/candles':
            return recent
        if path == '/api/v5/market/history-candles':
            pages['n'] += 1
            after = int(params.get('after', 0))
            base = after - 90000
            return {'code': '0', 'data': [[str(base - i * 90000), '90', '95', '85', '92', '5', '500', '520', '1'] for i in range(100)]}
        return None

    p._get_json = fake
    rows = p.fetch_klines('BTCUSDT', '15m', limit=360)
    assert rows and len(rows) >= 300  # استكمل من history
    assert pages['n'] >= 1
    ts = [r[0] for r in rows]
    assert ts == sorted(ts) and len(set(ts)) == len(ts)  # تصاعدي بلا تكرار
    assert abs(rows[-1][4] - 105.0) < 1e-9  # إغلاق أحدث شمعة
    assert abs(rows[-1][5] - 10.0) < 1e-9   # حجم الأساس
    assert abs(rows[-1][7] - 1050.0) < 1e-9  # volCcyQuote = حجم اقتباسي
    print("✅ 6) شموع OKX مع استكمال الصفحات الأقدم وتطبيع صحيح")


def test_gate_kline_normalization():
    p = vnz.GateProvider()
    # الصيغة الفعلية (مدققة حيًا): [t(ث), quote_vol, o, c, h, l, base_vol, done]
    payload = [
        ['1704067200', '550.0', '1.0', '1.1', '1.2', '0.9', '500', 'true'],
        ['1704066300', '410.0', '0.95', '1.0', '1.05', '0.85', '400', 'false'],
    ]
    p._get_json = lambda path, params=None: payload
    rows = p.fetch_klines('NMRUSDT', '15m', limit=100)
    assert rows and len(rows) == 2 and len(rows[0]) == 12
    assert rows[0][0] == 1704066300000 and rows[1][0] == 1704067200000  # ثوانٍ→مللي ثانية + ترتيب
    assert abs(rows[1][4] - 1.1) < 1e-9        # إغلاق = الموضع 3
    assert abs(rows[1][5] - 500.0) < 1e-9      # الحجم الأساسي = الموضع 6
    assert abs(rows[1][7] - 550.0) < 1e-9      # الحجم الاقتباسي = الموضع 1
    # صيغة كائن (احتياط للتوثيق القديم)
    p2 = vnz.GateProvider()
    p2._get_json = lambda path, params=None: [
        {'time': 1704067200, 'open': '1.0', 'high': '1.2', 'low': '0.9', 'close': '1.1',
         'volume': '500', 'sum': '550.0'}]
    rows2 = p2.fetch_klines('BTCUSDT', '15m', limit=10)
    assert rows2 and abs(rows2[0][7] - 550.0) < 1e-9
    print("✅ 7) شموع Gate.io: المصفوفة الفعلية (اقتباسي=1/أساسي=6) + ثوانٍ→مللي ثانية + ترتيب")


def test_failover_order():
    mf = vnz.MultiSourceDataFeed()
    mf.providers[0].fetch_klines = lambda *a, **k: None            # bybit يفشل
    good = [[1] + [100.0, 110.0, 90.0, 105.0, 10.0] + [1 + 89999999, 1000.0, 0, 0.0, 0.0, '0']]
    mf.providers[1].fetch_klines = lambda *a, **k: list(good)      # okx ينجح
    rows = mf.get_klines('BTCUSDT', '15m', limit=100)
    assert rows == good
    assert mf.stats['okx_ok'] >= 1 and mf.stats['bybit_fail'] >= 1
    assert mf.last_source['klines'] == 'okx'
    print("✅ 8) التجاوب: فشل bybit → نجاح okx مع محاسبة الإحصاء")


def test_total_failure_binance_fallback():
    mf = vnz.MultiSourceDataFeed()
    before = mf.stats['klines_binance_fallback']
    for p in mf.providers:
        p.fetch_klines = lambda *a, **k: None
    rows = mf.get_klines('BTCUSDT', '15m', limit=100)
    assert rows is None  # None → يستدعي الموقع مسار باينانس القديم
    assert mf.stats['klines_binance_fallback'] == before + 1
    print("✅ 9) فشل الجميع → None (احتياط باينانس) + عدّاد التراجع")


def test_binance_only_mode():
    old_mode = vnz.DATA_FEED_MODE
    try:
        vnz.DATA_FEED_MODE = 'binance_only'
        mf = vnz.MultiSourceDataFeed()
        called = {'n': 0}
        def boom(*a, **k):
            called['n'] += 1
            return None
        for p in mf.providers:
            p.fetch_klines = boom
        assert mf.get_klines('BTCUSDT', '15m') is None
        assert called['n'] == 0  # لا يلمس المزودين إطلاقًا
    finally:
        vnz.DATA_FEED_MODE = old_mode
    print("✅ 10) وضع binance_only: تجاوز كامل للطبقة بلا أي نداء")


def test_health_cooldown():
    mf = vnz.MultiSourceDataFeed()
    name = mf.providers[0].name
    assert mf._healthy(name)
    for _ in range(max(1, vnz.DATA_FEED_FAIL_THRESHOLD)):
        mf._record_fail(name, 'فشل اختبار')
    assert not mf._healthy(name)  # مبرود
    with mf.lock:
        mf.health[name]['cooldown_until'] = time.time() - 1
    assert mf._healthy(name)  # انتهى التبريد
    mf._record_ok(name)
    with mf.lock:
        assert mf.health[name]['consec_fail'] == 0
    print("✅ 11) تبريد المزود بعد الفشل المتتالي والتعافي مع النجاح")


def test_coverage_gating():
    mf = vnz.MultiSourceDataFeed()
    with mf.lock:
        mf.coverage['bybit'] = {'BTCUSDT'}  # bybit لا يملك NMRUSDT
    cands = mf._candidates('NMRUSDT', '15m')
    names = [p.name for p in cands]
    assert 'bybit' not in names and 'okx' in names and 'gate' in names
    print("✅ 12) خريطة التغطية: رمز غائب عن bybit يُقفز مباشرة إلى okx/gate")


def test_tickers_mapping():
    byb = vnz.BybitProvider()
    byb._get_json = lambda path, params=None: {'retCode': 0, 'result': {'list': [
        {'symbol': 'BTCUSDT', 'lastPrice': '42000.0', 'highPrice24h': '43000.0',
         'lowPrice24h': '41000.0', 'turnover24h': '900000000.0', 'price24hPcnt': '0.0521'}]}}
    t = byb.fetch_all_24h_tickers()[0]
    assert t['symbol'] == 'BTCUSDT' and abs(t['priceChangePercent'] - 5.21) < 1e-6

    okx = vnz.OKXProvider()
    okx._get_json = lambda path, params=None: {'code': '0', 'data': [
        {'instId': 'BTC-USDT', 'last': '42000.0', 'open24h': '40000.0',
         'high24h': '43000.0', 'low24h': '41000.0', 'volCcy24h': '800000000.0'}]}
    t2 = okx.fetch_all_24h_tickers()[0]
    assert t2['symbol'] == 'BTCUSDT' and abs(t2['priceChangePercent'] - 5.0) < 1e-6

    gate = vnz.GateProvider()
    gate._get_json = lambda path, params=None: [
        {'currency_pair': 'BTC_USDT', 'last': '42000.0', 'high_24h': '43000.0',
         'low_24h': '41000.0', 'quote_volume': '700000000.0', 'change_percentage': '4.8'}]
    t3 = gate.fetch_all_24h_tickers()[0]
    assert t3['symbol'] == 'BTCUSDT' and abs(t3['priceChangePercent'] - 4.8) < 1e-6

    mf = vnz.MultiSourceDataFeed()
    mf.providers[0].fetch_all_24h_tickers = lambda: None
    mf.providers[1].fetch_all_24h_tickers = lambda: [{'symbol': 'X', 'lastPrice': 1.0,
        'highPrice': 1.0, 'lowPrice': 1.0, 'quoteVolume': 1.0, 'priceChangePercent': 0.0}]
    out = mf.get_24h_tickers()
    assert out and out[0]['symbol'] == 'X'
    print("✅ 13) تطبيع تكه 24 ساعة من المزودين الثلاثة (نسبة التغير/السيولة)")


def test_order_book_normalization():
    mf = vnz.MultiSourceDataFeed()
    # نموّك مسار المزود الحقيقي (التطبيع داخل fetch_order_book عبر _ob_pairs)
    mf.providers[0]._get_json = lambda path, params=None: {
        'retCode': 0, 'result': {'bids': [['42000.1', '0.5'], ['bad', 'x']],
                                 'asks': [['42005.0', '1.2']]}}
    ob = mf.get_order_book('BTCUSDT', 25)
    assert ob and ob['bids'][0] == [42000.1, 0.5] and len(ob['bids']) == 1  # الصف التالف يُسقط
    assert ob['asks'][0] == [42005.0, 1.2]
    print("✅ 14) عمق السوق: صفوف عشرية نظيفة وإسقاط الصفوف التالفة")


def test_fetch_historical_data_integration():
    """المسار الكامل: fetch_historical_data يسحب من data_feed دون لمس باينانس."""
    old_client, old_hub, old_df = vnz.client, vnz.stream_hub, vnz.data_feed
    try:
        vnz.client = object()  # truthy فقط
        vnz.stream_hub = None
        base = int(time.time() * 1000) // 3600000 * 3600000
        rows = []
        for i in range(60):
            ts = base - (59 - i) * 3600000
            rows.append([ts, 100.0 + i, 105.0 + i, 99.0 + i, 102.0 + i,
                         50.0 + i, ts + 3599999, 5000.0 + i * 10, 0, 0.0, 0.0, '0'])

        class StubFeed:
            def get_klines(self, symbol, interval, limit=300):
                assert symbol == 'BTCUSDT' and interval == '1h'
                return list(rows)
        vnz.data_feed = StubFeed()

        df = vnz.fetch_historical_data('BTCUSDT', '1h', 2)
        assert df is not None and len(df) == 60
        for col in ('open', 'high', 'low', 'close', 'volume', 'quote_volume', 'taker_buy_base'):
            assert col in df.columns
        assert float(df['close'].iloc[-1]) == 161.0
    finally:
        vnz.client, vnz.stream_hub, vnz.data_feed = old_client, old_hub, old_df
    print("✅ 15) fetch_historical_data يعمل من المزودين البديلين (DataFrame سليم)")


def test_status_snapshot_and_api_route():
    snap = vnz.data_feed.status_snapshot()
    assert snap['mode'] == 'multi' and snap['enabled'] is True
    names = [p['name'] for p in snap['providers']]
    assert names == ['bybit', 'okx', 'gate']
    rules = {r.rule for r in vnz.app.url_map.iter_rules()}
    assert '/api/datafeed' in rules
    print("✅ 16) status_snapshot + endpoint /api/datafeed مسجّل")


if __name__ == '__main__':
    tests = [v for k, v in sorted(globals().items()) if k.startswith('test_')]
    failed = 0
    for t in tests:
        try:
            t()
        except Exception as e:
            failed += 1
            import traceback
            print(f"❌ {t.__name__}: {e}")
            traceback.print_exc()
    print(f"\n{'=' * 50}\nالنتيجة: {len(tests) - failed}/{len(tests)} ناجحة" + (" — كل الاختبارات خضراء ✅" if failed == 0 else f" — {failed} فاشلة ❌"))
    sys.exit(1 if failed else 0)

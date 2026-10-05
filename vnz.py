
import time
import os
import sys
import json
import logging
import requests
import numpy as np
import pandas as pd
import psycopg2
import pickle
import redis
import re
import gc
import math
import random
from decimal import Decimal, ROUND_DOWN
from urllib.parse import urlparse
from psycopg2 import sql, OperationalError, InterfaceError
from psycopg2.extras import RealDictCursor
from binance.client import Client
from binance.exceptions import BinanceAPIException
from flask import Flask, jsonify, render_template_string, request, Response
from flask_cors import CORS
from threading import Thread, Lock
from datetime import datetime, timezone, timedelta
from decouple import config
from typing import List, Dict, Optional, Any, Set, Tuple
from sklearn.preprocessing import StandardScaler
from collections import deque, Counter
import warnings

# --- إعدادات التجاهل واللوجر ---
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crypto_bot_v9_logs.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('CryptoBotV9.11.0')

# زمن إقلاع العملية لحساب مدة التشغيل في لوحة التحكم
BOOT_TIME = time.time()

# --- المشفر المخصص لأنواع بيانات NumPy ---
class NpEncoder(json.JSONEncoder):
    """ مشفر مخصص لأنواع بيانات NumPy """
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
        return super(NpEncoder, self).default(obj)

# --- تحميل متغيرات البيئة ---
try:
    API_KEY: str = config('BINANCE_API_KEY')
    API_SECRET: str = config('BINANCE_API_SECRET')
    DB_URL: str = config('DATABASE_URL')
    REDIS_URL: str = config('REDIS_URL', default='redis://localhost:6379/0')
    TELEGRAM_BOT_TOKEN: str = config('TELEGRAM_BOT_TOKEN', default='')
    TELEGRAM_CHAT_ID: str = config('TELEGRAM_CHAT_ID', default='')
    # --- [تحسين V9.8] إعدادات جديدة قابلة للضبط عبر متغيرات البيئة ---
    DASHBOARD_USERNAME: str = config('DASHBOARD_USERNAME', default='')
    DASHBOARD_PASSWORD: str = config('DASHBOARD_PASSWORD', default='')
    DAILY_MAX_LOSS_USDT: float = config('DAILY_MAX_LOSS_USDT', default=20.0, cast=float)

except Exception as e:
    logger.critical(f"❌ فشل حاسم في تحميل متغيرات البيئة الأساسية: {e}")
    exit(1)

# --- متغيرات عامة وإعدادات البوت ---
is_trading_enabled: bool = False
trading_status_lock = Lock()

# --- المتغيرات القابلة للتعديل ---
RISK_PER_TRADE_PERCENT: float = 0.85
risk_per_trade_lock = Lock()

BUY_CONFIDENCE_THRESHOLD = 0.53
buy_confidence_lock = Lock()

ORDER_BOOK_MIN_BID_ASK_RATIO: float = 1.15
order_book_ratio_lock = Lock()

VOLUME_FILTER_MULTIPLIER: float = 1.1
volume_filter_lock = Lock()

MIN_PROFIT_PERCENT: float = 0.8

# --- مفاتيح تفعيل الاستراتيجيات ---
USE_ML_STRATEGY: bool = False
ml_strategy_lock = Lock()

USE_BB_STOCH_STRATEGY: bool = True
bb_stoch_strategy_lock = Lock()

USE_MACD_EMA_STRATEGY: bool = True
macd_ema_strategy_lock = Lock()

USE_EMA_RSI_STRATEGY: bool = True
ema_rsi_strategy_lock = Lock()

USE_PULLBACK_STRATEGY: bool = True
pullback_strategy_lock = Lock()

USE_BB_SQUEEZE_STRATEGY: bool = True
bb_squeeze_strategy_lock = Lock()

USE_BULLISH_MOMENTUM_STRATEGY: bool = True
bullish_momentum_strategy_lock = Lock()

USE_SR_BREAKOUT_STRATEGY: bool = True
sr_breakout_strategy_lock = Lock()

# --- [تحسين V9.8] فلاتر تأكيد مستوى الإشارة ---
# تأكيد الترند الصاعد على فريم الساعة قبل قبول الإشارات الاتجاهية/الاختراقية
USE_HTF_CONFIRMATION: bool = config('USE_HTF_CONFIRMATION', default=True, cast=bool)
# فلتر زخم قصير الأجل إضافي (صارم - غير مفعّل افتراضيًا لتجنب إيقاف كل الإشارات)
USE_SHORT_TERM_MOMENTUM_FILTER: bool = config('USE_SHORT_TERM_MOMENTUM_FILTER', default=False, cast=bool)
# مدة تخزين نتيجة تأكيد الترند لكل عملة (بالثواني) لتقليل استهلاك API
HTF_CONFIRMATION_CACHE_TTL: int = config('HTF_CONFIRMATION_CACHE_TTL', default=900, cast=int)

# --- [تحسين V9.9.1] إعدادات حماية الحظر من Binance (خطأ -1003) ---
# حد Binance الرسمي 6000 وزن/دقيقة لكل IP — وعلى Render المجاني الـ IP مشترك مع خدمات أخرى،
# لذا الميزانية الافتراضية متحفظة (1500) وتنخفض تلقائيًا 40% عند كل حظر ثم تتعافى تدريجيًا
RATE_LIMIT_BUDGET_PER_MIN: int = config('RATE_LIMIT_BUDGET_PER_MIN', default=1500, cast=int)
# الفاصل الأدنى بالثواني بين أي طلبين REST متتاليين
API_MIN_SPACING_SEC: float = config('API_MIN_SPACING_SEC', default=0.15, cast=float)
# تبريد إضافي (ثوانٍ) بعد انتهاء الحظر قبل استئناف الطلبات — يمنع انفجار الخيوط
# فور انتهاء الحظر (Thundering Herd) الذي كان يسبب التصعيد 30 ثانية → 16 دقيقة
BAN_RESUME_COOLDOWN_SEC: int = config('BAN_RESUME_COOLDOWN_SEC', default=90, cast=int)
# بعد الاستئناف: الفاصل الأدنى يتضاعف ×4 لمدة (ثوانٍ) ثم يعود تدريجيًا للطبيعي
BAN_RESUME_RAMP_SEC: int = config('BAN_RESUME_RAMP_SEC', default=120, cast=int)
# فاصل تحديث أسعار Redis بالثواني (كل طلب أسعار شامل وزنه 4)
PRICE_UPDATE_INTERVAL_SEC: int = config('PRICE_UPDATE_INTERVAL_SEC', default=3, cast=int)

# --- [تحسين V9.11.0] منع اختناق خيوط الويب (waitress queue depth) ---
# السبب الجذري: /api/market_status كان يستدعي Binance مباشرة (وزن 5) في كل استطلاع
# من المتصفح كل 5 ثوانٍ — وأثناء انشغال الحارس أو الحظر يبقى خيط waitress محجوزًا
# في acquire() لثوانٍ إلى دقائق، فتتراكم الطابور (Task queue depth 1..15+).
# الحل: خيط خلفي يحدّث رصيد USDT في كاش، واللوحة تقرأ الكاش فورًا بلا أي نداء شبكي.
DASHBOARD_BALANCE_REFRESH_SEC: int = config('DASHBOARD_BALANCE_REFRESH_SEC', default=45, cast=int)

# --- [تحسين V9.10] الكشف الديناميكي عن العملات الأكثر حيوية (سيولة + تقلب + انفجارات) ---
# بدل قائمة ثابتة: طلب واحد (وزن 80) يجيب إحصائيات 24 ساعة لكل العملات، ثم ترشيح وترتيب:
# 45% السيولة (quoteVolume) + 35% التقلب (المدى اليومي high-low) + 20% الانفجار (|التغير%|)
USE_DYNAMIC_UNIVERSE: bool = config('USE_DYNAMIC_UNIVERSE', default=True, cast=bool)
# عدد العملات المستهدفة كل دورة (طلب المستخدم: 20 عملة مختلفة للبحث عن فرص أكثر)
DYNAMIC_UNIVERSE_SIZE: int = config('DYNAMIC_UNIVERSE_SIZE', default=20, cast=int)
# فترة تحديث القائمة بالدقائق (لا داعي لكل دورة — الترتيب يتغير ببطء نسبيًا)
DYNAMIC_UNIVERSE_REFRESH_MIN: int = config('DYNAMIC_UNIVERSE_REFRESH_MIN', default=30, cast=int)
# حد أدنى للسيولة: حجم تداول 24 ساعة بالمليون USDT (يستبعد العملات الراكدة)
DYNAMIC_UNIVERSE_MIN_QUOTE_VOLUME: float = config('DYNAMIC_UNIVERSE_MIN_QUOTE_VOLUME', default=10000000.0, cast=float)
# حد أدنى للتقلب: المدى اليومي % (يقصّ العملات الميتة ويستهدف الانفجارات السعرية)
DYNAMIC_UNIVERSE_MIN_RANGE_PCT: float = config('DYNAMIC_UNIVERSE_MIN_RANGE_PCT', default=1.5, cast=float)


BASE_ML_MODEL_NAME: str = 'LightGBM_Scalping_V9_With_Microstructure'
MODEL_FOLDER: str = 'V9'
SIGNAL_GENERATION_TIMEFRAME: str = '15m'
HIGHER_TIMEFRAME: str = '1h' # الإطار الزمني الأعلى المستخدم في فلتر التأكيد
TIMEFRAMES_FOR_TREND_LIGHTS: List[str] = ['15m', '1h', '4h']
# [تحسين V9.8] قيمة خفيفة افتراضيًا (30 يومًا) مناسبة للخطة المجانية على Render
# كافية تمامًا لكل المؤشرات (تتطلب ~200 شمعة) وتقلل زمن الدورة واستهلاك الذاكرة
SIGNAL_GENERATION_LOOKBACK_DAYS: int = config('SIGNAL_GENERATION_LOOKBACK_DAYS', default=30, cast=int)
REDIS_PRICES_HASH_NAME: str = "crypto_bot_current_prices_v10"
TRADING_FEE_PERCENT: float = 0.1
STATS_TRADE_SIZE_USDT: float = 4.0
BTC_SYMBOL: str = 'BTCUSDT'
MAX_OPEN_TRADES: int = config('MAX_OPEN_TRADES', default=5, cast=int)
SYMBOL_PROCESSING_BATCH_SIZE: int = 10

# --- إعدادات رحلة التداول الديناميكية ---
USE_DYNAMIC_JOURNEY = True

# --- إعدادات المؤشرات الفنية (تم تعديل بعضها للسكالبينج) ---
EMA_FAST_PERIOD: int = 21
EMA_SLOW_PERIOD: int = 50
ADX_PERIOD: int = 14
RSI_PERIOD: int = 14
ATR_PERIOD: int = 14
BTC_CORR_PERIOD: int = 30
REL_VOL_PERIOD: int = 30
MOMENTUM_PERIOD: int = 10 # أسرع
EMA_SLOPE_PERIOD: int = 5
SUPERTREND_ATR_PERIOD: int = 10
SUPERTREND_MULTIPLIER: float = 3.0
CANDLE_AVG_VOLUME_PERIOD: int = 15
SR_LOOKBACK_CANDLES: int = 60
SR_MIN_BOUNCES: int = 2

# --- إعدادات الفلاتر المتقدمة وإدارة الصفقات ---
ORDER_BOOK_DEPTH_LIMIT: int = 100
ORDER_BOOK_ANALYSIS_RANGE_PCT: float = 0.005
USE_ATR_TRAILING_STOP: bool = True
ATR_TS_PERIOD: int = 14
ATR_TS_MULTIPLIER: float = 2.2

# --- متغيرات الحالة والكاش ---
conn: Optional[psycopg2.extensions.connection] = None
client: Optional[Client] = None
redis_client: Optional[redis.Redis] = None
ml_models_cache: Dict[str, Any] = {}
exchange_info_map: Dict[str, Any] = {}
validated_symbols_to_scan: List[str] = []
# [تحسين V9.10] حالة القائمة الديناميكية (العملات الأكثر حيوية)
universe_lock = Lock()
universe_last_refresh: float = 0.0
universe_source: str = 'static'  # dynamic / static_fallback / static
universe_meta: Dict[str, Any] = {}
_static_fallback_symbols: List[str] = []
open_signals_cache: Dict[str, Dict] = {}
signal_cache_lock = Lock()
notifications_cache = deque(maxlen=50)
notifications_lock = Lock()
rejection_logs_cache = deque(maxlen=100)
rejection_logs_lock = Lock()
current_market_state: Dict[str, Any] = {"overall_regime": "INITIALIZING", "trend_details_by_tf": {}, "last_updated": None}
market_state_lock = Lock()
last_market_state_check = 0
technical_signals_cache: Dict[str, Dict] = {}
TECHNICAL_SIGNAL_CACHE_DURATION: int = 60 * 5
technical_signals_lock = Lock()

# --- [تحسين V9.8] حالة قاطع الحماية اليومي وكاش ATR ---
daily_realized_pnl_usdt: float = 0.0
daily_pnl_date: str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
daily_loss_notified: bool = False
daily_pnl_lock = Lock()
ATR_TRAILING_CACHE: Dict[str, Tuple[float, float]] = {}
atr_trailing_lock = Lock()

# ============================================================
# [تحسين V9.9] طبقة الحماية من حظر Binance (خطأ -1003)
# المشكلة السابقة: البوت كان ينهار (exit) عند الحظر، وRender يعيد تشغيله
# فورًا فيضرب Binance مرارًا أثناء الحظر فيطول مدته (Ban Escalation).
# الحل: حارس وزن يمنع تجاوز الحد أصلًا + انتظار ذكي بدل الانهيار.
# ============================================================
class BinanceRateGuard:
    """حارس وزن الطلبات V9.9.1: نافذة دقيقة منزلقة (حد Binance = 6000/دقيقة) + ميزانية تكيفية.
    دروس تصعيد الحظر (30 ثانية → 16 دقيقة) التي عولجت هنا:
    1) محاسبة وزن الشموع الخاطئة (طلب واحد = عدة طلبات داخلية) → مقدّر وزن دقيق في safe_get_klines
    2) انفجار الخيوط فور انتهاء الحظر Thundering Herd → تبريد استئناف + تدرج في الفاصل
    3) ميزانية ثابتة تعيد ضرب الحظر على IP مشترك (Render مجاني) → ميزانية تكيفية:
       تنخفض 40% عند كل حظر وتتعافى 8% كل 3 دقائق نظيفة حتى المستوى المُعدّ."""
    def __init__(self, budget_per_min: int = 1500, min_spacing: float = 0.15):
        self.configured_budget = max(400, int(budget_per_min))
        self.budget = self.configured_budget
        self.budget_floor = max(300, int(self.configured_budget * 0.25))
        self.min_spacing = max(0.0, float(min_spacing))
        self.resume_cooldown = max(0, int(BAN_RESUME_COOLDOWN_SEC))
        self.resume_ramp_sec = max(0, int(BAN_RESUME_RAMP_SEC))
        self.resume_spacing_factor = 4.0
        self._lock = Lock()
        self._window: deque = deque()  # (timestamp, weight)
        self._last_call = 0.0
        self.banned_until = 0.0
        self.last_error: str = ''
        self.total_requests = 0
        self.total_throttled_sec = 0.0
        self.ban_count = 0
        self._last_ban_ts = 0.0
        self._last_recover_ts = 0.0
        self._ramp_until = 0.0
        self._resumed = False  # هل استُؤفي الطلبات بعد آخر حظر؟

    def _prune(self, now: float) -> None:
        while self._window and now - self._window[0][0] > 60:
            self._window.popleft()

    def used_weight(self) -> int:
        with self._lock:
            self._prune(time.time())
            return int(sum(w for _, w in self._window))

    def _effective_resume_wait(self, now: float) -> float:
        """الانتظار الفعلي: بقيّة الحظر + تبريد الاستئناف (إن وُجد حظر سابق).
        عند لحظة الاستئناف بالضبط: يُفعّل تدرج الفاصل ×4 لمدة resume_ramp_sec مرة واحدة."""
        with self._lock:
            if self.banned_until <= 0:
                return 0.0
            wait = self.banned_until - now + float(self.resume_cooldown)
            if wait <= 0 and not self._resumed:
                # لحظة الاستئناف بعد الحظر: بدء فترة التدرج مرة واحدة فقط
                self._resumed = True
                self._ramp_until = now + float(self.resume_ramp_sec)
                self._last_call = now
            return wait

    def _maybe_recover(self, now: float) -> None:
        """تعافي تدريجي للميزانية: +8% كل 3 دقائق بلا حظر حتى المستوى المُعدّ."""
        with self._lock:
            if self.budget >= self.configured_budget:
                return
            anchor = max(self._last_ban_ts, self._last_recover_ts)
            if now - anchor >= 180:
                self._last_recover_ts = now
                self.budget = min(self.configured_budget, int(self.budget * 1.08) + 1)

    def acquire(self, weight: int = 1) -> None:
        """يُستدعى قبل كل طلب REST: يحجز الوزن مسبقًا وينتظر عند الحاجة.
        [V9.9.1] بعد الحظر: تبريد استئناف ثم فاصل متدرج ×4 يمنع انفجار الخيوط.
        [V9.11.0] إصلاح اختناق اللوحة: النوم يتم خارج القفل — كان النوم داخل
        القفل يُسلسل كل الخيوط (بوت + لوحة) خلف بعضها ويستنزف خيوط waitress."""
        weight = max(1, int(weight))
        while True:
            now = time.time()
            resume_wait = self._effective_resume_wait(now)
            if resume_wait > 0:
                time.sleep(min(resume_wait, 5.0))
                continue
            self._maybe_recover(now)
            need = 0.0
            with self._lock:
                now = time.time()
                self._prune(now)
                used = int(sum(w for _, w in self._window))
                spacing = self.min_spacing * (self.resume_spacing_factor if now < self._ramp_until else 1.0)
                if used + weight <= self.budget:
                    gap = now - self._last_call
                    if gap >= spacing:
                        # حجز فوري بدون أي نوم داخل القفل (V9.11.0)
                        self._window.append((now, weight))
                        self._last_call = now
                        self.total_requests += 1
                        return
                    need = spacing - gap  # ننام خارج القفل ثم نعيد المحاولة
                else:
                    need = (61.0 - (now - self._window[0][0])) if self._window else 1.0
                    self.total_throttled_sec += min(need, 5.0)
            time.sleep(min(max(need, 0.05), 5.0))

    def register_ban(self, until_ms: Optional[int] = None, fallback_sec: float = 120.0) -> None:
        with self._lock:
            until = (until_ms / 1000.0) if until_ms else (time.time() + fallback_sec)
            until += 5.0  # هامش أمان فوق موعد Binance (توقيتات الخادم قد تختلف ثوانٍ)
            self.banned_until = max(self.banned_until, until)
            self.ban_count += 1
            self._last_ban_ts = time.time()
            self._ramp_until = 0.0
            self._resumed = False  # سيُفعّل التدرج عند لحظة الاستئناف
            old_budget = self.budget
            self.budget = max(self.budget_floor, int(self.budget * 0.6))
        logger.warning(f"🚫 [حارس الطلبات] حظر مؤقت من Binance — الانتظار حتى: "
                       f"{datetime.fromtimestamp(self.banned_until, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')} "
                       f"(+تبريد {self.resume_cooldown}ث) | الميزانية التكيفية: {old_budget} → {self.budget} وزن/دقيقة")

    def snapshot(self) -> Dict[str, Any]:
        now = time.time()
        with self._lock:
            self._prune(now)
            banned = self.banned_until if self.banned_until > now else None
            return {
                'used_weight_last_min': int(sum(w for _, w in self._window)),
                'budget_per_min': self.budget,
                'configured_budget': self.configured_budget,
                'banned_until': banned,
                'ban_remaining_sec': round(self.banned_until - now + self.resume_cooldown, 0) if banned else 0,
                'ban_count': self.ban_count,
                'last_error': self.last_error,
                'total_requests': self.total_requests,
                'throttled_sec_rounded': round(self.total_throttled_sec, 1),
            }

rate_guard = BinanceRateGuard(RATE_LIMIT_BUDGET_PER_MIN, API_MIN_SPACING_SEC)

# --- [تحسين V9.11.0] كاش رصيد USDT للوحة: قراءة فورية بلا نداء شبكي ---
_usdt_balance_cache: Dict[str, Any] = {'value': None, 'ts': 0.0}
_usdt_balance_lock = Lock()

def get_cached_usdt_balance(max_age_sec: float = 180.0) -> Optional[float]:
    """يعيد رصيد USDT من الكاش فقط — لا شبكة أبدًا. None إن كان غائبًا/قديمًا جدًا."""
    with _usdt_balance_lock:
        val, ts = _usdt_balance_cache['value'], _usdt_balance_cache['ts']
    if val is not None and (time.time() - ts) <= max_age_sec:
        return float(val)
    return None

def update_usdt_balance_cache() -> bool:
    """نداء شبكي واحد (وزن 5) لتحديث كاش الرصيد — يُستدعى من خيط خلفي فقط."""
    try:
        b = safe_get_asset_balance('USDT')
        val = float(b['free']) if b and 'free' in b else None
        with _usdt_balance_lock:
            _usdt_balance_cache['value'] = val
            _usdt_balance_cache['ts'] = time.time()
        return val is not None
    except Exception as e:
        logger.debug(f"[كاش الرصيد] تعذر التحديث: {e}")
        return False

def balance_refresh_loop():
    """خيط خلفي: يحدّث كاش رصيد USDT دوريًا — يستثمر فترات الحظر ولا يزاحم المسح."""
    while True:
        try:
            ban_remain = rate_guard.banned_until - time.time()
            if ban_remain > 0:
                time.sleep(min(ban_remain, 10.0)); continue
            if client:
                update_usdt_balance_cache()
        except Exception as e:
            logger.debug(f"[كاش الرصيد] خطأ: {e}")
        time.sleep(max(10, DASHBOARD_BALANCE_REFRESH_SEC))

# --- [تحسين V9.11] بوصلة اتجاه BTC: فريمات ثلاث عبر API مجاني بتكلفة وزن شبه معدومة ---
# طلب واحد لكل فريم (limit=150 شمعة، وزن 2) كل دقيقة = 6 وزن/دقيقة فقط.
# تعطي اللوحة اتجاهًا واضحًا (درجة -100..+100 + تسمية عربية) لكل فريم 15م/1س/4س.
BTC_TREND_REFRESH_SEC: int = config('BTC_TREND_REFRESH_SEC', default=60, cast=int)
BTC_TREND_TFS: List[str] = ['15m', '1h', '4h']
_TF_WEIGHTS: Dict[str, float] = {'15m': 0.2, '1h': 0.3, '4h': 0.5}

_btc_trend_cache: Dict[str, Any] = {'data': None, 'ts': 0.0}
_btc_trend_lock = Lock()

def _rsi_series(closes: pd.Series, period: int = 14) -> pd.Series:
    """RSI بطريقة Wilder المُنعّمة — يعيد سلسلة بنفس الطول.
    حالات حدّية: صعود خالص (خسارة=0) → 100، سكون تام → 50."""
    delta = closes.diff()
    gain = delta.clip(lower=0).ewm(alpha=1.0 / period, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1.0 / period, adjust=False).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    rsi = rsi.mask((loss == 0) & (gain > 0), 100.0)   # صعود خالص
    rsi = rsi.mask((gain == 0) & (loss == 0), 50.0)   # سكون تام
    return rsi.fillna(50.0)

def compute_tf_trend(closes: List[float]) -> Dict[str, Any]:
    """يحسب اتجاه فريم واحد من أسعار الإغلاق: درجة -100..+100 + تسمية عربية.
    مكونات الدرجة: محاذاة EMA9/21/50 + السعر مقابل EMA50 + MACD هيستوغرام
    + RSI (منطقة 50) + زخم 10 شموع — كلها مؤشرات قياسية مجانية بلا أي خدمة خارجية."""
    s = pd.Series([float(c) for c in closes], dtype=float)
    if len(s) < 60:
        return {'score': 0, 'label': 'بيانات غير كافية', 'arrow': '⏳', 'color': 'yellow',
                'rsi': 50.0, 'momentum_pct': 0.0, 'price': float(s.iloc[-1]) if len(s) else None}
    ema9 = s.ewm(span=9, adjust=False).mean().iloc[-1]
    ema21 = s.ewm(span=21, adjust=False).mean().iloc[-1]
    ema50 = s.ewm(span=50, adjust=False).mean().iloc[-1]
    rsi = float(_rsi_series(s).iloc[-1])
    macd_line = s.ewm(span=12, adjust=False).mean() - s.ewm(span=26, adjust=False).mean()
    macd_hist = float((macd_line - macd_line.ewm(span=9, adjust=False).mean()).iloc[-1])
    momentum_pct = float((s.iloc[-1] / s.iloc[-11] - 1.0) * 100.0) if s.iloc[-11] else 0.0
    last = float(s.iloc[-1])
    # [V9.11] قياس الفصل بين المتوسطات نسبةً إلى ضجيج السعر الفعلي (متوسط حركة الشمعة)
    # بدل المقارنة الثنائية — يمنع تصنيف السوق العرضي الضيق كـ"هابط قوي" أو "صاعد قوي"
    noise = float(s.diff().abs().tail(20).mean())
    if not noise or noise <= 0: noise = max(last * 1e-4, 1e-9)
    # مكوّنات الدرجة (مجموعها الأقصى ≈ ±100)
    score = 0.0
    score += 18.0 * float(np.tanh((ema9 - ema21) / (2.0 * noise)))    # محاذاة سريعة
    score += 18.0 * float(np.tanh((ema21 - ema50) / (2.0 * noise)))   # محاذاة هيكلية
    score += 14.0 * float(np.tanh((last - ema50) / (3.0 * noise)))    # السعر مقابل الهيكل
    score += 20.0 * float(np.tanh(macd_hist / (2.0 * noise)))         # قوة MACD
    score += max(-20.0, min(20.0, (rsi - 50.0) * 0.5))                # انحياز RSI حول 50
    score += 10.0 * float(np.tanh(momentum_pct / 2.0))                # زخم 10 شموع
    score = max(-100.0, min(100.0, score))
    if score >= 45: label, arrow, color = 'صاعد قوي', '▲▲', 'green'
    elif score >= 18: label, arrow, color = 'صاعد', '▲', 'green'
    elif score > -18: label, arrow, color = 'محايد', '▬', 'yellow'
    elif score > -45: label, arrow, color = 'هابط', '▼', 'red'
    else: label, arrow, color = 'هابط قوي', '▼▼', 'red'
    return {'score': round(score, 1), 'label': label, 'arrow': arrow, 'color': color,
            'rsi': round(rsi, 1), 'momentum_pct': round(momentum_pct, 2), 'price': last}

def _trend_label_from_score(score: float) -> Tuple[str, str, str]:
    if score >= 45: return 'صاعد قوي', '▲▲', 'green'
    if score >= 18: return 'صاعد', '▲', 'green'
    if score > -18: return 'محايد', '▬', 'yellow'
    if score > -45: return 'هابط', '▼', 'red'
    return 'هابط قوي', '▼▼', 'red'

def fetch_btc_trend_matrix(force: bool = False) -> Optional[Dict[str, Any]]:
    """يجلب شموع BTC للفريمات الثلاث (طلبات عامة مجانية وزن 2 لكل فريم) ويحسب البوصلة.
    يعيد الكاش خلال نافذة التحديث ما لم force=True. عند الفشل تبقى آخر بيانات صالحة."""
    now = time.time()
    with _btc_trend_lock:
        if not force and _btc_trend_cache['data'] and (now - _btc_trend_cache['ts']) < BTC_TREND_REFRESH_SEC:
            return _btc_trend_cache['data']
    if not client:
        return _btc_trend_cache['data']
    tfs_out: Dict[str, Any] = {}
    try:
        for tf in BTC_TREND_TFS:
            klines = safe_api_call(client.get_klines, symbol=BTC_SYMBOL, interval=tf, limit=150, weight=2)
            closes = [float(k[4]) for k in klines] if klines else []
            tfs_out[tf] = compute_tf_trend(closes)
        if not tfs_out:
            return _btc_trend_cache['data']
        overall_score = sum(float(tfs_out[tf]['score']) * w for tf, w in _TF_WEIGHTS.items() if tf in tfs_out)
        o_label, o_arrow, o_color = _trend_label_from_score(overall_score)
        bulls = sum(1 for t in tfs_out.values() if t['score'] >= 18)
        bears = sum(1 for t in tfs_out.values() if t['score'] <= -18)
        agreement = ('موحد صاعد ✓' if bulls == len(tfs_out)
                     else 'موحد هابط ✓' if bears == len(tfs_out)
                     else 'متضارب ⚠' if bulls and bears else 'انتظاري')
        price = tfs_out.get('15m', {}).get('price')
        data = {'tfs': tfs_out,
                'overall': {'score': round(overall_score, 1), 'label': o_label,
                            'arrow': o_arrow, 'color': o_color, 'agreement': agreement},
                'updated': datetime.now(timezone.utc).isoformat(),
                'stale': False}
        with _btc_trend_lock:
            _btc_trend_cache['data'] = data
            _btc_trend_cache['ts'] = time.time()
        return data
    except Exception as e:
        logger.warning(f"⚠️ [بوصلة BTC] تعذر التحديث: {e}")
        data = _btc_trend_cache['data']
        if data and (now - _btc_trend_cache['ts']) > 3 * BTC_TREND_REFRESH_SEC:
            stale = dict(data); stale['stale'] = True
            return stale
        return data

def btc_trend_loop():
    """خيط خلفي: يحدّث بوصلة BTC دوريًا — يستثمر فترات الحظر ولا يزاحم المسح."""
    time.sleep(5)  # مهلة تهيئة العميل
    while True:
        try:
            ban_remain = rate_guard.banned_until - time.time()
            if ban_remain > 0:
                time.sleep(min(ban_remain, 10.0)); continue
            if client:
                fetch_btc_trend_matrix(force=True)
        except Exception as e:
            logger.debug(f"[بوصلة BTC] خطأ: {e}")
        time.sleep(max(20, BTC_TREND_REFRESH_SEC))

BAN_UNTIL_RE = re.compile(r'banned until (\d+)', re.IGNORECASE)

def _is_rate_error(e: Exception) -> bool:
    """يكشف أخطاء تجاوز الوزن/الحظر من Binance (-1003, -1015)"""
    s = str(e)
    return ('-1003' in s or '-1015' in s or
            'Way too much request weight' in s or 'Too many requests' in s or 'banned until' in s.lower())

def safe_api_call(func, *args, weight: int = 1, max_retries: int = 4, retry_on_ban: bool = True, **kwargs):
    """غلاف موحد لكل استدعاءات REST: يحجز الوزن مسبقًا ويعيد المحاولة بعد انتهاء الحظر بدل الانهيار."""
    last_exc = None
    for attempt in range(1, max_retries + 1):
        if not client:
            raise RuntimeError("عميل Binance غير مهيأ بعد")
        rate_guard.acquire(weight)
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exc = e
            if _is_rate_error(e) and retry_on_ban and attempt < max_retries:
                m = BAN_UNTIL_RE.search(str(e))
                if m:
                    rate_guard.register_ban(int(m.group(1)))
                else:
                    rate_guard.register_ban(fallback_sec=30.0 * attempt)
                rate_guard.last_error = str(e)[:160]
                logger.warning(f"⚠️ [حارس الطلبات] تجاوز الوزن في {_sanitize_label(func)} (محاولة {attempt}/{max_retries}) — إعادة المحاولة بعد انتهاء الحظر...")
                continue
            raise
    raise last_exc

def _sanitize_label(func) -> str:
    try: return getattr(func, '__name__', 'api_call')
    except Exception: return 'api_call'

# --- أغلفة جاهزة (الوزن لكل نقطة حسب توثيق Binance الرسمي) ---
# [تحسين V9.9.1] مقدّر الوزن الفعلي لطلبات الشموع:
# python-binance يقسّم get_historical_klines داخليًا إلى صفحات limit=1000،
# ووزن كل صفحة: ≤100 → 1، ≤500 → 2، ≤1000 → 5.
# المحاسبة القديمة (وزن 2 ثابت) كانت تخفي حتى 10 أضعاف الوزن الفعلي — السبب الرئيسي لتصعيد الحظر
_KLINE_PAGE_TIERS: Tuple[Tuple[int, int], ...] = ((100, 1), (500, 2), (10**9, 5))
_INTERVAL_MINUTES: Dict[str, int] = {
    '1m': 1, '3m': 3, '5m': 5, '15m': 15, '30m': 30, '1h': 60, '2h': 120,
    '4h': 240, '6h': 360, '8h': 480, '12h': 720, '1d': 1440, '3d': 4320, '1w': 10080,
}
_LOOKBACK_UNIT_MINUTES: Dict[str, int] = {
    'minute': 1, 'min': 1, 'm': 1, 'hour': 60, 'h': 60, 'day': 1440, 'd': 1440,
    'week': 10080, 'w': 10080, 'month': 43200,
}

def estimate_klines_weight(interval: str, lookback_str: str, limit: int = 1000) -> int:
    """يقدّر الوزن الفعلي الكامل لاستدعاء get_historical_klines شاملًا كل الصفحات الداخلية."""
    try:
        interval_min = _INTERVAL_MINUTES.get(str(interval).lower())
        if not interval_min:
            return 15  # تقدير متحفظ لفريم غير معروف
        m = re.match(r'\s*(\d+)\s*([a-zA-Z]+)', str(lookback_str))
        if not m:
            return 15
        qty = int(m.group(1))
        unit_min = _LOOKBACK_UNIT_MINUTES.get(m.group(2).lower(), 1440)
        page_limit = max(1, min(int(limit), 1000))
        candles = qty * unit_min / float(interval_min)
        pages = max(1, int(math.ceil(candles / page_limit)))
        per_page = next((w for cap, w in _KLINE_PAGE_TIERS if page_limit <= cap), 5)
        return per_page * pages
    except Exception:
        return 15

def safe_get_klines(symbol: str, interval: str, lookback_str: str, **kw):
    w = estimate_klines_weight(interval, lookback_str, limit=int(kw.get('limit', 1000)))
    return safe_api_call(client.get_historical_klines, symbol, interval, lookback_str, weight=w, **kw)

def safe_get_symbol_ticker(symbol: Optional[str] = None):
    if symbol:
        return safe_api_call(lambda: client.get_symbol_ticker(symbol=symbol), weight=2)
    return safe_api_call(lambda: client.get_symbol_ticker(), weight=4)

def safe_get_24h_stats():
    """[تحسين V9.10] إحصائيات 24 ساعة لكل الرموز في طلب واحد (وزن 80)
    — أرخص طريقة لقياس حيوية السوق كله: سيولة + مدى + تغير + عدد الصفقات."""
    return safe_api_call(lambda: client.get_ticker(), weight=80)

def safe_get_exchange_info():
    return safe_api_call(client.get_exchange_info, weight=20)

def safe_get_order_book(symbol: str, limit: int = 100):
    w = 5 if limit <= 100 else 25
    return safe_api_call(client.get_order_book, symbol=symbol, limit=limit, weight=w)

def safe_get_asset_balance(asset: str):
    return safe_api_call(client.get_asset_balance, asset=asset, weight=5)

def safe_get_order(symbol: str, order_id):
    return safe_api_call(client.get_order, symbol=symbol, orderId=order_id, weight=2)

def safe_create_order(**params):
    """الأوامر الحقيقية: نحجز الوزن فقط ولا نعيد المحاولة تلقائيًا لتجنب ازدواجية الأوامر."""
    rate_guard.acquire(weight=1)
    return client.create_order(**params)

# --- قاموس أسباب الرفض باللغة العربية ---
REJECTION_REASONS_AR = {
    "Market Volatility Filter Failed": "فلتر تقلب السوق رفض الدخول",
    "Trend Strength Filter Failed": "فلتر قوة الاتجاه رفض الدخول",
    "ML Model Rejected Signal": "نموذج التعلم الآلي رفض الإشارة",
    "ML Model Load Failed": "فشل تحميل نموذج التعلم الآلي",
    "Bullish Reversal Candle Pattern Failed": "لم يظهر نمط شمعة انعكاسية صاعدة",
    "Signal Candle Volume Too Low": "حجم تداول شمعة الإشارة منخفض",
    "Order Book Filter Failed": "فشل فلتر دفتر الطلبات (Bids/Asks)",
    "Order Book Fetch Failed": "فشل جلب دفتر الطلبات",
    "Invalid Position Size": "حجم الصفقة غير صالح",
    "Lot Size Adjustment Failed": "فشل ضبط حجم العقد",
    "Min Notional Filter": "قيمة الصفقة أقل من الحد الأدنى",
    "Insufficient Balance": "الرصيد غير كافٍ",
    "Insufficient data for TP/SL calculation": "بيانات غير كافية لحساب TP/SL",
    "Insufficient Historical Data": "بيانات تاريخية غير كافية للفحص",
    "HTF Trend Confirmation Failed": "فشل تأكيد الترند على الفريم الأعلى",
    "Short-Term Momentum Filter Failed": "فشل فلتر الزخم قصير الأجل",
    "Bullish Momentum Strategy Conditions Not Met": "شروط استراتيجية الزخم الصعودي لم تتحقق",
    "BB_Stoch Strategy Conditions Not Met": "شروط استراتيجية BB+Stoch لم تتحقق",
    "MACD_EMA Strategy Conditions Not Met": "شروط استراتيجية MACD+EMA لم تتحقق",
    "EMA_RSI Strategy Conditions Not Met": "شروط استراتيجية EMA+RSI لم تتحقق",
    "Pullback Strategy Conditions Not Met": "شروط استراتيجية Pullback لم تتحقق",
    "BB Squeeze Strategy Conditions Not Met": "شروط استراتيجية BB Squeeze لم تتحقق",
    "SR Breakout Strategy Conditions Not Met": "شروط استراتيجية اختراق الدعم/المقاومة لم تتحقق",
    "Price Peak Avoidance": "تجنب الدخول عند قمة آخر 24 ساعة (استراتيجية ارتدادية)",
    "Daily Loss Limit": "قاطع الحماية: تم إيقاف فتح صفقات جديدة بسبب تجاوز حد الخسارة اليومي",
}


# --- دالة إرسال رسائل تليجرام ---
def send_telegram_message(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("[تليجرام] Token أو Chat ID غير معين، تم تخطي الإرسال.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': 'Markdown'}
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        logger.info("[تليجرام] تم إرسال الرسالة بنجاح.")
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ [تليجرام] فشل إرسال الرسالة: {e}")

# --- دوال تهيئة الخدمات ---
def init_db(retries: int = 5, delay: int = 5) -> None:
    global conn
    logger.info("[قاعدة البيانات] تهيئة الاتصال...")
    db_url_to_use = DB_URL
    if 'postgres' in db_url_to_use and 'sslmode' not in db_url_to_use:
        db_url_to_use += f"{'?' if '?' not in db_url_to_use else '&'}sslmode=require"
    for attempt in range(retries):
        try:
            conn = psycopg2.connect(db_url_to_use, connect_timeout=15, cursor_factory=RealDictCursor)
            conn.autocommit = False
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS signals (
                        id SERIAL PRIMARY KEY, symbol TEXT NOT NULL, entry_price DOUBLE PRECISION NOT NULL,
                        target_price DOUBLE PRECISION NOT NULL, stop_loss DOUBLE PRECISION NOT NULL,
                        status TEXT DEFAULT 'open', closing_price DOUBLE PRECISION, closed_at TIMESTAMP,
                        profit_percentage DOUBLE PRECISION, strategy_name TEXT, signal_details JSONB,
                        current_peak_price DOUBLE PRECISION, is_real_trade BOOLEAN DEFAULT FALSE,
                        quantity DOUBLE PRECISION, order_id TEXT, closing_reason TEXT
                    );
                """)
                cur.execute("ALTER TABLE signals ADD COLUMN IF NOT EXISTS journey_state JSONB;")
                cur.execute("ALTER TABLE signals ADD COLUMN IF NOT EXISTS original_quantity DOUBLE PRECISION;")
                cur.execute("ALTER TABLE signals ADD COLUMN IF NOT EXISTS rr_ratio DOUBLE PRECISION;")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_signals_status ON signals (status);")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS notifications (
                        id SERIAL PRIMARY KEY, timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        type TEXT NOT NULL, message TEXT NOT NULL, is_read BOOLEAN DEFAULT FALSE
                    );
                """)
            conn.commit()
            logger.info("✅ [قاعدة البيانات] الاتصال وتحديث المخطط بنجاح.")
            return
        except Exception as e:
            logger.error(f"❌ [قاعدة البيانات] خطأ أثناء التهيئة (محاولة {attempt + 1}/{retries}): {e}")
            if conn: conn.rollback()
            if attempt < retries - 1: time.sleep(delay)
            else: logger.critical("❌ [قاعدة البيانات] فشل الاتصال.")

def check_db_connection() -> bool:
    global conn
    if conn is None or conn.closed != 0:
        logger.warning("[قاعدة البيانات] الاتصال مغلق، محاولة إعادة الاتصال...")
        init_db()
    try:
        if conn and conn.closed == 0:
            with conn.cursor() as cur: cur.execute("SELECT 1;")
            return True
        return False
    except (OperationalError, InterfaceError) as e:
        logger.error(f"❌ [قاعدة البيانات] فقدان الاتصال: {e}. إعادة الاتصال...")
        try:
            init_db()
            return conn is not None and conn.closed == 0
        except Exception as retry_e:
            logger.error(f"❌ [قاعدة البيانات] فشل إعادة الاتصال: {retry_e}")
            return False

def log_and_notify(level: str, message: str, notification_type: str):
    log_methods = {'info': logger.info, 'warning': logger.warning, 'error': logger.error, 'critical': logger.critical}
    log_methods.get(level.lower(), logger.info)(message)
    if not check_db_connection() or not conn: return
    try:
        new_notification = {"timestamp": datetime.now(timezone.utc).isoformat(), "type": notification_type, "message": message}
        with notifications_lock: notifications_cache.appendleft(new_notification)
        with conn.cursor() as cur: cur.execute("INSERT INTO notifications (type, message) VALUES (%s, %s);", (notification_type, message))
        conn.commit()
    except Exception as e:
        logger.error(f"❌ [قاعدة البيانات] فشل حفظ الإشعار: {e}")
        if conn: conn.rollback()

def log_rejection(symbol: str, reason_key: str, details: Optional[Dict] = None):
    reason_ar = REJECTION_REASONS_AR.get(reason_key, reason_key)
    log_message = f"🚫 [{symbol}] تم الرفض | السبب: {reason_ar} | تفاصيل: {details or {}}"
    logger.info(log_message)
    with rejection_logs_lock:
        rejection_logs_cache.appendleft({
            "timestamp": datetime.now(timezone.utc).isoformat(), "symbol": symbol,
            "reason": reason_ar, "details": json.loads(json.dumps(details, cls=NpEncoder)) or {}
        })

class InMemoryRedis:
    """[تحسين V9.8] بديل بسيط في الذاكرة عندما لا يتوفر خادم Redis.
    يدعم العمليات المستخدمة في البوت فقط (hset/hget/hgetall/ping)،
    ويعمل بشكل ممتاز على الخطة المجانية في Render حيث الأسعار مخزنة داخل نفس العملية.
    """
    def __init__(self):
        self._data: Dict[str, Dict[str, str]] = {}

    def _hash(self, name: str) -> Dict[str, str]:
        return self._data.setdefault(name, {})

    def hset(self, name, key=None, value=None, mapping=None):
        h = self._hash(str(name))
        if mapping:
            h.update({str(k): str(v) for k, v in mapping.items()})
        elif key is not None:
            h[str(key)] = str(value)
        return 1

    def hget(self, name, key):
        return self._hash(str(name)).get(str(key))

    def hgetall(self, name):
        return dict(self._hash(str(name)))

    def ping(self):
        return True

def init_redis() -> None:
    global redis_client
    logger.info("[Redis] تهيئة الاتصال...")
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=5)
        redis_client.ping()
        logger.info("✅ [Redis] تم الاتصال بنجاح.")
    except Exception as e:
        # [تحسين V9.8] لم يعد البوت ينهار عند غياب Redis — يتحول تلقائيًا للذاكرة
        logger.warning(f"⚠️ [Redis] تعذر الاتصال ({e}). تشغيل البديل في الذاكرة (مناسب للخطة المجانية على Render).")
        redis_client = InMemoryRedis()
        logger.info("✅ [Redis] تم تفعيل البديل في الذاكرة بنجاح.")

def get_exchange_info_map() -> None:
    global exchange_info_map
    if not client: return
    logger.info("ℹ️ [معلومات المنصة] جاري جلب قواعد التداول...")
    try:
        info = safe_get_exchange_info()
        exchange_info_map = {s['symbol']: s for s in info['symbols']}
        logger.info(f"✅ [معلومات المنصة] تم تحميل القواعد لـ {len(exchange_info_map)} عملة.")
    except Exception as e:
        logger.error(f"❌ [معلومات المنصة] فشل جلب المعلومات: {e}")

def get_validated_symbols(filename: str = 'crypto_list.txt') -> List[str]:
    if not client: return []
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(script_dir, filename)

        if not os.path.exists(file_path):
            logger.critical(f"❌ [التحقق من الرموز] ملف العملات '{filename}' غير موجود!")
            return []

        with open(file_path, 'r', encoding='utf-8') as f:
            raw_symbols = {line.strip().upper() for line in f if line.strip() and not line.startswith('#')}

        if not raw_symbols:
            logger.warning(f"⚠️ [التحقق من الرموز] ملف العملات '{filename}' فارغ.")
            return []

        formatted = {f"{s}USDT" if not s.endswith('USDT') else s for s in raw_symbols}
        if not exchange_info_map: get_exchange_info_map()

        active = {s for s, info in exchange_info_map.items() if info.get('quoteAsset') == 'USDT' and info.get('status') == 'TRADING'}

        validated = sorted(list(formatted.intersection(active)))

        logger.info(f"✅ [التحقق من الرموز] تم العثور على {len(validated)} عملة صالحة للتداول.")
        if not validated:
             logger.warning(f"⚠️ [التحقق من الرموز] لا توجد عملات متطابقة في ملفك مع المتاح في المنصة.")
        else:
            logger.info(f"🔍 [التحقق من الرموز] عينة من العملات للمراقبة: {validated[:5]}")

        return validated
    except Exception as e:
        logger.error(f"❌ [التحقق من الرموز] خطأ: {e}", exc_info=True)
        return []


# ============================================================
# [تحسين V9.10] الكشف الديناميكي عن العملات الأكثر حيوية (Dynamic Universe)
# طلب واحد كل 30 دقيقة (وزن 80) يعطي إحصائيات 24 ساعة لكل العملات ثم:
#   ترشيح: USDT فقط + حالة TRADING + استبعاد المستقرة والرافعة + سيولة ≥ 10M + مدى ≥ 1.5%
#   ترتيب: 45% السيولة + 35% التقلب (المدى اليومي = صيد الانفجارات) + 20% الانفجار (|تغير 24س|)
# مع بديل آمن: القائمة الثابتة من crypto_list.txt عند أي فشل
# ============================================================
_LEVERAGED_PATTERNS = ('UP', 'DOWN', 'BULL', 'BEAR')
_STABLE_BASES = {'USDC', 'FDUSD', 'TUSD', 'DAI', 'USDP', 'BUSD', 'EUR', 'AEUR',
                 'PYUSD', 'USD1', 'USDE', 'XUSD', 'USDT'}

def compute_dynamic_universe(size: Optional[int] = None) -> Tuple[List[str], Dict[str, Any]]:
    """يرشّح كل أزواج USDT ويرتّبها بنقاط الحيوية (سيولة + تقلب + انفجار) ويختار الأعلى."""
    size = max(5, int(size or DYNAMIC_UNIVERSE_SIZE))
    if not client:
        return [], {'reason': 'client غير مهيأ'}
    if not exchange_info_map:
        get_exchange_info_map()
    trading_usdt = {s for s, info in exchange_info_map.items()
                    if info.get('quoteAsset') == 'USDT' and info.get('status') == 'TRADING'}
    rows: List[Dict[str, Any]] = []
    for t in safe_get_24h_stats():
        sym = str(t.get('symbol', '') or '')
        if sym not in trading_usdt:
            continue
        base = sym[:-4]
        if base in _STABLE_BASES:
            continue
        if any(base.endswith(p) for p in _LEVERAGED_PATTERNS):
            continue
        try:
            last = float(t.get('lastPrice') or 0)
            high = float(t.get('highPrice') or 0)
            low = float(t.get('lowPrice') or 0)
            qvol = float(t.get('quoteVolume') or 0)
            chg = abs(float(t.get('priceChangePercent') or 0))
        except (TypeError, ValueError):
            continue
        if last <= 0 or high <= 0 or low <= 0 or high < low:
            continue
        if qvol < DYNAMIC_UNIVERSE_MIN_QUOTE_VOLUME:
            continue
        range_pct = (high - low) / low * 100.0
        if range_pct < DYNAMIC_UNIVERSE_MIN_RANGE_PCT:
            continue
        rows.append({'symbol': sym, 'qvol': qvol, 'range_pct': range_pct, 'chg': chg})
    if not rows:
        return [], {'reason': 'لا مرشحين بعد الفلترة (سيولة/تقلب)'}
    def _ranks(key: str) -> Dict[int, float]:
        order = sorted(rows, key=lambda r: r[key], reverse=True)
        n = max(1, len(order) - 1)
        return {id(r): (n - i) / n for i, r in enumerate(order)}
    vol_r, rng_r, chg_r = _ranks('qvol'), _ranks('range_pct'), _ranks('chg')
    for r in rows:
        r['score'] = round(100 * (0.45 * vol_r[id(r)] + 0.35 * rng_r[id(r)] + 0.20 * chg_r[id(r)]), 2)
    rows.sort(key=lambda r: r['score'], reverse=True)
    top = rows[:size]
    picked = [r['symbol'] for r in top]
    meta = {
        'candidates': len(rows),
        'top_preview': [{'symbol': r['symbol'], 'score': r['score'],
                         'qvol_musd': round(r['qvol'] / 1e6, 1),
                         'range_pct': round(r['range_pct'], 2),
                         'chg24h': round(r['chg'], 2)} for r in top[:8]],
    }
    logger.info(f"⚡ [القائمة الديناميكية] رُشِّح {len(rows)} عملة حيوية — اختيار أفضل {len(picked)}: {picked}")
    try:
        logger.info("⚡ [الأعلى حيوية] " + " | ".join(
            f"{p['symbol']} (نقاط {p['score']}, سيولة {p['qvol_musd']}M, مدى {p['range_pct']}%, تغير {p['chg24h']}%)"
            for p in meta['top_preview'][:5]))
    except Exception:
        pass
    return picked, meta

def refresh_universe_if_needed(force: bool = False) -> None:
    """يحدّث قائمة المسح للعملات الأكثر حيوية كل DYNAMIC_UNIVERSE_REFRESH_MIN دقيقة.
    عند أي فشل: تبقى القائمة الحالية كما هي، وإن لم توجد أصلًا يُستخدم البديل الثابت."""
    global validated_symbols_to_scan, universe_last_refresh, universe_source, universe_meta
    if not USE_DYNAMIC_UNIVERSE:
        return
    with universe_lock:
        if not force and time.time() - universe_last_refresh < DYNAMIC_UNIVERSE_REFRESH_MIN * 60:
            return
    reason = ''
    try:
        picked, meta = compute_dynamic_universe()
        if picked:
            with universe_lock:
                validated_symbols_to_scan = picked
                universe_last_refresh = time.time()
                universe_source = 'dynamic'
                universe_meta = meta
            return
        reason = meta.get('reason', 'غير معروف')
    except Exception as e:
        reason = str(e)[:120]
        logger.warning(f"⚠️ [القائمة الديناميكية] فشل الجلب: {reason}")
    # بديل آمن: القائمة الثابتة — ولا نلمس القائمة الحالية إن كانت تعمل
    with universe_lock:
        if not validated_symbols_to_scan and _static_fallback_symbols:
            validated_symbols_to_scan = _static_fallback_symbols[:]
        if validated_symbols_to_scan:
            if universe_source != 'dynamic' or universe_last_refresh == 0.0:
                universe_source = 'static_fallback'
            universe_last_refresh = time.time()
            universe_meta = {'reason': f'fallback: {reason}'}


# --- دوال جلب البيانات وحساب المؤشرات ---
def fetch_historical_data(symbol: str, interval: str, days: int) -> Optional[pd.DataFrame]:
    if not client: return None
    try:
        lookback_str = f"{days + 50} day" if 'd' in interval.lower() else f"{days * 24 + 200} hour"
        
        klines = safe_get_klines(symbol, interval, lookback_str)
        if not klines: return None
        cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'trades', 'taker_buy_base', 'taker_buy_quote', 'ignore']
        df = pd.DataFrame(klines, columns=cols)
        required_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'taker_buy_base']
        df = df[required_cols]
        numeric_cols = {'open': 'float', 'high': 'float', 'low': 'float', 'close': 'float', 'volume': 'float', 'quote_volume': 'float', 'taker_buy_base': 'float'}
        df = df.astype(numeric_cols)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        df.set_index('timestamp', inplace=True)
        return df.dropna()
    except Exception as e:
        logger.error(f"❌ [جلب البيانات] خطأ في جلب البيانات التاريخية لـ {symbol} ({interval}): {e}")
        return None

def check_price_peak_filter(df: pd.DataFrame, current_price: float) -> bool:
    """[مفعّل في V9.8] فلتر تجنب الدخول عند القمم لاستراتيجيات الارتداد (Mean-Reversion).
    يمنع الدخول إذا كان السعر الحالي عند قمة آخر 24 ساعة (96 شمعة على فريم 15 دقيقة)،
    لأن شراء الارتداد عند القمة له نسبة نجاح منخفضة.
    ملاحظة: لا يطبق على استراتيجيات الاختراق لأنها تشتري عند القمم بطبيعتها.
    """
    try:
        if len(df) < 96:
            return True  # بيانات غير كافية، لا نرفض
        recent_peak_24h = df['high'].iloc[-96:].max()
        if current_price >= recent_peak_24h * 0.999:
            log_rejection(getattr(df, 'name', 'UNKNOWN'), "Price Peak Avoidance", {
                "current_price": current_price, "recent_peak_24h": float(recent_peak_24h)
            })
            return False
        return True
    except Exception as e:
        logger.warning(f"⚠️ [فلتر القمم] خطأ غير متوقع: {e}")
        return True

def calculate_advanced_momentum_features(df: pd.DataFrame) -> pd.DataFrame:
    highest_high = df['high'].rolling(window=14).max()
    lowest_low = df['low'].rolling(window=14).min()
    df['williams_r'] = -100 * (highest_high - df['close']) / (highest_high - lowest_low).replace(0, 1e-9)
    df['stoch_k'] = 100 * (df['close'] - lowest_low) / (highest_high - lowest_low).replace(0, 1e-9)
    df['stoch_d'] = df['stoch_k'].rolling(3).mean()
    exp1 = df['close'].ewm(span=12, adjust=False).mean()
    exp2 = df['close'].ewm(span=26, adjust=False).mean()
    df['macd'] = exp1 - exp2
    df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
    df['macd_histogram'] = df['macd'] - df['macd_signal']
    bb_period = 20
    df['bb_middle'] = df['close'].rolling(window=bb_period).mean()
    bb_std = df['close'].rolling(window=bb_period).std()
    df['bb_upper'] = df['bb_middle'] + (bb_std * 2)
    df['bb_lower'] = df['bb_middle'] - (bb_std * 2)
    df['bb_position'] = (df['close'] - df['bb_lower']) / (df['bb_upper'] - df['bb_lower']).replace(0, 1e-9)
    df['kc_middle'] = df['close'].ewm(span=20, adjust=False).mean()
    if 'atr' in df.columns:
        df['kc_upper'] = df['kc_middle'] + (df['atr'] * 1.5)
        df['kc_lower'] = df['kc_middle'] - (df['atr'] * 1.5)
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    money_flow = typical_price * df['volume']
    positive_flow = money_flow.where(typical_price > typical_price.shift(1), 0).rolling(14).sum()
    negative_flow = money_flow.where(typical_price < typical_price.shift(1), 0).rolling(14).sum()
    money_ratio = positive_flow / negative_flow.replace(0, 1e-9)
    df['mfi'] = 100 - (100 / (1 + money_ratio))
    return df

def calculate_market_microstructure_features(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = ['taker_buy_base', 'volume', 'quote_volume', 'high', 'low', 'open', 'close']
    if not all(col in df.columns for col in required_cols): return df
    df['buy_pressure'] = df['taker_buy_base'] / df['volume'].replace(0, 1e-9)
    volume_ma = df['volume'].rolling(20).mean()
    df['volume_ratio'] = df['volume'] / volume_ma.replace(0, 1e-9)
    df['price_impact'] = df['quote_volume'] / df['volume'].replace(0, 1e-9)
    log_hl = np.log(df['high'] / df['low'].replace(0, 1e-9))
    log_co = np.log(df['close'] / df['open'].replace(0, 1e-9))
    gk_vol_sq = (0.5 * (log_hl ** 2) - (2 * np.log(2) - 1) * (log_co ** 2)).clip(lower=0)
    df['garman_klass_vol'] = np.sqrt(gk_vol_sq)
    log_hc = np.log(df['high'] / df['close'].replace(0, 1e-9))
    log_ho = np.log(df['high'] / df['open'].replace(0, 1e-9))
    log_lc = np.log(df['low'] / df['close'].replace(0, 1e-9))
    log_lo = np.log(df['low'] / df['open'].replace(0, 1e-9))
    rs_vol_sq = (log_hc * log_ho + log_lc * log_lo).clip(lower=0)
    df['rogers_satchell_vol'] = np.sqrt(rs_vol_sq)
    return df

def calculate_advanced_volatility_features(df: pd.DataFrame) -> pd.DataFrame:
    high_low = df['high'] - df['low']
    ema_high_low = high_low.ewm(span=10, adjust=False).mean()
    ema_high_low_shifted = ema_high_low.shift(10)
    df['chaikin_volatility'] = (ema_high_low - ema_high_low_shifted) / ema_high_low_shifted.replace(0, 1e-9) * 100
    period = 14
    max_close = df['close'].rolling(window=period).max()
    percentage_drawdown = 100 * (df['close'] - max_close) / max_close.replace(0, 1e-9)
    df['ulcer_index'] = np.sqrt((percentage_drawdown ** 2).rolling(window=period).mean())
    if 'atr' not in df.columns: return df
    high_low_tr = df['high'] - df['low']
    high_close_prev = (df['high'] - df['close'].shift()).abs()
    low_close_prev = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([high_low_tr, high_close_prev, low_close_prev], axis=1).max(axis=1)
    for p in [5, 10, 20]:
        atr_p = tr.ewm(span=p, adjust=False).mean()
        df[f'atr_ratio_{p}'] = df['atr'] / atr_p.replace(0, 1e-9)
    return df

def calculate_temporal_features(df: pd.DataFrame) -> pd.DataFrame:
    df['hour_sin'] = np.sin(2 * np.pi * df.index.hour / 24)
    df['hour_cos'] = np.cos(2 * np.pi * df.index.hour / 24)
    df['day_of_week'] = df.index.dayofweek
    df['is_weekend'] = (df.index.dayofweek >= 5).astype(int)
    df['asia_session'] = ((df.index.hour >= 0) & (df.index.hour < 8)).astype(int)
    df['london_session'] = ((df.index.hour >= 8) & (df.index.hour < 16)).astype(int)
    df['ny_session'] = ((df.index.hour >= 13) & (df.index.hour < 21)).astype(int)
    df['month_sin'] = np.sin(2 * np.pi * df.index.month / 12)
    df['month_cos'] = np.cos(2 * np.pi * df.index.month / 12)
    return df

def calculate_supertrend(df: pd.DataFrame, atr_period: int, multiplier: float) -> pd.DataFrame:
    high = df['high']
    low = df['low']
    close = df['close']
    high_low = high - low
    high_close_prev = np.abs(high - close.shift(1))
    low_close_prev = np.abs(low - close.shift(1))
    tr = pd.concat([high_low, high_close_prev, low_close_prev], axis=1).max(axis=1)
    atr = tr.ewm(com=atr_period - 1, min_periods=atr_period, adjust=False).mean()
    hl2 = (high + low) / 2
    final_upper_band = upper_band = hl2 + (multiplier * atr)
    final_lower_band = lower_band = hl2 - (multiplier * atr)
    supertrend = pd.Series(np.nan, index=df.index)
    supertrend_direction = pd.Series(np.nan, index=df.index)
    for i in range(1, len(df)):
        curr, prev = i, i - 1
        if close[curr] > final_upper_band[prev]:
            supertrend_direction[curr] = 1
        elif close[curr] < final_lower_band[prev]:
            supertrend_direction[curr] = -1
        else:
            supertrend_direction[curr] = supertrend_direction[prev]
            if supertrend_direction[curr] == -1 and final_upper_band[curr] < final_upper_band[prev]:
                final_upper_band[curr] = final_upper_band[curr]
            if supertrend_direction[curr] == 1 and final_lower_band[curr] > final_lower_band[prev]:
                final_lower_band[curr] = final_lower_band[curr]
        if supertrend_direction[curr] == 1:
            supertrend[curr] = final_lower_band[curr]
        else:
            supertrend[curr] = final_upper_band[curr]
    df['supertrend'] = supertrend
    df['supertrend_direction'] = supertrend_direction
    return df

def calculate_all_features(df: pd.DataFrame, btc_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    df_calc = df.copy()
    df_calc['ema_9'] = df_calc['close'].ewm(span=9, adjust=False).mean()
    df_calc['ema_21'] = df_calc['close'].ewm(span=21, adjust=False).mean()
    df_calc['sma_50'] = df_calc['close'].rolling(window=50).mean()
    df_calc['sma_200'] = df_calc['close'].rolling(window=200).mean()
    df_calc['volume_sma_20'] = df_calc['volume'].rolling(window=20).mean()
    df_calc['ema_50'] = df_calc['close'].ewm(span=EMA_SLOW_PERIOD, adjust=False).mean()
    df_calc['ema_100'] = df_calc['close'].ewm(span=100, adjust=False).mean()
    high_low = df_calc['high'] - df_calc['low']
    high_close = (df_calc['high'] - df_calc['close'].shift()).abs()
    low_close = (df_calc['low'] - df_calc['close'].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1, skipna=False)
    df_calc['atr'] = tr.ewm(span=ATR_PERIOD, adjust=False).mean()
    up_move = df_calc['high'].diff()
    down_move = -df_calc['low'].diff()
    plus_dm = pd.Series(np.where((up_move > down_move) & (up_move > 0), up_move, 0.0), index=df_calc.index)
    minus_dm = pd.Series(np.where((down_move > up_move) & (down_move > 0), down_move, 0.0), index=df_calc.index)
    plus_di = 100 * plus_dm.ewm(span=ADX_PERIOD, adjust=False).mean() / df_calc['atr'].replace(0, 1e-9)
    minus_di = 100 * minus_dm.ewm(span=ADX_PERIOD, adjust=False).mean() / df_calc['atr'].replace(0, 1e-9)
    dx = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, 1e-9))
    df_calc['adx'] = dx.ewm(span=ADX_PERIOD, adjust=False).mean()
    df_calc['plus_di'] = plus_di
    df_calc['minus_di'] = minus_di
    delta = df_calc['close'].diff()
    gain = delta.clip(lower=0).ewm(com=RSI_PERIOD - 1, adjust=False).mean()
    loss = -delta.clip(upper=0).ewm(com=RSI_PERIOD - 1, adjust=False).mean()
    df_calc['rsi'] = 100 - (100 / (1 + (gain / loss.replace(0, 1e-9))))
    STOCH_RSI_PERIOD = 14
    STOCH_RSI_K_PERIOD = 3
    STOCH_RSI_D_PERIOD = 3
    rsi = df_calc['rsi']
    stoch_rsi_val = (rsi - rsi.rolling(STOCH_RSI_PERIOD).min()) / (rsi.rolling(STOCH_RSI_PERIOD).max() - rsi.rolling(STOCH_RSI_PERIOD).min()).replace(0, 1e-9)
    df_calc['stoch_rsi_k'] = stoch_rsi_val.rolling(STOCH_RSI_K_PERIOD).mean() * 100
    df_calc['stoch_rsi_d'] = df_calc['stoch_rsi_k'].rolling(STOCH_RSI_D_PERIOD).mean()
    df_calc['relative_volume'] = df_calc['volume'] / (df_calc['volume'].rolling(window=REL_VOL_PERIOD, min_periods=1).mean() + 1e-9)
    df_calc['price_vs_ema50'] = (df_calc['close'] / df_calc['ema_50']) - 1
    df_calc['price_vs_ema200'] = (df_calc['close'] / df_calc['close'].ewm(span=200, adjust=False).mean()) - 1
    if btc_df is not None and not btc_df.empty:
        asset_returns = df_calc['close'].pct_change()
        if 'btc_returns' not in btc_df.columns:
            btc_df['btc_returns'] = btc_df['close'].pct_change()
        merged_df = pd.merge(df_calc, btc_df[['btc_returns']], left_index=True, right_index=True, how='left').fillna(0)
        df_calc['btc_correlation'] = asset_returns.rolling(window=BTC_CORR_PERIOD).corr(merged_df['btc_returns'])
    else:
        df_calc['btc_correlation'] = 0.0
    df_calc = calculate_advanced_momentum_features(df_calc)
    df_calc['bb_width'] = (df_calc['bb_upper'] - df_calc['bb_lower']) / df_calc['bb_middle'].replace(0, 1e-9)
    df_calc = calculate_market_microstructure_features(df_calc)
    df_calc = calculate_advanced_volatility_features(df_calc)
    df_calc = calculate_temporal_features(df_calc)
    df_calc = calculate_supertrend(df_calc, SUPERTREND_ATR_PERIOD, SUPERTREND_MULTIPLIER)
    df_calc[f'roc_{MOMENTUM_PERIOD}'] = (df_calc['close'] / df_calc['close'].shift(MOMENTUM_PERIOD) - 1) * 100
    df_calc['roc_acceleration'] = df_calc[f'roc_{MOMENTUM_PERIOD}'].diff()
    ema_slope = df_calc['close'].ewm(span=EMA_SLOPE_PERIOD, adjust=False).mean()
    df_calc[f'ema_slope_{EMA_SLOPE_PERIOD}'] = (ema_slope - ema_slope.shift(1)) / ema_slope.shift(1).replace(0, 1e-9) * 100
    # [تحسين V9.8] تجنب deprecation في pandas الحديث (astype with errors='ignore')
    numeric_cols = df_calc.select_dtypes(include=[np.number]).columns
    df_calc[numeric_cols] = df_calc[numeric_cols].astype('float32')
    return df_calc


def get_session_state() -> Tuple[List[str], str, str]:
    sessions = {"London": (8, 17), "New York": (13, 22), "Tokyo": (0, 9)}
    active_sessions = []
    now_utc = datetime.now(timezone.utc)
    current_hour = now_utc.hour
    if now_utc.weekday() >= 5: return [], "WEEKEND", "عطلة نهاية الأسبوع"
    
    for session, (start, end) in sessions.items():
        if start > end:
            if current_hour >= start or current_hour < end:
                active_sessions.append(session)
        elif start <= current_hour < end:
            active_sessions.append(session)

    if "London" in active_sessions and "New York" in active_sessions:
        return active_sessions, "HIGH_LIQUIDITY", "تداخل لندن/نيويورك"
    elif len(active_sessions) >= 1:
        return active_sessions, "NORMAL_LIQUIDITY", f"{', '.join(active_sessions)}"
    else:
        return [], "LOW_LIQUIDITY", "خارج أوقات الذروة"

def get_btc_data_for_bot() -> Optional[pd.DataFrame]:
    btc_data = fetch_historical_data(BTC_SYMBOL, SIGNAL_GENERATION_TIMEFRAME, SIGNAL_GENERATION_LOOKBACK_DAYS)
    if btc_data is not None: btc_data['btc_returns'] = btc_data['close'].pct_change()
    return btc_data

def load_open_signals_to_cache():
    if not check_db_connection() or not conn: return
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM signals WHERE status IN ('open', 'updated');")
            open_signals = cur.fetchall()
            with signal_cache_lock:
                open_signals_cache.clear()
                for signal in open_signals: open_signals_cache[signal['symbol']] = dict(signal)
            logger.info(f"✅ [تحميل] تم تحميل {len(open_signals)} صفقة مفتوحة إلى الذاكرة المؤقتة.")
    except Exception as e:
        logger.error(f"❌ [تحميل] فشل تحميل الصفقات المفتوحة: {e}")

def load_notifications_to_cache():
    if not check_db_connection() or not conn: return
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM notifications ORDER BY timestamp DESC LIMIT 50;")
            recent = cur.fetchall()
            with notifications_lock:
                notifications_cache.clear()
                for n in reversed(recent):
                    n['timestamp'] = n['timestamp'].isoformat()
                    notifications_cache.appendleft(dict(n))
            logger.info(f"✅ [تحميل] تم تحميل {len(notifications_cache)} إشعار إلى الذاكرة المؤقتة.")
    except Exception as e:
        logger.error(f"❌ [تحميل] فشل تحميل الإشعارات: {e}")

# ---------------------- منطق التداول والفلاتر ----------------------

# --- [إضافة] فلاتر التأكيد الجديدة ---
def check_market_volatility_filter(df: pd.DataFrame) -> bool:
    """فلتر لتجنب التداول في فترات التقلب الشديد أو المنخفض جدًا"""
    if len(df) < 50:
        return False
    
    last = df.iloc[-1]
    # التأكد من وجود الأعمدة المطلوبة
    if 'atr' not in last or 'close' not in last or last['close'] == 0:
        return False # لا يمكن الحساب، نفترض أنه غير صالح
        
    atr_percent = (last['atr'] / last['close']) * 100
    
    # تجنب التداول عندما يكون التقلب منخفض جدًا أو مرتفع جدًا
    if atr_percent < 0.5 or atr_percent > 5.0:
        log_rejection(df.name, "Market Volatility Filter Failed", {"atr_percent": f"{atr_percent:.2f}"})
        return False
    
    return True

def check_trend_strength_filter(df: pd.DataFrame) -> bool:
    """فلتر لتأكيد قوة الاتجاه الحالي"""
    if len(df) < 50:
        return False
        
    last = df.iloc[-1]
    
    # التأكد من وجود الأعمدة المطلوبة
    if 'adx' not in last or f'roc_{MOMENTUM_PERIOD}' not in last:
        return False # لا يمكن الحساب، نفترض أنه غير صالح

    # تجنب التداول في الأسواق الجانبية
    if last['adx'] < 18:
        log_rejection(df.name, "Trend Strength Filter Failed", {"reason": "ADX too low", "adx": f"{last['adx']:.2f}"})
        return False
        
    # التأكد من وجود زخم كافٍ
    if abs(last[f'roc_{MOMENTUM_PERIOD}']) < 0.5:
        log_rejection(df.name, "Trend Strength Filter Failed", {"reason": "ROC too low", "roc_10": f"{last[f'roc_{MOMENTUM_PERIOD}']:.2f}"})
        return False
        
    return True


# --- [تحسين] دوال منطق الاستراتيجيات (تم تحسينها) ---
def check_bb_stoch_strategy_enhanced(df: pd.DataFrame) -> bool:
    """استراتيجية BB+Stoch المحسنة مع فلاتر إضافية"""
    if len(df) < 21: 
        return False
        
    last, prev = df.iloc[-1], df.iloc[-2]
    
    # الشروط الأساسية
    price_touch_bb = last['low'] <= (last['bb_lower'] * 1.002)
    stoch_cross_up = prev['stoch_rsi_k'] < prev['stoch_rsi_d'] and last['stoch_rsi_k'] > last['stoch_rsi_d']
    oversold_area = last['stoch_rsi_k'] < 35 and last['stoch_rsi_d'] < 35
    
    # فلاتر إضافية
    volume_spike = last['volume'] > last['volume_sma_20'] * 1.2
    with market_state_lock:
        trend_ok = "DOWNTREND" not in current_market_state.get("overall_regime", "UNCERTAIN")
    
    # فلتر جديد: تجنب الإشارات في الأسواق الجانبية
    bb_width_ok = last['bb_width'] > 0.02  # تجنب الأسواق ذات النطاق الضيق جدًا
    
    # فلتر جديد: التأكد من أن السعر ليس بعيدًا جدًا عن المتوسطات
    price_not_oversold = last['rsi'] > 25
    
    conditions = {
        "price_touch_bb": price_touch_bb,
        "stoch_cross_up": stoch_cross_up,
        "oversold_area": oversold_area,
        "volume_spike": volume_spike,
        "trend_ok": trend_ok,
        "bb_width_ok": bb_width_ok,
        "price_not_oversold": price_not_oversold
    }
    
    if all(conditions.values()):
        logger.info(f"  -> [{df.name}] ✅ إشارة BB+Stoch (معززة).")
        return True
    
    # تسجيل الشروط الفاشلة للمساعدة في التحليل
    failed_conditions = {k: v for k, v in conditions.items() if not v}
    if any(failed_conditions): # نسجل فقط إذا كانت هناك إشارة محتملة لكنها فشلت
        if price_touch_bb or stoch_cross_up:
             log_rejection(df.name, "BB_Stoch Strategy Conditions Not Met", {"failed": list(failed_conditions.keys())})
    return False

def check_macd_ema_strategy(df: pd.DataFrame) -> bool:
    if len(df) < 3: return False
    last, prev, prev_prev = df.iloc[-1], df.iloc[-2], df.iloc[-3]
    
    # الشروط الحالية
    macd_cross_up = prev['macd'] < prev['macd_signal'] and last['macd'] > last['macd_signal']
    price_above_ema = last['close'] > last['ema_21']
    
    # إضافة فلتر قوة MACD
    macd_strength = last['macd_histogram'] > 0 and last['macd_histogram'] > prev['macd_histogram']
    
    # إضافة فلتر ADX للتأكد من وجود اتجاه
    trend_strength = last['adx'] > 20
    
    # إضافة فلتر أن يكون MACD كان تحت الصفر قبل العبور
    macd_position = prev_prev['macd'] < 0
    
    if macd_cross_up and price_above_ema and macd_strength and trend_strength and macd_position:
        logger.info(f"  -> [{df.name}] ✅ إشارة MACD+EMA (مع فلاتر قوة واتجاه).")
        return True
    return False

def check_ema_rsi_strategy(df: pd.DataFrame) -> bool:
    if len(df) < 2: return False
    last, prev = df.iloc[-1], df.iloc[-2]
    ema_cross_up = prev['ema_9'] < prev['ema_21'] and last['ema_9'] > last['ema_21']
    rsi_strong = last['rsi'] > 52
    trend_filter = last['close'] > last['ema_50']
    if ema_cross_up and rsi_strong and trend_filter:
        logger.info(f"  -> [{df.name}] ✅ إشارة استراتيجية EMA+RSI Cross.")
        return True
    return False

def check_pullback_strategy(df: pd.DataFrame) -> bool:
    if len(df) < 2: return False
    last, prev = df.iloc[-1], df.iloc[-2]
    uptrend_confirmed = last['close'] > last['ema_21'] and last['ema_21'] > last['ema_50']
    macd_cross_up = prev['macd'] < prev['macd_signal'] and last['macd'] > last['macd_signal']
    if uptrend_confirmed and macd_cross_up:
        logger.info(f"  -> [{df.name}] ✅ إشارة استراتيجية Pullback MACD.")
        return True
    return False

def check_bb_squeeze_strategy(df: pd.DataFrame) -> bool:
    if len(df) < 100: return False
    last = df.iloc[-1]
    
    squeeze_threshold = df['bb_width'].rolling(100).quantile(0.20).iloc[-1]
    is_squeeze = last['bb_width'] < squeeze_threshold
    
    breakout = last['close'] > last['bb_upper']
    volume_confirmed = last['relative_volume'] > 1.25
    
    if is_squeeze and breakout and volume_confirmed:
        logger.info(f"  -> [{df.name}] ✅ إشارة استراتيجية BB Squeeze Breakout.")
        return True
    return False

def check_bullish_momentum_strategy(df: pd.DataFrame) -> bool:
    if len(df) < 50:
        return False

    last = df.iloc[-1]
    
    price_above_sma50 = last['close'] > last['sma_50']
    strong_trend = last['adx'] > 25
    bullish_direction = last['plus_di'] > last['minus_di']
    rsi_is_bullish = 50 < last['rsi'] < 75
    
    if len(df) < 8: 
        return False
        
    recent_highs = df['high'].iloc[-8:-1]
    recent_lows = df['low'].iloc[-8:-1]
    
    def is_higher_highs(highs, min_count=3):
        if len(highs) < min_count + 1: return False
        count = 0
        for i in range(1, len(highs)):
            if highs.iloc[i] > highs.iloc[i-1]: count += 1
        return count >= min_count
    
    def is_higher_lows(lows, min_count=3):
        if len(lows) < min_count + 1: return False
        count = 0
        for i in range(1, len(lows)):
            if lows.iloc[i] > lows.iloc[i-1]: count += 1
        return count >= min_count
    
    price_momentum_confirmed = is_higher_highs(recent_highs) and is_higher_lows(recent_lows)
    volume_confirmation = last['volume'] > last['volume_sma_20'] * 1.1
    
    if all([price_above_sma50, strong_trend, bullish_direction, rsi_is_bullish, price_momentum_confirmed, volume_confirmation]):
        logger.info(f"  -> [{df.name}] ✅ إشارة زخم صعودي (معززة).")
        return True

    return False

def check_support_resistance_strategy_enhanced(df: pd.DataFrame) -> bool:
    """استراتيجية اختراق الدعم والمقاومة المحسنة"""
    if len(df) < 50:
        return False
    
    last = df.iloc[-1]
    
    # تحديد مستويات المقاومة بدقة أكبر
    resistance_candidates = df[df['high'] == df['high'].rolling(5, center=True).max()]['high']
    
    if resistance_candidates.empty:
        return False
    
    # أقرب مستوى مقاومة فوق السعر
    current_price = last['close']
    next_resistance_series = resistance_candidates[resistance_candidates > current_price]
    closest_resistance = next_resistance_series.min() if not next_resistance_series.empty else None
    
    # شرط اختراق المقاومة
    if closest_resistance is not None:
        # فلتر جديد: التحقق من قوة مستوى المقاومة (عدد مرات الاختبار)
        tolerance = closest_resistance * 0.01  # 1% tolerance
        resistance_tests = df[(df['high'] >= closest_resistance - tolerance) & 
                              (df['high'] <= closest_resistance + tolerance)]
        resistance_strength = len(resistance_tests)

        if resistance_strength < 2: # التأكد من أن المستوى تم اختباره مرتين على الأقل
            return False

        breakout = last['close'] > closest_resistance and df['close'].iloc[-2] <= closest_resistance
        
        # تأكيد الاختراق بحجم التداول
        volume_confirmation = last['volume'] > last['volume_sma_20'] * 1.3
        
        # تأكيد الاتجاه
        trend_confirmation = last['close'] > last['ema_21']
        
        # فلتر جديد: تجنب الاختراقات الكاذبة
        not_false_breakout = last['close'] > closest_resistance * 1.005  # اختراق حقيقي وليس مجرد لمس
        
        conditions = {
            "breakout": breakout,
            "volume_confirmation": volume_confirmation,
            "trend_confirmation": trend_confirmation,
            "not_false_breakout": not_false_breakout
        }

        if all(conditions.values()):
            logger.info(f"  -> [{df.name}] ✅ إشارة اختراق مقاومة (معززة) - قوة المستوى: {resistance_strength}")
            return True
        
        failed_conditions = {k: v for k, v in conditions.items() if not v}
        if breakout: # نسجل فقط إذا كان هناك اختراق لكنه فشل في الفلاتر الأخرى
             log_rejection(df.name, "SR Breakout Strategy Conditions Not Met", {"failed": list(failed_conditions.keys())})

    return False

# --- دالة تأكيد الترند على فريم أعلى ---
def is_htf_bullish_confirmation(symbol: str, htf: str = '1h', lookback: int = 200) -> bool:
    try:
        df = fetch_historical_data(symbol, htf, days=40) 
        if df is None or len(df) < lookback:
            logger.warning(f"  -> [HTF {htf}] {symbol} بيانات غير كافية للتأكيد ({len(df) if df is not None else 0} شمعة).")
            return False

        df['ema50']  = df['close'].ewm(span=50, adjust=False).mean()
        df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()

        tr = pd.concat([df['high'] - df['low'], (df['high'] - df['close'].shift()).abs(), (df['low']  - df['close'].shift()).abs()], axis=1).max(axis=1)
        df['atr'] = tr.rolling(14).mean()
        plus_dm = np.where((df['high'] - df['high'].shift()) > (df['low'].shift() - df['low']), df['high'] - df['high'].shift(), 0)
        minus_dm = np.where((df['low'].shift() - df['low']) > (df['high'] - df['high'].shift()), df['low'].shift() - df['low'], 0)
        plus_di = 100 * pd.Series(plus_dm).rolling(14).mean() / df['atr'].replace(0, 1e-9)
        minus_di = 100 * pd.Series(minus_dm).rolling(14).mean() / df['atr'].replace(0, 1e-9)
        dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di + 1e-9)
        df['adx'] = dx.rolling(14).mean()

        exp1 = df['close'].ewm(span=12, adjust=False).mean()
        exp2 = df['close'].ewm(span=26, adjust=False).mean()
        df['macd'] = exp1 - exp2
        df['signal_line'] = df['macd'].ewm(span=9, adjust=False).mean()

        last = df.iloc[-1]
        prev = df.iloc[-2]

        strong_uptrend = (last['ema50'] > last['ema200'] and last['adx'] > 25 and last['close'] > last['ema50'])
        macd_cross_up = prev['macd'] < prev['signal_line'] and last['macd'] > last['signal_line']
        ema_cross_up  = prev['ema50'] < prev['ema200'] and last['ema50'] > last['ema200']
        recent_bullish_flip = macd_cross_up and ema_cross_up

        is_confirmed = strong_uptrend or recent_bullish_flip
        logger.info(f"  -> [HTF {htf}] {symbol} تأكيد الترند: {is_confirmed} (قوي: {strong_uptrend} | تحول: {recent_bullish_flip})")
        return is_confirmed

    except Exception as e:
        logger.error(f"❌ [HTF Confirm] خطأ في {symbol}: {e}")
        return False

def is_htf_bullish_confirmation_cached(symbol: str) -> bool:
    """[تحسين V9.8] نسخة مخزنة مؤقتًا من تأكيد الترند على الفريم الأعلى.
    تمنع تكرار طلبات API لنفس العملة خلال مدة HTF_CONFIRMATION_CACHE_TTL.
    """
    now = time.time()
    with technical_signals_lock:
        cached = technical_signals_cache.get(symbol)
        if cached and now - cached.get('ts', 0) < HTF_CONFIRMATION_CACHE_TTL:
            return cached.get('value', False)
    value = is_htf_bullish_confirmation(symbol, HIGHER_TIMEFRAME)
    with technical_signals_lock:
        technical_signals_cache[symbol] = {'ts': now, 'value': value}
    return value

# --- دالة فلتر الزخم قصير الأجل ---
def passes_short_term_momentum_filter(symbol: str, df: pd.DataFrame) -> bool:
    if len(df) < 100:
        return False

    last = df.iloc[-1]
    
    bb_width = last.get('bb_width', 0)
    price_vs_bb_upper = abs(last['close'] - last['bb_upper']) / last['bb_upper'] if last['bb_upper'] > 0 else 0
    price_vs_bb_lower = abs(last['close'] - last['bb_lower']) / last['bb_lower'] if last['bb_lower'] > 0 else 0
    
    close_to_bands = price_vs_bb_upper < 0.005 or price_vs_bb_lower < 0.005
    
    with volume_filter_lock:
        vol_mult = VOLUME_FILTER_MULTIPLIER
    volume_spike = last.get('relative_volume', 0) > vol_mult
    
    macd_momentum = last['macd'] > last['macd_signal'] and last['macd'] > 0
    rsi_momentum  = last['rsi'] > 55
    
    squeeze_threshold = df['bb_width'].rolling(100).quantile(0.25).iloc[-1]
    is_squeeze = bb_width < squeeze_threshold
    
    price_momentum = last['close'] > last['ema_9'] and last['close'] > df['close'].iloc[-4] # Check against 3 candles ago
    
    trend_strength = last['adx'] > 20
    
    is_valid = (
        (is_squeeze or close_to_bands) and
        volume_spike and
        (macd_momentum or rsi_momentum) and
        price_momentum and
        trend_strength
    )
    
    logger.info(f"  -> [فلتر الزخم المحسن] {symbol}: Valid={is_valid}")
    return is_valid

class EnhancedTradingStrategy:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.ml_model, self.scaler, self.feature_names = None, None, None

    def load_model(self) -> bool:
        model_name = f"{BASE_ML_MODEL_NAME}_{self.symbol}"
        if model_name in ml_models_cache:
            model_bundle = ml_models_cache[model_name]
        else:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            model_dir_path = os.path.join(script_dir, MODEL_FOLDER)
            model_path = os.path.join(model_dir_path, f"{model_name}.pkl")

            if not os.path.exists(model_path):
                return False
            try:
                with open(model_path, 'rb') as f:
                    model_bundle = pickle.load(f)
                ml_models_cache[model_name] = model_bundle
            except Exception as e:
                logger.error(f"❌ [نموذج ML] خطأ في تحميل النموذج لـ {self.symbol}: {e}")
                return False

        if 'model' in model_bundle and 'scaler' in model_bundle and 'feature_names' in model_bundle:
            self.ml_model = model_bundle['model']
            self.scaler = model_bundle['scaler']
            self.feature_names = model_bundle['feature_names']
            return True
        else:
            logger.error(f"  -> [{self.symbol}] 🛑 ملف نموذج ML غير مكتمل.")
            return False

    def get_features_for_model(self, df_15m: pd.DataFrame, df_4h: pd.DataFrame, btc_df: pd.DataFrame) -> Optional[pd.DataFrame]:
        if self.feature_names is None: return None
        try:
            df_featured = calculate_all_features(df_15m, btc_df)
            delta_4h = df_4h['close'].diff()
            gain_4h = delta_4h.clip(lower=0).ewm(com=RSI_PERIOD - 1, adjust=False).mean()
            loss_4h = -delta_4h.clip(upper=0).ewm(com=RSI_PERIOD - 1, adjust=False).mean()
            df_4h['rsi_4h'] = 100 - (100 / (1 + (gain_4h / loss_4h.replace(0, 1e-9))))
            ema_fast_4h = df_4h['close'].ewm(span=EMA_FAST_PERIOD, adjust=False).mean()
            df_4h['price_vs_ema50_4h'] = (df_4h['close'] / ema_fast_4h) - 1
            mtf_features = df_4h[['rsi_4h', 'price_vs_ema50_4h']]
            df_featured = df_featured.join(mtf_features)
            df_featured[['rsi_4h', 'price_vs_ema50_4h']] = df_featured[['rsi_4h', 'price_vs_ema50_4h']].ffill()
            for col in self.feature_names:
                if col not in df_featured.columns:
                    df_featured[col] = 0.0
            df_featured.replace([np.inf, -np.inf], np.nan, inplace=True)
            return df_featured.dropna(subset=self.feature_names)
        except Exception as e:
            logger.error(f"❌ [{self.symbol}] فشل هندسة الميزات لنموذج ML: {e}", exc_info=True)
            return None

    def generate_prediction_result(self, df_features: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if not all([self.ml_model, self.scaler, self.feature_names]) or df_features.empty:
            return None
        try:
            last_row_ordered_df = df_features.iloc[[-1]][self.feature_names]
            features_scaled = self.scaler.transform(last_row_ordered_df)

            prediction = self.ml_model.predict(features_scaled)[0]
            prediction_proba = self.ml_model.predict_proba(features_scaled)
            confidence = float(np.max(prediction_proba[0]))

            return {'prediction': int(prediction), 'confidence': confidence}
        except Exception as e:
            logger.warning(f"⚠️ [{self.symbol}] خطأ في توليد تنبؤ نموذج ML: {e}", exc_info=True)
            return None

# --- التعرف على أنماط الشموع ---
def is_bullish_reversal_pattern(df: pd.DataFrame) -> bool:
    if len(df) < 3: return False
    c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]
    patterns = {
        "Hammer": is_hammer(c3, c2), "Inverse Hammer": is_inverse_hammer(c3, c2),
        "Bullish Engulfing": is_bullish_engulfing(c3, c2), "Piercing Line": is_piercing_line(c3, c2),
        "Morning Star": is_morning_star(c1, c2, c3), "Three White Soldiers": is_three_white_soldiers(c1, c2, c3)
    }
    for pattern_name, is_present in patterns.items():
        if is_present:
            logger.info(f"  -> [{df.name}] ✅ نمط شمعة صاعدة: {pattern_name}")
            return True
    return False

def is_hammer(candle: pd.Series, prev_candle: pd.Series) -> bool:
    body = abs(candle['open'] - candle['close'])
    lower_wick = candle['close'] - candle['low'] if candle['open'] < candle['close'] else candle['open'] - candle['low']
    upper_wick = candle['high'] - candle['close'] if candle['open'] < candle['close'] else candle['high'] - candle['open']
    return body > 0 and lower_wick > 2 * body and upper_wick < body

def is_inverse_hammer(candle: pd.Series, prev_candle: pd.Series) -> bool:
    body = abs(candle['open'] - candle['close'])
    lower_wick = candle['close'] - candle['low'] if candle['open'] < candle['close'] else candle['open'] - candle['low']
    upper_wick = candle['high'] - candle['close'] if candle['open'] < candle['close'] else candle['high'] - candle['open']
    return body > 0 and upper_wick > 2 * body and lower_wick < body

def is_bullish_engulfing(candle: pd.Series, prev_candle: pd.Series) -> bool:
    return (prev_candle['close'] < prev_candle['open'] and
            candle['close'] > candle['open'] and
            candle['close'] > prev_candle['open'] and
            candle['open'] < prev_candle['close'])

def is_piercing_line(candle: pd.Series, prev_candle: pd.Series) -> bool:
    midpoint = (prev_candle['open'] + prev_candle['close']) / 2
    return (prev_candle['close'] < prev_candle['open'] and
            candle['close'] > candle['open'] and
            candle['open'] < prev_candle['low'] and
            candle['close'] > midpoint and
            candle['close'] < prev_candle['open'])

def is_morning_star(c1: pd.Series, c2: pd.Series, c3: pd.Series) -> bool:
    c1_body = abs(c1['open'] - c1['close'])
    c3_body = abs(c3['open'] - c3['close'])
    return (c1['close'] < c1['open'] and c1_body > c1.atr * 0.7 and
            abs(c2['open'] - c2['close']) < c1_body * 0.3 and
            c2['close'] < c1['close'] and c2['open'] < c1['close'] and
            c3['close'] > c3['open'] and
            c3['close'] > (c1['open'] + c1['close']) / 2)

def is_three_white_soldiers(c1: pd.Series, c2: pd.Series, c3: pd.Series) -> bool:
    return (c1['close'] > c1['open'] and c2['close'] > c2['open'] and c3['close'] > c3['open'] and
            c2['open'] > c1['open'] and c2['open'] < c1['close'] and c2['close'] > c1['close'] and
            c3['open'] > c2['open'] and c3['open'] < c2['close'] and c3['close'] > c2['close'])

def passes_final_order_book_check(symbol: str, entry_price: float) -> bool:
    if not client:
        log_rejection(symbol, "Order Book Fetch Failed", {"error": "Client not initialized"})
        return False
    try:
        with order_book_ratio_lock:
             current_ratio_threshold = ORDER_BOOK_MIN_BID_ASK_RATIO

        order_book = safe_get_order_book(symbol, ORDER_BOOK_DEPTH_LIMIT)
        bids = pd.DataFrame(order_book['bids'], columns=['price', 'qty'], dtype=float)
        asks = pd.DataFrame(order_book['asks'], columns=['price', 'qty'], dtype=float)

        price_range_upper = entry_price * (1 + ORDER_BOOK_ANALYSIS_RANGE_PCT)
        price_range_lower = entry_price * (1 - ORDER_BOOK_ANALYSIS_RANGE_PCT)

        relevant_bids_vol = bids[bids['price'].between(price_range_lower, entry_price)]['qty'].sum()
        relevant_asks_vol = asks[asks['price'].between(entry_price, price_range_upper)]['qty'].sum()

        if relevant_asks_vol == 0: return True

        bid_ask_ratio = relevant_bids_vol / relevant_asks_vol

        if bid_ask_ratio >= current_ratio_threshold:
            return True
        else:
            log_rejection(symbol, "Order Book Filter Failed", {"ratio": f"{bid_ask_ratio:.2f}", "required": f"{current_ratio_threshold}"})
            return False

    except Exception as e:
        log_rejection(symbol, "Order Book Fetch Failed", {"error": str(e)})
        return False

# --- [تحسين] دوال حساب الأهداف ووقف الخسارة ---
def calculate_dynamic_tp_sl(df: pd.DataFrame, entry_price: float, is_long: bool = True) -> Optional[Dict[str, Any]]:
    """حساب وقف الخسارة وجني الأرباح بشكل ديناميكي بناءً على ظروف السوق"""
    try:
        if len(df) < 20:
            log_rejection(df.name, "Insufficient data for TP/SL calculation")
            return None
        
        last = df.iloc[-1]
        
        # استخدام ATR لحساب وقف الخسارة وجني الأرباح
        atr_multiplier_sl = 2.5
        atr_multiplier_tp = 4.0
        
        # تعديل المضاعفات بناءً على تقلب السوق
        atr_percent = (last['atr'] / last['close']) * 100 if last['close'] > 0 else 0
        if atr_percent > 3.0:  # سوق متقلب
            atr_multiplier_sl *= 1.2
            atr_multiplier_tp *= 1.2
        elif atr_percent < 1.0:  # سوق هادئ
            atr_multiplier_sl *= 0.8
            atr_multiplier_tp *= 0.8
        
        sl_distance = last['atr'] * atr_multiplier_sl
        tp_distance = last['atr'] * atr_multiplier_tp
        
        if is_long:
            stop_loss = entry_price - sl_distance
            take_profit = entry_price + tp_distance
        else: # (Not currently used for long-only bot, but good practice)
            stop_loss = entry_price + sl_distance
            take_profit = entry_price - sl_distance
        
        if (entry_price - stop_loss) <= 0:
             log_rejection(df.name, "Invalid Position Size", {"details": "Stop loss is at or above entry price"})
             return None
        
        rr_ratio = (take_profit - entry_price) / (entry_price - stop_loss)

        return {
            'target_price': round(take_profit, 6), 
            'stop_loss': round(stop_loss, 6),
            'source': 'DYNAMIC_ATR_BASED_V2', 
            'rr_ratio': round(rr_ratio, 2)
        }
    except Exception as e:
        logger.error(f"❌ [{df.name}] خطأ في حساب TP/SL الديناميكي: {e}", exc_info=True)
        return None


# ---------------------- دوال إدارة الصفقات ----------------------
def adjust_quantity_to_lot_size(symbol: str, quantity: float) -> Optional[Decimal]:
    try:
        symbol_info = exchange_info_map.get(symbol)
        if not symbol_info: return None
        lot_size_filter = next((f for f in symbol_info['filters'] if f['filterType'] == 'LOT_SIZE'), None)
        if lot_size_filter:
            step_size = Decimal(lot_size_filter['stepSize'])
            return (Decimal(str(quantity)) // step_size) * step_size
        return Decimal(str(quantity))
    except Exception as e:
        logger.error(f"[{symbol}] خطأ في تعديل الكمية لـ LOT_SIZE: {e}", exc_info=True)
        return None

def calculate_position_size(symbol: str, entry_price: float, stop_loss_price: float) -> Optional[Decimal]:
    if not client: return None
    try:
        with risk_per_trade_lock: current_risk_percent = RISK_PER_TRADE_PERCENT
        balance_response = safe_get_asset_balance('USDT')
        available_balance = Decimal(balance_response['free'])
        risk_amount_usdt = available_balance * (Decimal(str(current_risk_percent)) / Decimal('100'))
        risk_per_coin = Decimal(str(entry_price)) - Decimal(str(stop_loss_price))
        if risk_per_coin <= 0: log_rejection(symbol, "Invalid Position Size"); return None
        initial_quantity = risk_amount_usdt / risk_per_coin
        adjusted_quantity = adjust_quantity_to_lot_size(symbol, float(initial_quantity))
        if adjusted_quantity is None or adjusted_quantity <= 0: log_rejection(symbol, "Lot Size Adjustment Failed"); return None
        notional_value = adjusted_quantity * Decimal(str(entry_price))
        symbol_info = exchange_info_map.get(symbol)
        if symbol_info:
            for f in symbol_info['filters']:
                if f['filterType'] in ('MIN_NOTIONAL', 'NOTIONAL'):
                    min_notional = Decimal(f.get('minNotional', f.get('notional', '0')))
                    if notional_value < min_notional: log_rejection(symbol, "Min Notional Filter", {"value": f"{notional_value:.2f}"}); return None
        if notional_value > available_balance: log_rejection(symbol, "Insufficient Balance", {"required": f"{notional_value:.2f}"}); return None
        return adjusted_quantity
    except Exception as e:
        logger.error(f"❌ [{symbol}] خطأ في حساب حجم الصفقة: {e}", exc_info=True); return None

def place_order(symbol: str, side: str, quantity: Decimal, order_type: str = Client.ORDER_TYPE_MARKET) -> Optional[Dict]:
    if not client: return None
    logger.info(f"➡️ [{symbol}] محاولة تنفيذ أمر {side} حقيقي لكمية {quantity}.")
    try:
        order = safe_create_order(symbol=symbol, side=side, type=order_type, quantity=str(quantity))
        log_and_notify('info', f"صفقة حقيقية: تم وضع أمر {side} لـ {quantity} {symbol}.", "REAL_TRADE")
        return order
    except Exception as e:
        logger.error(f"❌ [{symbol}] خطأ من المنصة عند تنفيذ الأمر: {e}")
        log_and_notify('error', f"فشل صفقة حقيقية: {symbol} | {e}", "REAL_TRADE_ERROR")
        return None

def verify_order_filled(symbol: str, order_id: str, timeout_seconds: int = 30) -> bool:
    if not client: return False
    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        try:
            order_status = safe_get_order(symbol=symbol, orderId=order_id)
            if order_status['status'] == 'FILLED': return True
            elif order_status['status'] in ['CANCELED', 'EXPIRED', 'REJECTED']: return False
            time.sleep(2)
        except Exception as e:
            logger.error(f"❌ [{symbol}] خطأ غير متوقع عند التحقق من الأمر {order_id}: {e}", exc_info=True)
            return False
    return False

def close_signal(signal_id: int, closing_price: float, reason: str) -> bool:
    with signal_cache_lock:
        signal_to_close, symbol_to_close = None, None
        for symbol, signal_data in open_signals_cache.items():
            if signal_data['id'] == signal_id:
                signal_to_close, symbol_to_close = signal_data, symbol
                break
        if not signal_to_close:
            logger.warning(f"⚠️ [إغلاق] محاولة إغلاق صفقة غير موجودة في الكاش (ID: {signal_id}). ربما أغلقت بالفعل.")
            return False

        entry_price = float(signal_to_close['entry_price'])
        profit_percentage = ((closing_price - entry_price) / entry_price) * 100

        # --- [إصلاح] منطق البيع عند الإغلاق ---
        if signal_to_close.get('is_real_trade'):
            try:
                base_asset = symbol_to_close.replace('USDT', '')
                balance_response = safe_get_asset_balance(base_asset)
                actual_free_balance = Decimal(balance_response['free'])
                
                logger.info(f"  -> [{symbol_to_close}] التحقق من الرصيد للإغلاق الكامل. الرصيد الفعلي: {actual_free_balance} {base_asset}")

                if actual_free_balance > 0:
                    # [تحسين V9.8] بيع الكمية المرتبطة بالصفقة فقط بدلاً من كامل الرصيد،
                    # لتجنب بيع أرصدة نفس العملة المملوكة خارج البوت
                    original_qty = Decimal(str(signal_to_close.get('original_quantity') or signal_to_close.get('quantity') or '0'))
                    if original_qty <= 0 or actual_free_balance <= original_qty * Decimal('1.05'):
                        amount_to_sell = actual_free_balance
                    else:
                        amount_to_sell = min(actual_free_balance, original_qty)
                    quantity_to_sell = adjust_quantity_to_lot_size(symbol_to_close, float(amount_to_sell))
                    
                    if quantity_to_sell and quantity_to_sell > 0:
                        # التحقق من فلتر MIN_NOTIONAL قبل البيع
                        notional_value = quantity_to_sell * Decimal(str(closing_price))
                        symbol_info = exchange_info_map.get(symbol_to_close)
                        min_notional_ok = True
                        if symbol_info:
                            min_notional_filter = next((f for f in symbol_info['filters'] if f['filterType'] in ('MIN_NOTIONAL', 'NOTIONAL')), None)
                            if min_notional_filter:
                                min_notional = Decimal(min_notional_filter.get('minNotional', min_notional_filter.get('notional', '0')))
                                if notional_value < min_notional:
                                    min_notional_ok = False
                                    logger.warning(f"⚠️ [{symbol_to_close}] الرصيد الفعلي للبيع ({quantity_to_sell}) أقل من الحد الأدنى ({min_notional}). سيتم اعتباره غبارًا.")
                        
                        if min_notional_ok:
                            sell_order = place_order(symbol_to_close, Client.SIDE_SELL, quantity_to_sell)
                            if not sell_order:
                                logger.warning(f"⚠️ [{symbol_to_close}] فشل أمر البيع عند الإغلاق. سيتم إكمال عملية الإغلاق في قاعدة البيانات على أي حال.")
                    else:
                        logger.info(f"  -> [{symbol_to_close}] الرصيد الفعلي بعد الضبط هو صفر أو لا شيء. لا يوجد ما يمكن بيعه.")
            except Exception as e:
                logger.error(f"❌ [{symbol_to_close}] خطأ أثناء محاولة بيع الرصيد عند الإغلاق: {e}", exc_info=True)
                # نستمر في إغلاق الصفقة في قاعدة البيانات على أي حال
        
        if not check_db_connection() or not conn: return False

        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE signals SET status = 'closed', closing_price = %s, closed_at = NOW(),
                    profit_percentage = %s, closing_reason = %s WHERE id = %s;
                """, (closing_price, profit_percentage, reason, signal_id))
            conn.commit()

            if symbol_to_close in open_signals_cache:
                del open_signals_cache[symbol_to_close]

            log_and_notify('info', f"تم الإغلاق: {symbol_to_close} عند {closing_price:.4f}. السبب: {reason}. الربح/الخسارة: {profit_percentage:.2f}%", "TRADE_CLOSED")
            register_realized_pnl(signal_to_close, entry_price, closing_price)

            reason_map = {
                'take_profit': '🎯 أخذ الربح', 'stop_loss': '🛑 وقف الخسارة', 'manual': '🖐️ إغلاق يدوي',
                'atr_trailing_stop': '🛡️ وقف خسارة متحرك', 'journey_completed': '🏁 اكتملت الرحلة',
                'take_profit_full_exit_on_small_size': '🎯 أخذ الربح (إغلاق كامل لصفقة صغيرة)'
            }
            emoji = "✅" if profit_percentage >= 0 else "🔻"
            trade_type = "حقيقية" if signal_to_close.get('is_real_trade') else "تجريبية"
            telegram_message = (
                f"{emoji} *إغلاق صفقة {trade_type}*\n\n"
                f"*العملة:* `{symbol_to_close}`\n*سبب الإغلاق:* {reason_map.get(reason, reason)}\n"
                f"*سعر الدخول:* `{entry_price:.4f}`\n*سعر الإغلاق:* `{closing_price:.4f}`\n"
                f"*الربح/الخسارة النهائي:* `{profit_percentage:.2f}%`"
            )
            send_telegram_message(telegram_message)
            return True
        except Exception as e:
            logger.error(f"❌ [قاعدة البيانات] فشل تحديث الصفقة المغلقة: {e}"); conn.rollback(); return False

def insert_signal_into_db(signal_data: Dict) -> Optional[Dict]:
    if not check_db_connection() or not conn: return None
    try:
        with conn.cursor() as cur:
            entry_price = float(signal_data['entry_price'])
            target_price = float(signal_data['target_price'])
            stop_loss = float(signal_data['stop_loss'])
            quantity = float(signal_data['quantity']) if signal_data.get('quantity') is not None else None
            rr_ratio = float(signal_data.get('rr_ratio', 0.0))

            journey_state = None
            if USE_DYNAMIC_JOURNEY:
                journey_state = {
                    "targets_hit": 0,
                    "is_complete": False,
                    "partial_exit_done": False
                }

            signal_details_json = json.dumps(signal_data['signal_details'], cls=NpEncoder)
            journey_state_json = json.dumps(journey_state, cls=NpEncoder) if journey_state else None

            cur.execute("""
                INSERT INTO signals (symbol, entry_price, target_price, stop_loss, strategy_name, signal_details, is_real_trade, quantity, original_quantity, order_id, current_peak_price, journey_state, rr_ratio)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING *;
            """, (
                signal_data['symbol'], entry_price, target_price, stop_loss,
                signal_data['strategy_name'], signal_details_json, signal_data.get('is_real_trade', False),
                quantity, quantity, signal_data.get('order_id'), entry_price, journey_state_json, rr_ratio
            ))
            saved_signal = cur.fetchone()
            conn.commit()
            logger.info(f"💾 [{signal_data['symbol']}] تم حفظ الإشارة الجديدة في قاعدة البيانات.")

            trade_type = "حقيقية" if signal_data.get('is_real_trade') else "تجريبية"
            telegram_message = (
                f"💡 *توصية شراء {trade_type} جديدة*\n\n"
                f"*العملة:* `{signal_data['symbol']}`\n*الاستراتيجية:* `{signal_data['strategy_name'].replace('_', ' ')}`\n"
                f"*سعر الدخول:* `{entry_price:.4f}`\n*الهدف الأول:* `{target_price:.4f}`\n"
                f"*وقف الخسارة:* `{stop_loss:.4f}`\n*RR Ratio:* `{rr_ratio:.2f}`\n\n"
                f"Confidence: {signal_data['signal_details'].get('ML_Confidence', 'N/A')}"
            )
            send_telegram_message(telegram_message)
            return dict(saved_signal)
    except Exception as e:
        logger.error(f"❌ [قاعدة البيانات] فشل إدراج الإشارة: {e}", exc_info=True); conn.rollback(); return None


# ---------------------- دوال النظام الأساسية ----------------------
def determine_market_state_enhanced():
    global current_market_state, last_market_state_check
    if time.time() - last_market_state_check < 180: return
    logger.info("🧠 [حالة السوق] جاري تحديث حالة السوق العامة...")
    try:
        trend_details = {}
        for tf in TIMEFRAMES_FOR_TREND_LIGHTS:
            df = fetch_historical_data(BTC_SYMBOL, tf, 20)
            if df is not None and not df.empty:
                ema_fast = df['close'].ewm(span=12, adjust=False).mean().iloc[-1]
                ema_slow = df['close'].ewm(span=26, adjust=False).mean().iloc[-1]
                adx_features = calculate_all_features(df, None)
                adx = adx_features['adx'].iloc[-1] if not adx_features.empty else 0
                if ema_fast > ema_slow and adx > 25: trend = "Strong Uptrend"
                elif ema_fast > ema_slow: trend = "Uptrend"
                elif ema_fast < ema_slow and adx > 25: trend = "Strong Downtrend"
                elif ema_fast < ema_slow: trend = "Downtrend"
                else: trend = "Ranging"
                trend_details[tf] = {"trend": trend, "adx": float(adx)}
            else: trend_details[tf] = {"trend": "Uncertain", "adx": 0}
        trends = [d['trend'] for d in trend_details.values()]
        overall_regime = max(set(trends), key=trends.count) if trends else "Uncertain"
        with market_state_lock:
            current_market_state = {"overall_regime": overall_regime.upper().replace(" ", "_"), "trend_details_by_tf": trend_details, "last_updated": datetime.now(timezone.utc).isoformat()}
            last_market_state_check = time.time()
        logger.info(f"✅ [حالة السوق] الحالة العامة المحددة: {overall_regime}")
    except Exception as e:
        logger.error(f"❌ [حالة السوق] خطأ في التحديث: {e}", exc_info=True)

# ---------------------- واجهة الويب (Flask) ----------------------
app = Flask(__name__)
CORS(app)

# --- [تحسين V9.8] حماية لوحة التحكم بمصادقة أساسية (Basic Auth) ---
# تُفعّل تلقائيًا عند ضبط DASHBOARD_USERNAME و DASHBOARD_PASSWORD في متغيرات البيئة
DASHBOARD_PROTECTED = bool(DASHBOARD_USERNAME and DASHBOARD_PASSWORD)

@app.before_request
def require_dashboard_auth():
    """يحمي جميع مسارات اللوحة بكلمة مرور. نقطة /health مستثناة لمراقبة الخدمة."""
    if not DASHBOARD_PROTECTED or request.path == '/health':
        return None
    auth = request.authorization
    if auth and auth.username == DASHBOARD_USERNAME and auth.password == DASHBOARD_PASSWORD:
        return None
    logger.warning(f"🔒 [أمن] محاولة وصول غير مصرح بها إلى {request.path} من: {request.remote_addr}")
    # [تحسين V9.9] صفحة رفض بثيم الطرفية الهاكرية
    denied_html = """<!DOCTYPE html><html lang="ar" dir="rtl"><head><meta charset="UTF-8">
<title>ACCESS DENIED // CryptoBot</title><style>
body{background:#020403;color:#00ff41;font-family:'Courier New',monospace;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0}
.box{border:1px solid #114d23;padding:2.5rem 3rem;text-align:center;box-shadow:0 0 30px rgba(0,255,65,.25)}
h1{font-size:2rem;margin:0 0 .6rem;text-shadow:0 0 12px rgba(0,255,65,.8);direction:ltr;letter-spacing:2px}
p{color:#5d8f6d;margin:.3rem 0;font-size:.9rem}
.t{color:#ff2b4e}</style></head>
<body><div class="box"><h1>&gt;&gt; ACCESS DENIED &lt;&lt;</h1>
<p>لوحة التحكم محمية — يتطلب تسجيل الدخول</p>
<p dir="ltr">[<span class="t">401 Unauthorized</span>] realm: CryptoBot Dashboard</p></div></body></html>"""
    return Response(denied_html, 401,
                    {"WWW-Authenticate": 'Basic realm="CryptoBot Dashboard"'})

def get_dashboard_html():
    return """
<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
    <meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CryptoBot V9.11.0 // NEON TERMINAL</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script>
        // [تحسين V9.9] ألوان الثيم الهاكر: أخضر مصفوفة + سماوي سيبراني على أسود
        tailwind.config = { theme: { extend: {
            colors: {
                'accent-green': '#00ff41', 'accent-red': '#ff2b4e', 'accent-yellow': '#f5ff00', 'accent-blue': '#00e5ff',
                'text-primary': '#c9ffd6', 'text-secondary': '#5d8f6d', 'border-color': '#114d23',
                'blue': {600:'#00e5ff',700:'#00b7d4'}, 'red': {600:'#ff2b4e',700:'#d9203f'},
                'gray': {500:'#4a7a5a',600:'#0d2a17',700:'#082010'}
            },
            fontFamily: { mono: ['"Share Tech Mono"','Consolas','monospace'] }
        } } }
    </script>
    <link href="https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Noto+Kufi+Arabic:wght@400;500;700;800&display=swap" rel="stylesheet">
    <style>
        :root { --neon:#00ff41; --neon-dim:#114d23; --cyan:#00e5ff; --red:#ff2b4e; --yellow:#f5ff00; --bg:#020403; }
        * { scrollbar-width: thin; scrollbar-color: #114d23 #020403; }
        ::-webkit-scrollbar { width: 8px; height: 8px; }
        ::-webkit-scrollbar-track { background: #020403; }
        ::-webkit-scrollbar-thumb { background: #114d23; border-radius: 4px; }
        body { font-family: 'Noto Kufi Arabic', 'Share Tech Mono', monospace; background-color: #020403; color: #c9ffd6; }
        /* [V9.9] خلفية المطر الرقمي (Matrix Rain) + طبقة CRT */
        #matrix-rain { position: fixed; inset: 0; z-index: 0; opacity: .15; pointer-events: none; }
        .crt-overlay { position: fixed; inset: 0; z-index: 50; pointer-events: none; background: repeating-linear-gradient(0deg, rgba(0,0,0,.18) 0px, rgba(0,0,0,.18) 1px, transparent 1px, transparent 3px); }
        .crt-vignette { position: fixed; inset: 0; z-index: 50; pointer-events: none; background: radial-gradient(ellipse at center, transparent 55%, rgba(0,0,0,.5) 100%); }
        .wrap { position: relative; z-index: 2; }
        .card { background: rgba(4,16,8,.82); border: 1px solid var(--neon-dim); border-radius: .3rem; box-shadow: 0 0 14px rgba(0,255,65,.08), inset 0 0 30px rgba(0,255,65,.03); position: relative; backdrop-filter: blur(2px); }
        .card::before, .card::after { content: ''; position: absolute; width: 12px; height: 12px; opacity: .8; }
        .card::before { top: -1px; right: -1px; border-top: 2px solid var(--neon); border-right: 2px solid var(--neon); }
        .card::after { bottom: -1px; left: -1px; border-bottom: 2px solid var(--neon); border-left: 2px solid var(--neon); }
        h1, h3, h4 { text-shadow: 0 0 10px rgba(0,255,65,.4); }
        .neon-text { text-shadow: 0 0 6px rgba(0,255,65,.9), 0 0 18px rgba(0,255,65,.5); }
        @keyframes blinkC { 0%,49% {opacity:1} 50%,100% {opacity:0} }
        .cursor::after { content: '▊'; animation: blinkC 1s steps(1) infinite; color: var(--neon); }
        @keyframes flickerA { 0%,100% {opacity:1} 91% {opacity:1} 92% {opacity:.55} 93% {opacity:1} 96% {opacity:.75} 97% {opacity:1} }
        .flicker { animation: flickerA 7s infinite; }
        .trend-light { width: .9rem; height: .9rem; border-radius: 9999px; border: 1px solid #114d23; transition: all 0.5s ease; }
        .light-on-green { background-color: var(--neon); box-shadow: 0 0 10px 2px var(--neon); animation: pulseG 2s infinite; }
        .light-on-red { background-color: var(--red); box-shadow: 0 0 10px 2px var(--red); }
        .light-on-yellow { background-color: var(--yellow); box-shadow: 0 0 10px 2px var(--yellow); }
        @keyframes pulseG { 0%,100% {box-shadow:0 0 6px 1px var(--neon)} 50% {box-shadow:0 0 14px 4px var(--neon)} }
        .tab-btn.active { border-bottom: 2px solid var(--neon); color: var(--neon); text-shadow: 0 0 8px rgba(0,255,65,.6); }
        input:checked + .toggle-bg { background-color: var(--neon); box-shadow: 0 0 12px rgba(0,255,65,.55); }
        #modal-overlay { transition: opacity 0.3s ease; background: rgba(0,0,0,.8) !important; }
        .input-field { background-color: #031008 !important; border: 1px solid var(--neon-dim); border-radius: .25rem; padding: 0.5rem 0.75rem; color: #c9ffd6; font-family: 'Share Tech Mono', monospace; transition: all .2s; }
        .input-field:focus { outline: none; border-color: var(--neon); box-shadow: 0 0 10px rgba(0,255,65,.35); }
        .save-btn { background: linear-gradient(180deg,#00ff41,#00c431); color: #01130a; padding: 0.5rem 1.4rem; border-radius: .25rem; font-weight: 800; letter-spacing: .5px; transition: all .2s; border: 1px solid #00ff41; box-shadow: 0 0 14px rgba(0,255,65,.4); }
        .save-btn:hover { box-shadow: 0 0 22px rgba(0,255,65,.8); transform: translateY(-1px); }
        .strategy-toggle { border-left: 4px solid var(--cyan); }
        .strategy-toggle-new { border-left: 4px solid var(--yellow); }
        .strategy-toggle-momentum { border-left: 4px solid var(--red); }
        .tp-slider { -webkit-appearance: none; width: 100%; height: 6px; background: linear-gradient(90deg,#114d23,#0a2f14); border-radius: 5px; outline: none; opacity: .85; transition: opacity .2s; }
        .tp-slider:hover { opacity: 1; }
        .tp-slider::-webkit-slider-thumb { -webkit-appearance: none; appearance: none; width: 16px; height: 16px; background: var(--neon); cursor: pointer; border-radius: 50%; box-shadow: 0 0 10px 2px rgba(0,255,65,.7); }
        .tp-slider::-moz-range-thumb { width: 16px; height: 16px; background: var(--neon); cursor: pointer; border-radius: 50%; border: none; box-shadow: 0 0 10px 2px rgba(0,255,65,.7); }
        table thead tr { background: rgba(0,255,65,.05); }
        tbody tr:hover { background: rgba(0,255,65,.05) !important; }
        .sys-bar { height: 6px; background: #082010; border: 1px solid var(--neon-dim); border-radius: 4px; overflow: hidden; min-width: 120px; width: 140px; }
        .sys-bar > div { height: 100%; background: linear-gradient(90deg,#00ff41,#f5ff00); transition: width .6s ease; box-shadow: 0 0 8px rgba(0,255,65,.6); }
    </style>
</head>
<body class="p-4 md:p-6">
    <canvas id="matrix-rain"></canvas>
    <div class="crt-overlay"></div>
    <div class="crt-vignette"></div>
    <div id="modal-overlay" class="fixed inset-0 bg-black bg-opacity-70 hidden items-center justify-center z-50">
        <div id="modal-content" class="card p-6 rounded-lg shadow-xl max-w-sm w-full">
            <h3 id="modal-title" class="text-xl font-bold mb-4"></h3>
            <p id="modal-body" class="text-text-secondary mb-6"></p>
            <div class="flex justify-end gap-3">
                <button id="modal-cancel" class="px-4 py-2 rounded-md bg-gray-600 hover:bg-gray-700">إلغاء</button>
                <button id="modal-confirm" class="px-4 py-2 rounded-md bg-red-600 hover:bg-red-700">تأكيد</button>
            </div>
        </div>
    </div>

    <div class="container mx-auto max-w-screen-2xl wrap">
        <header class="mb-6 flex flex-wrap justify-between items-center gap-4">
            <div>
                <div dir="ltr" class="font-mono text-xs md:text-sm text-text-secondary mb-1">&gt;&gt; root@crypto-bot:~$ ./trading_engine --live --region=eu-frankfurt<span class="cursor"></span></div>
                <h1 class="text-2xl md:text-3xl font-extrabold flicker"><span class="text-accent-green neon-text">لوحة تحكم</span> <span class="font-mono text-text-secondary text-lg md:text-xl" dir="ltr">V9.11.0//NEON</span></h1>
            </div>
            <div id="trend-lights-container" class="flex items-center gap-x-6 bg-black/40 px-4 py-2 rounded-lg border border-border-color"></div>
        </header>
        <section class="mb-6 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-5">
            <div class="card p-4"><h3 class="font-bold mb-3 text-lg text-text-secondary">حالة السوق</h3><div id="overall-regime" class="text-2xl font-bold text-center">...</div></div>
            <div class="card p-4"><h3 class="font-bold mb-3 text-lg text-text-secondary">الجلسات النشطة</h3><div id="active-sessions-list" class="flex flex-wrap gap-2 items-center justify-center pt-2">...</div></div>
            <div class="card p-4"><h3 class="font-bold mb-3 text-lg text-text-secondary">الصفقات المفتوحة</h3><div id="open-trades-count" class="text-2xl font-bold text-center">...</div></div>
            <div class="card p-4 flex flex-col justify-center items-center"><h3 class="font-bold text-lg text-text-secondary mb-2">التداول الحقيقي</h3><div class="flex items-center space-x-3 space-x-reverse"><span id="trading-status-text" class="font-bold text-lg"></span><label class="flex items-center cursor-pointer"><div class="relative"><input type="checkbox" id="trading-toggle" class="sr-only" onchange="toggleTrading()"><div class="toggle-bg block bg-gray-600 w-12 h-7 rounded-full"></div></div></label></div><div class="mt-2 text-xs text-text-secondary">رصيد USDT: <span id="usdt-balance" class="font-mono">...</span></div></div>
        </section>
        <!-- [تحسين V9.11] بوصلة اتجاه BTC على الفريمات الثلاث (API مجاني) -->
        <section class="card p-4 mb-6">
            <div class="flex items-center justify-between mb-3 flex-wrap gap-2">
                <h3 class="font-bold text-lg text-text-secondary">🧭 بوصلة اتجاه BTC</h3>
                <div class="text-xs text-text-secondary font-mono" id="btc-trend-updated" dir="ltr">--</div>
            </div>
            <div class="grid grid-cols-1 lg:grid-cols-4 gap-4 items-stretch">
                <div class="lg:col-span-1 flex flex-col justify-center items-center bg-black/40 rounded-lg border border-border-color p-3">
                    <div id="btc-overall-arrow" class="text-4xl leading-none">⏳</div>
                    <div id="btc-overall-label" class="text-xl font-bold mt-2 text-text-secondary">جاري التحليل...</div>
                    <div class="w-full mt-2 h-2 bg-gray-800 rounded-full relative overflow-hidden" dir="ltr">
                        <div class="absolute left-1/2 top-0 w-px h-full bg-gray-600"></div>
                        <div id="btc-overall-bar" class="absolute top-0 h-full rounded-full transition-all duration-700" style="left:50%;width:0"></div>
                    </div>
                    <div id="btc-overall-agreement" class="text-xs text-text-secondary mt-2">--</div>
                    <div id="btc-price" class="font-mono text-sm mt-1 text-accent-green" dir="ltr">--</div>
                </div>
                <div class="lg:col-span-3 grid grid-cols-1 md:grid-cols-3 gap-3" id="btc-tf-grid">
                    <div class="text-text-secondary text-sm text-center py-6">جاري أول تحليل للفريمات (15م / 1س / 4س)...</div>
                </div>
            </div>
        </section>
        <!-- [تحسين V9.9] شريط مراقبة النظام الحي: وزن API، الاتصال، قاطع الحماية -->
        <section class="card p-3 md:p-4 mb-6">
            <div class="flex flex-wrap items-center gap-x-6 gap-y-3 text-sm">
                <span class="font-mono text-accent-green" dir="ltr">[SYS.MONITOR]</span>
                <div class="flex items-center gap-2">
                    <span class="text-text-secondary">وزن API/دقيقة:</span>
                    <span id="sys-weight" class="font-mono text-accent-yellow">--</span>
                    <div class="sys-bar"><div id="sys-weight-bar" style="width:0%"></div></div>
                </div>
                <div class="flex items-center gap-2"><span class="text-text-secondary">الاتصال:</span><span id="sys-conn" class="font-mono">--</span></div>
                <div class="flex items-center gap-2"><span class="text-text-secondary">PnL اليوم:</span><span id="sys-pnl" class="font-mono">--</span></div>
                <div class="flex items-center gap-2"><span class="text-text-secondary">قاطع الحماية:</span><span id="sys-lossguard" class="font-mono">--</span></div>
                <div class="flex items-center gap-2"><span class="text-text-secondary">التخزين:</span><span id="sys-storage" class="font-mono text-accent-blue">--</span></div>
                <div class="flex items-center gap-2"><span class="text-text-secondary">العملات:</span><span id="sys-symbols" class="font-mono">--</span></div>
                <div class="flex items-center gap-2"><span class="text-text-secondary">مدة التشغيل:</span><span id="sys-uptime" class="font-mono" dir="ltr">--</span></div>
            </div>
        </section>
        <div class="mb-4 border-b border-border-color"><nav class="flex space-x-6 space-x-reverse -mb-px">
            <button onclick="showTab('signals', this)" class="tab-btn active text-white py-3 px-1 font-semibold">الصفقات</button>
            <button onclick="showTab('stats', this)" class="tab-btn text-text-secondary hover:text-white py-3 px-1">الإحصائيات</button>
            <button onclick="showTab('settings', this)" class="tab-btn text-text-secondary hover:text-white py-3 px-1">الإعدادات</button>
            <button onclick="showTab('notifications', this)" class="tab-btn text-text-secondary hover:text-white py-3 px-1">الإشعارات</button>
            <button onclick="showTab('rejections', this)" class="tab-btn text-text-secondary hover:text-white py-3 px-1">الصفقات المرفوضة</button>
        </nav></div>
        <main>
            <div id="signals-tab" class="tab-content"><div class="overflow-x-auto card p-0"><table class="min-w-full text-sm text-right"><thead class="border-b border-border-color bg-black/20"><tr><th class="p-4 font-semibold">العملة</th><th class="p-4 font-semibold">الربح/الخسارة</th><th class="p-4 font-semibold">الدخول/الحالي/الهدف</th><th class="p-4 font-semibold">تحديث الهدف</th><th class="p-4 font-semibold">إجراء</th></tr></thead><tbody id="signals-table"></tbody></table></div></div>
            <div id="stats-tab" class="tab-content hidden"><div id="stats-container" class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4"></div></div>
            <div id="settings-tab" class="tab-content hidden">
                <div class="card p-6">
                    <h4 class="text-lg font-bold mb-4 text-text-secondary">الإعدادات العامة</h4>
                    <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                        <div>
                            <label for="risk-percent" class="block text-sm font-medium text-text-secondary mb-1">نسبة المخاطرة للصفقة (%)</label>
                            <input type="number" id="risk-percent" name="risk_percent" step="0.1" class="input-field w-full">
                        </div>
                        <div>
                            <label for="ob-ratio" class="block text-sm font-medium text-text-secondary mb-1">نسبة فلتر دفتر الطلبات</label>
                            <input type="number" id="ob-ratio" name="ob_ratio" step="0.1" class="input-field w-full">
                        </div>
                        <div>
                            <label for="vol-multiplier" class="block text-sm font-medium text-text-secondary mb-1">مضاعف فلتر حجم التداول</label>
                            <input type="number" id="vol-multiplier" name="vol_multiplier" step="0.01" class="input-field w-full">
                        </div>
                         <div>
                            <label for="min-profit" class="block text-sm font-medium text-text-secondary mb-1">أدنى ربح مستهدف (%)</label>
                            <input type="number" id="min-profit" name="min_profit" step="0.1" class="input-field w-full">
                        </div>
                    </div>
                    
                    <hr class="border-border-color my-6">
                    
                    <h4 class="text-lg font-bold mb-4 text-text-secondary">الاستراتيجيات المفعّلة</h4>
                    <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6 mt-6">
                        <div class="flex items-center justify-between p-3 bg-black/20 rounded-lg strategy-toggle">
                            <span class="font-semibold">BB+Stoch (Enhanced)</span>
                            <label class="flex items-center cursor-pointer"><div class="relative"><input type="checkbox" id="bb-stoch-strategy-toggle" class="sr-only"><div class="toggle-bg block bg-gray-600 w-12 h-7 rounded-full"></div></div></label>
                        </div>
                        <div class="flex items-center justify-between p-3 bg-black/20 rounded-lg strategy-toggle">
                            <span class="font-semibold">MACD+EMA</span>
                            <label class="flex items-center cursor-pointer"><div class="relative"><input type="checkbox" id="macd-ema-strategy-toggle" class="sr-only"><div class="toggle-bg block bg-gray-600 w-12 h-7 rounded-full"></div></div></label>
                        </div>
                        <div class="flex items-center justify-between p-3 bg-black/20 rounded-lg strategy-toggle-new">
                            <span class="font-semibold">EMA+RSI Cross</span>
                            <label class="flex items-center cursor-pointer"><div class="relative"><input type="checkbox" id="ema-rsi-strategy-toggle" class="sr-only"><div class="toggle-bg block bg-gray-600 w-12 h-7 rounded-full"></div></div></label>
                        </div>
                        <div class="flex items-center justify-between p-3 bg-black/20 rounded-lg strategy-toggle-new">
                            <span class="font-semibold">Pullback MACD</span>
                            <label class="flex items-center cursor-pointer"><div class="relative"><input type="checkbox" id="pullback-strategy-toggle" class="sr-only"><div class="toggle-bg block bg-gray-600 w-12 h-7 rounded-full"></div></div></label>
                        </div>
                        <div class="flex items-center justify-between p-3 bg-black/20 rounded-lg strategy-toggle-new">
                            <span class="font-semibold">BB Squeeze</span>
                            <label class="flex items-center cursor-pointer"><div class="relative"><input type="checkbox" id="bb-squeeze-strategy-toggle" class="sr-only"><div class="toggle-bg block bg-gray-600 w-12 h-7 rounded-full"></div></div></label>
                        </div>
                        <div class="flex items-center justify-between p-3 bg-black/20 rounded-lg strategy-toggle-momentum">
                            <span class="font-semibold">زخم صعودي</span>
                            <label class="flex items-center cursor-pointer"><div class="relative"><input type="checkbox" id="bullish-momentum-strategy-toggle" class="sr-only"><div class="toggle-bg block bg-gray-600 w-12 h-7 rounded-full"></div></div></label>
                        </div>
                        <div class="flex items-center justify-between p-3 bg-black/20 rounded-lg strategy-toggle-momentum">
                            <span class="font-semibold">S/R Breakout (Enhanced)</span>
                            <label class="flex items-center cursor-pointer"><div class="relative"><input type="checkbox" id="sr-breakout-strategy-toggle" class="sr-only"><div class="toggle-bg block bg-gray-600 w-12 h-7 rounded-full"></div></div></label>
                        </div>
                    </div>

                    <div class="mt-8 text-left">
                        <button onclick="saveSettings()" class="save-btn">حفظ الإعدادات</button>
                    </div>
                    <div id="settings-feedback" class="mt-4 text-center"></div>
                </div>
            </div>
            <div id="notifications-tab" class="tab-content hidden"><div id="notifications-list" class="card p-4 max-h-[60vh] overflow-y-auto space-y-2"></div></div>
            <div id="rejections-tab" class="tab-content hidden"><div id="rejections-list" class="card p-4 max-h-[60vh] overflow-y-auto space-y-2"></div></div>
        </main>
    </div>
<script>
// [تحسين V9.9] خلفية المطر الرقمي — خفيفة على المعالج وتحترم تفضيل تقليل الحركة
(function(){
    const c = document.getElementById('matrix-rain'); if(!c) return;
    if (window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches) { c.style.display='none'; return; }
    const ctx = c.getContext('2d');
    const glyphs = '01アイウエオカキクケコサシスセソ$#*+=<>010';
    let w, h, cols, drops;
    function resize(){ w = c.width = window.innerWidth; h = c.height = window.innerHeight; cols = Math.floor(w/16); drops = Array(cols).fill(0).map(() => Math.floor(Math.random()*h/16)); }
    resize(); window.addEventListener('resize', resize);
    setInterval(() => {
        ctx.fillStyle = 'rgba(2,4,3,0.14)'; ctx.fillRect(0,0,w,h);
        ctx.fillStyle = '#00ff41'; ctx.font = '14px "Share Tech Mono", monospace';
        for (let i=0;i<cols;i++){
            ctx.fillText(glyphs[Math.floor(Math.random()*glyphs.length)], i*16, drops[i]*16);
            if (drops[i]*16 > h && Math.random() > 0.972) drops[i] = 0;
            drops[i]++;
        }
    }, 70);
})();

let confirmCallback = null;
const modal = {
    overlay: document.getElementById('modal-overlay'),
    title: document.getElementById('modal-title'),
    body: document.getElementById('modal-body'),
    confirmBtn: document.getElementById('modal-confirm'),
    cancelBtn: document.getElementById('modal-cancel'),
};
modal.cancelBtn.onclick = () => { modal.overlay.classList.add('hidden'); };
modal.confirmBtn.onclick = () => { if(confirmCallback) confirmCallback(); modal.overlay.classList.add('hidden'); };

function showConfirmation(title, bodyText, onConfirm) {
    modal.title.textContent = title;
    modal.body.textContent = bodyText;
    confirmCallback = onConfirm;
    modal.overlay.classList.remove('hidden');
    modal.overlay.classList.add('flex');
}
function showTab(tabId, el) {
    document.querySelectorAll('.tab-content').forEach(t => t.classList.add('hidden'));
    document.getElementById(tabId + '-tab').classList.remove('hidden');
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active', 'text-white'));
    el.classList.add('active', 'text-white');
}
async function fetchData(url) { try { const r = await fetch(url); return r.ok ? await r.json() : null; } catch (e) { console.error('Fetch Error:', e); return null; } }

function updateBtcTrend() {
    fetchData('/api/btc_trend').then(data => {
        if (!data) return;
        if (data.status === 'init') return; // تبقى رسالة "جاري أول تحليل"
        const cMap = {green: 'text-accent-green', red: 'text-accent-red', yellow: 'text-accent-yellow'};
        const barMap = {green: '#00ff41', red: '#ff3b3b', yellow: '#ffd60a'};
        // البطاقة العامة
        const ov = data.overall || {};
        const arrowEl = document.getElementById('btc-overall-arrow');
        arrowEl.textContent = ov.arrow || '▬';
        arrowEl.className = `text-4xl leading-none ${cMap[ov.color] || 'text-text-secondary'}`;
        const labelEl = document.getElementById('btc-overall-label');
        labelEl.textContent = ov.label || '--';
        labelEl.className = `text-xl font-bold mt-2 ${cMap[ov.color] || 'text-text-secondary'}`;
        const pct = Math.min(100, Math.abs(ov.score || 0)) / 2; // نصف العرض لكل اتجاه
        const bar = document.getElementById('btc-overall-bar');
        if ((ov.score || 0) >= 0) { bar.style.left = '50%'; bar.style.right = 'auto'; }
        else { bar.style.left = (50 - pct) + '%'; bar.style.right = 'auto'; }
        bar.style.width = pct + '%';
        bar.style.background = barMap[ov.color] || '#888';
        document.getElementById('btc-overall-agreement').textContent =
            (data.stale ? '⏳ بيانات قديمة | ' : '') + (ov.agreement || '--');
        document.getElementById('btc-price').textContent =
            data.tfs?.['15m']?.price ? 'BTC ' + parseFloat(data.tfs['15m'].price).toLocaleString('en-US', {maximumFractionDigits: 1}) + '$' : '--';
        const upd = document.getElementById('btc-trend-updated');
        if (data.updated) { const d = new Date(data.updated); upd.textContent = 'تحديث: ' + d.toLocaleTimeString('ar-EG', {hour: '2-digit', minute: '2-digit'}); }
        // بطاقات الفريمات الثلاث
        const grid = document.getElementById('btc-tf-grid');
        const tfNames = {'15m': '15 دقيقة', '1h': 'ساعة', '4h': '4 ساعات'};
        grid.innerHTML = ['15m', '1h', '4h'].map(tf => {
            const t = data.tfs?.[tf];
            if (!t) return '';
            const tp = Math.min(100, Math.abs(t.score)) / 2;
            const pos = t.score >= 0 ? `left:50%;width:${tp}%` : `left:${50 - tp}%;width:${tp}%`;
            const mom = t.momentum_pct != null ? (t.momentum_pct >= 0 ? '+' : '') + t.momentum_pct + '%' : '--';
            const momClass = (t.momentum_pct || 0) >= 0 ? 'text-accent-green' : 'text-accent-red';
            return `
            <div class="bg-black/40 rounded-lg border border-border-color p-3 flex flex-col items-center justify-center gap-1">
                <div class="text-xs font-mono text-text-secondary" dir="ltr">${tf} · ${tfNames[tf]}</div>
                <div class="text-3xl leading-none ${cMap[t.color] || ''}">${t.arrow || '▬'}</div>
                <div class="font-bold ${cMap[t.color] || 'text-text-secondary'}">${t.label || '--'}</div>
                <div class="w-full h-1.5 bg-gray-800 rounded-full relative overflow-hidden mt-1" dir="ltr">
                    <div class="absolute left-1/2 top-0 w-px h-full bg-gray-600"></div>
                    <div class="absolute top-0 h-full rounded-full transition-all duration-700" style="${pos};background:${barMap[t.color] || '#888'}"></div>
                </div>
                <div class="flex justify-between w-full text-xs font-mono mt-1" dir="ltr">
                    <span class="text-text-secondary">RSI ${t.rsi != null ? t.rsi : '--'}</span>
                    <span class="${momClass}">${mom}</span>
                </div>
            </div>`;
        }).join('');
    });
}
function updateMarketStatus() {
    fetchData('/api/market_status').then(data => {
        if (!data) return;
        document.getElementById('overall-regime').textContent = (data.market_state?.overall_regime || 'UNCERTAIN').replace(/_/g, ' ');
        document.getElementById('open-trades-count').textContent = `${data.open_trades_count} / ${data.max_open_trades}`;
        const lights = document.getElementById('trend-lights-container');
        lights.innerHTML = '';
        ['15m', '1h', '4h'].forEach(tf => {
            const trendInfo = data.market_state?.trend_details_by_tf[tf];
            const trend = trendInfo?.trend || 'Uncertain';
            let c = trend.includes('Uptrend') ? 'light-on-green' : trend.includes('Downtrend') ? 'light-on-red' : 'light-on-yellow';
            lights.innerHTML += `<div class="flex items-center gap-2"><div class="trend-light ${c}"></div><span class="text-sm font-bold text-text-secondary">${tf}</span></div>`;
        });
        const sessions = document.getElementById('active-sessions-list');
        sessions.innerHTML = data.active_sessions.length > 0 ? data.active_sessions.map(s => `<span class="bg-accent-blue/20 text-accent-blue text-xs font-bold px-2 py-1 rounded">${s}</span>`).join('') : `<span class="bg-gray-700 text-text-secondary text-xs font-bold px-2 py-1 rounded">لا توجد</span>`;

        const tradeToggle = document.getElementById('trading-toggle'), tradeText = document.getElementById('trading-status-text');
        tradeToggle.checked = data.is_trading_enabled;
        tradeText.textContent = data.is_trading_enabled ? 'مُفعَّل' : 'غير مُفعَّل';
        tradeText.className = `font-bold text-lg ${data.is_trading_enabled ? 'text-accent-green' : 'text-accent-red'}`;
        document.getElementById('usdt-balance').textContent = (data.usdt_balance != null && !isNaN(parseFloat(data.usdt_balance))) ? parseFloat(data.usdt_balance).toFixed(2) : 'N/A';

        if(data.settings) {
            document.getElementById('risk-percent').value = data.settings.risk_percent;
            document.getElementById('ob-ratio').value = data.settings.ob_ratio;
            document.getElementById('vol-multiplier').value = data.settings.vol_multiplier;
            document.getElementById('min-profit').value = data.settings.min_profit;
            document.getElementById('bb-stoch-strategy-toggle').checked = data.settings.use_bb_stoch_strategy;
            document.getElementById('macd-ema-strategy-toggle').checked = data.settings.use_macd_ema_strategy;
            document.getElementById('ema-rsi-strategy-toggle').checked = data.settings.use_ema_rsi_strategy;
            document.getElementById('pullback-strategy-toggle').checked = data.settings.use_pullback_strategy;
            document.getElementById('bb-squeeze-strategy-toggle').checked = data.settings.use_bb_squeeze_strategy;
            document.getElementById('bullish-momentum-strategy-toggle').checked = data.settings.use_bullish_momentum_strategy;
            document.getElementById('sr-breakout-strategy-toggle').checked = data.settings.use_sr_breakout_strategy;
        }
    });
}
function updateSignals() {
    fetchData('/api/signals').then(data => {
        if (!data) return;
        const tableBody = document.getElementById('signals-table');
        tableBody.innerHTML = '';
        data.filter(s => ['open', 'updated'].includes(s.status)).forEach(s => {
            const profit = parseFloat(s.profit_percentage || 0);
            const pClass = profit > 0 ? 'text-accent-green' : profit < 0 ? 'text-accent-red' : 'text-text-secondary';
            const entry = parseFloat(s.entry_price);
            const current = parseFloat(s.current_price || entry);
            const currentTarget = parseFloat(s.target_price);
            const stopLoss = parseFloat(s.stop_loss);
            const sliderMax = Math.max(currentTarget, current * 1.15);
            const pricePrecision = parseInt(s.price_precision, 10);
            const step = 1 / Math.pow(10, pricePrecision);

            tableBody.innerHTML += `
            <tr class="border-b border-border-color hover:bg-white/5">
                <td class="p-4 font-bold">${s.symbol}<br><span class="text-xs text-text-secondary">${s.strategy_name.replace(/_/g, ' ')}</span></td>
                <td class="p-4 font-mono ${pClass}">${profit.toFixed(2)}%</td>
                <td class="p-4 font-mono text-xs">
                    <div><span class="text-text-secondary">الدخول:</span> ${entry.toFixed(pricePrecision)}</div>
                    <div><span class="text-accent-blue">الحالي:</span> ${current.toFixed(pricePrecision)}</div>
                    <div><span class="text-accent-green">الهدف:</span> ${currentTarget.toFixed(pricePrecision)}</div>
                </td>
                <td class="p-4 min-w-[250px]">
                    <div class="flex items-center gap-2">
                        <input type="range" id="tp-slider-${s.id}" class="tp-slider flex-grow" 
                               min="${stopLoss}" max="${sliderMax}" step="${step}" value="${currentTarget}"
                               oninput="updateSliderValue(this, ${s.id}, ${pricePrecision})">
                        <span id="tp-value-${s.id}" class="font-mono text-accent-yellow text-sm w-24 text-center">
                            ${currentTarget.toFixed(pricePrecision)}
                        </span>
                    </div>
                </td>
                <td class="p-4">
                    <button onclick="saveNewTarget(${s.id}, ${pricePrecision})" class="bg-blue-600 hover:bg-blue-700 text-white font-bold py-1 px-3 rounded text-xs mb-2 w-full">حفظ الهدف</button>
                    <button onclick="manualClose(${s.id}, '${s.symbol}')" class="bg-red-600 hover:bg-red-700 text-white font-bold py-1 px-3 rounded text-xs w-full">إغلاق</button>
                </td>
            </tr>`;
        });
    });
}
function updateSliderValue(slider, signalId, precision) {
    const valueSpan = document.getElementById(`tp-value-${signalId}`);
    valueSpan.textContent = parseFloat(slider.value).toFixed(precision);
}
function saveNewTarget(signalId, precision) {
    const slider = document.getElementById(`tp-slider-${signalId}`);
    const newValue = parseFloat(slider.value);
    showConfirmation('تأكيد تحديث الهدف', `هل تريد تغيير الهدف إلى ${newValue.toFixed(precision)}؟`, () => {
        fetch(`/api/signals/update_target/${signalId}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ new_target: newValue })
        })
        .then(res => res.json())
        .then(data => {
            if (data.success) {
                console.log("Target updated successfully");
                updateSignals();
            } else {
                alert(`فشل تحديث الهدف: ${data.message}`);
            }
        });
    });
}
function updateStats() {
    fetchData('/api/stats').then(data => {
        if (!data) return;
        const container = document.getElementById('stats-container');
        if (data.error) {
            container.innerHTML = `<div class="card p-4 text-center col-span-full text-accent-red">${data.error}</div>`;
            return;
        }
        container.innerHTML = `<div class="card p-4 text-center"><h4 class="text-text-secondary">صافي الربح</h4><div class="text-2xl font-bold ${data.net_profit_usdt >= 0 ? 'text-accent-green' : 'text-accent-red'}">${parseFloat(data.net_profit_usdt).toFixed(2)}</div></div><div class="card p-4 text-center"><h4 class="text-text-secondary">معدل الربح</h4><div class="text-2xl font-bold">${parseFloat(data.win_rate).toFixed(2)}%</div></div><div class="card p-4 text-center"><h4 class="text-text-secondary">عامل الربح</h4><div class="text-2xl font-bold">${data.profit_factor === 'Infinity' ? '∞' : parseFloat(data.profit_factor).toFixed(2)}</div></div><div class="card p-4 text-center"><h4 class="text-text-secondary">الصفقات المغلقة</h4><div class="text-2xl font-bold">${data.total_closed_trades}</div></div>`;
    });
}
function updateNotifications() {
    fetchData('/api/notifications').then(data => {
        if (!data) return;
        document.getElementById('notifications-list').innerHTML = data.map(n => `<div class="p-2 border-b border-border-color"><span class="font-mono text-xs text-text-secondary">${new Date(n.timestamp).toLocaleString('ar-EG')}</span>: ${n.message}</div>`).join('');
    });
}
function updateRejections() {
    fetchData('/api/rejection_logs').then(data => {
        if (!data) return;
        document.getElementById('rejections-list').innerHTML = data.map(r => `<div class="p-2 border-b border-border-color"><span class="font-mono text-xs text-text-secondary">${new Date(r.timestamp).toLocaleString('ar-EG')}</span>: <strong class="text-accent-yellow">${r.symbol}</strong> - ${r.reason} <span class="text-xs text-gray-500">${JSON.stringify(r.details)}</span></div>`).join('');
    });
}
function manualClose(signalId, symbol) {
    showConfirmation('تأكيد الإغلاق', `هل أنت متأكد من رغبتك في إغلاق الصفقة لـ ${symbol} يدوياً؟`, () => {
        fetch(`/api/signals/close/${signalId}`, { method: 'POST' })
            .then(res => res.json())
            .then(data => { if(data.success) { updateSignals(); } else { alert(data.message); } });
    });
}
function toggleTrading() { fetch('/api/trading/toggle', { method: 'POST' }).then(() => updateMarketStatus()); }

// [تحسين V9.9] مراقب حالة النظام الحي: وزن الطلبات، الحظر، قاطع الحماية
function fmtUptime(sec){ const d=Math.floor(sec/86400), h=Math.floor((sec%86400)/3600), m=Math.floor((sec%3600)/60), s=sec%60; return `${d}d ${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`; }
function updateSystemStatus() {
    fetchData('/api/system_status').then(data => {
        if (!data || data.error) return;
        const rg = data.rate_guard || {};
        document.getElementById('sys-weight').textContent = `${rg.used_weight_last_min ?? 0} / ${rg.budget_per_min ?? '-'}`;
        const pct = rg.budget_per_min ? Math.min(100, (rg.used_weight_last_min / rg.budget_per_min) * 100) : 0;
        document.getElementById('sys-weight-bar').style.width = pct.toFixed(1) + '%';
        const conn = document.getElementById('sys-conn');
        if (rg.banned_until) { conn.textContent = 'محظور مؤقتًا'; conn.className = 'font-mono text-accent-red'; }
        else if (data.client_ready) { conn.textContent = 'متصل'; conn.className = 'font-mono text-accent-green'; }
        else { conn.textContent = 'غير مهيأ'; conn.className = 'font-mono text-accent-yellow'; }
        const pnl = document.getElementById('sys-pnl');
        pnl.textContent = `${data.daily_pnl_usdt} / -${data.daily_max_loss_usdt}$`;
        pnl.className = 'font-mono ' + (data.daily_pnl_usdt >= 0 ? 'text-accent-green' : 'text-accent-red');
        const lg = document.getElementById('sys-lossguard');
        if (data.daily_loss_limit_hit) { lg.textContent = 'مفعّل!'; lg.className = 'font-mono text-accent-red'; }
        else { lg.textContent = 'سليم'; lg.className = 'font-mono text-accent-green'; }
        const st = document.getElementById('sys-storage');
        st.textContent = data.redis_mode === 'redis' ? 'Redis' : (data.redis_mode === 'memory' ? 'ذاكرة داخلية' : 'غير متصل');
        document.getElementById('sys-symbols').textContent = data.symbols_count + (data.universe_mode === 'dynamic' ? ' ⚡ديناميكية' : ' 📋ثابتة');
        document.getElementById('sys-uptime').textContent = fmtUptime(data.uptime_sec);
    });
}

function saveSettings() {
    const settings = {
        risk_percent: parseFloat(document.getElementById('risk-percent').value),
        ob_ratio: parseFloat(document.getElementById('ob-ratio').value),
        vol_multiplier: parseFloat(document.getElementById('vol-multiplier').value),
        min_profit: parseFloat(document.getElementById('min-profit').value),
        use_bb_stoch_strategy: document.getElementById('bb-stoch-strategy-toggle').checked,
        use_macd_ema_strategy: document.getElementById('macd-ema-strategy-toggle').checked,
        use_ema_rsi_strategy: document.getElementById('ema-rsi-strategy-toggle').checked,
        use_pullback_strategy: document.getElementById('pullback-strategy-toggle').checked,
        use_bb_squeeze_strategy: document.getElementById('bb-squeeze-strategy-toggle').checked,
        use_bullish_momentum_strategy: document.getElementById('bullish-momentum-strategy-toggle').checked,
        use_sr_breakout_strategy: document.getElementById('sr-breakout-strategy-toggle').checked,
    };
    const feedbackEl = document.getElementById('settings-feedback');
    feedbackEl.textContent = 'جاري الحفظ...';
    feedbackEl.className = 'mt-4 text-center text-accent-yellow';

    fetch('/api/settings/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(settings)
    })
    .then(res => res.json())
    .then(data => {
        if (data.success) {
            feedbackEl.textContent = '✅ تم حفظ الإعدادات بنجاح!';
            feedbackEl.className = 'mt-4 text-center text-accent-green';
        } else {
            feedbackEl.textContent = `❌ فشل الحفظ: ${data.message}`;
            feedbackEl.className = 'mt-4 text-center text-accent-red';
        }
        setTimeout(() => { feedbackEl.textContent = ''; }, 3000);
    }).catch(err => {
        feedbackEl.textContent = `❌ خطأ في الشبكة: ${err}`;
        feedbackEl.className = 'mt-4 text-center text-accent-red';
    });
}

document.addEventListener('DOMContentLoaded', () => {
    ['MarketStatus', 'Signals', 'Stats', 'Notifications', 'Rejections', 'SystemStatus', 'BtcTrend'].forEach(f => window[`update${f}`]());
    // [تحسين V9.11.0] إيقاف الاستطلاع عند إخفاء التبويب — يمنع تراكم الطلبات
    // من التبويبات الخلفية ويخفف الضغط على خيوط الخادم (waitress queue)
    const whenVisible = (fn, ms) => setInterval(() => { if (!document.hidden) fn(); }, ms);
    whenVisible(updateMarketStatus, 5000); whenVisible(updateSignals, 7000); whenVisible(updateStats, 60000);
    whenVisible(updateNotifications, 15000); whenVisible(updateRejections, 15000); whenVisible(updateSystemStatus, 5000);
    whenVisible(updateBtcTrend, 30000);  // [تحسين V9.11] البوصلة تُحدّث كل 30 ثانية
});
</script>
</body></html>
"""

@app.route('/')
def home(): return render_template_string(get_dashboard_html())

@app.route('/health')
def health_check():
    """[تحسين V9.8] نقطة فحص صحة خفيفة لمراقبة الخدمة على Render وأدوات Uptime."""
    return jsonify({"status": "ok", "version": "V9.11.0", "time": datetime.now(timezone.utc).isoformat()})

# --- [تحسين V9.9] نقطة حالة النظام: وزن الطلبات، الحظر، قاطع الحماية، التخزين ---
@app.route('/api/system_status')
def api_system_status():
    try:
        snap = rate_guard.snapshot()
        try: daily_hit = is_daily_loss_limit_hit()
        except Exception: daily_hit = False
        with daily_pnl_lock:
            pnl = round(daily_realized_pnl_usdt, 2)
        with signal_cache_lock:
            open_count = len(open_signals_cache)
        is_real_redis = False
        try:
            is_real_redis = redis_client is not None and not isinstance(redis_client, InMemoryRedis)
        except Exception:
            pass
        return jsonify({
            'version': 'V9.11.0',
            'client_ready': bool(client),
            'rate_guard': snap,
            'daily_pnl_usdt': pnl,
            'daily_max_loss_usdt': DAILY_MAX_LOSS_USDT,
            'daily_loss_limit_hit': daily_hit,
            'lookback_days': SIGNAL_GENERATION_LOOKBACK_DAYS,
            'symbols_count': len(validated_symbols_to_scan),
            'open_trades': open_count,
            'redis_mode': 'redis' if is_real_redis else ('memory' if redis_client is not None else 'none'),
            'uptime_sec': int(time.time() - BOOT_TIME),
            # [تحسين V9.10] حالة القائمة الديناميكية
            'universe_mode': universe_source,
            'universe_refresh_min': DYNAMIC_UNIVERSE_REFRESH_MIN if USE_DYNAMIC_UNIVERSE else 0,
            'universe_updated_sec_ago': int(time.time() - universe_last_refresh) if universe_last_refresh else None,
            'universe_top_preview': universe_meta.get('top_preview', []) if isinstance(universe_meta, dict) else [],
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/market_status')
def get_market_status():
    with market_state_lock: state_copy = dict(current_market_state)
    with trading_status_lock: is_enabled = is_trading_enabled
    active_sessions, _, _ = get_session_state()
    # [تحسين V9.11.0] قراءة الرصيد من الكاش فقط — لا نداء Binance من خيوط الويب أبدًا
    # (كان يستهلك خيط waitress أثناء انشغال الحارس/الحظر ويسبب تراكم Task queue)
    usdt_balance = get_cached_usdt_balance()
    if usdt_balance is None: usdt_balance = 'N/A'

    with risk_per_trade_lock: risk = RISK_PER_TRADE_PERCENT
    with order_book_ratio_lock: ob_ratio = ORDER_BOOK_MIN_BID_ASK_RATIO
    with volume_filter_lock: vol_mult = VOLUME_FILTER_MULTIPLIER
    with bb_stoch_strategy_lock: use_bb_stoch = USE_BB_STOCH_STRATEGY
    with macd_ema_strategy_lock: use_macd_ema = USE_MACD_EMA_STRATEGY
    with ema_rsi_strategy_lock: use_ema_rsi = USE_EMA_RSI_STRATEGY
    with pullback_strategy_lock: use_pullback = USE_PULLBACK_STRATEGY
    with bb_squeeze_strategy_lock: use_bb_squeeze = USE_BB_SQUEEZE_STRATEGY
    with bullish_momentum_strategy_lock: use_bullish_momentum = USE_BULLISH_MOMENTUM_STRATEGY
    with sr_breakout_strategy_lock: use_sr_breakout = USE_SR_BREAKOUT_STRATEGY
    with signal_cache_lock: open_trades = len(open_signals_cache)


    return jsonify({
        "market_state": state_copy, "active_sessions": active_sessions, "usdt_balance": usdt_balance,
        "is_trading_enabled": is_enabled,
        "open_trades_count": open_trades, "max_open_trades": MAX_OPEN_TRADES,
        "settings": {
            "risk_percent": risk, "ob_ratio": ob_ratio, "vol_multiplier": vol_mult,
            "min_profit": MIN_PROFIT_PERCENT,
            "use_bb_stoch_strategy": use_bb_stoch,
            "use_macd_ema_strategy": use_macd_ema,
            "use_ema_rsi_strategy": use_ema_rsi, "use_pullback_strategy": use_pullback,
            "use_bb_squeeze_strategy": use_bb_squeeze,
            "use_bullish_momentum_strategy": use_bullish_momentum,
            "use_sr_breakout_strategy": use_sr_breakout,
        }
    })

@app.route('/api/btc_trend')
def api_btc_trend():
    """[تحسين V9.11] بوصلة اتجاه BTC — تقرأ الكاش فقط بلا أي نداء شبكي من خيوط الويب."""
    data = _btc_trend_cache['data']
    if not data:
        return jsonify({'status': 'init', 'message': 'جاري أول تحليل للفريمات...'})
    return jsonify(data)

@app.route('/api/stats')
def get_stats():
    if not check_db_connection() or not conn:
        return jsonify({"error": "DB connection failed"}), 500
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT profit_percentage, is_real_trade, original_quantity, entry_price FROM signals WHERE status = 'closed';")
            closed_trades = cur.fetchall()

        if not closed_trades:
            return jsonify({"net_profit_usdt": 0, "win_rate": 0, "profit_factor": 0, "total_closed_trades": 0})

        total_net_profit_usdt = sum(
            ((float(t['profit_percentage']) - (2 * TRADING_FEE_PERCENT)) / 100) * (float(t['original_quantity']) * float(t['entry_price']) if t.get('is_real_trade') and t.get('original_quantity') and t.get('entry_price') else STATS_TRADE_SIZE_USDT)
            for t in closed_trades
        )
        wins = [float(s['profit_percentage']) for s in closed_trades if float(s['profit_percentage']) > 0]
        losses = [float(s['profit_percentage']) for s in closed_trades if float(s['profit_percentage']) < 0]
        win_rate = (len(wins) / len(closed_trades) * 100) if closed_trades else 0.0
        total_loss = abs(sum(losses))
        profit_factor = sum(wins) / total_loss if total_loss > 0 else "Infinity"

        return jsonify({
            "net_profit_usdt": total_net_profit_usdt, "win_rate": win_rate,
            "profit_factor": profit_factor, "total_closed_trades": len(closed_trades)
        })
    except Exception as e:
        logger.error(f"❌ [API إحصائيات] خطأ: {e}", exc_info=True)
        return jsonify({"error": "Internal server error fetching stats"}), 500

@app.route('/api/signals')
def get_signals():
    if not (check_db_connection() and redis_client):
        return jsonify({"error": "Service connection failed"}), 500
    try:
        current_prices = redis_client.hgetall(REDIS_PRICES_HASH_NAME)
        with signal_cache_lock:
            signals_copy = list(open_signals_cache.values())
        
        for signal in signals_copy:
            symbol_info = exchange_info_map.get(signal['symbol'])
            signal['price_precision'] = symbol_info.get('pricePrecision', 2) if symbol_info else 2
            
            current_price = current_prices.get(signal['symbol'])
            if current_price:
                signal['current_price'] = current_price
                signal['profit_percentage'] = ((float(current_price) - float(signal['entry_price'])) / float(signal['entry_price'])) * 100

        return jsonify(signals_copy)
    except Exception as e:
        logger.error(f"❌ [API إشارات] خطأ: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/notifications')
def get_notifications():
    with notifications_lock: return jsonify(list(notifications_cache))

@app.route('/api/rejection_logs')
def get_rejection_logs():
    with rejection_logs_lock: return jsonify(list(rejection_logs_cache))

@app.route('/api/trading/toggle', methods=['POST'])
def toggle_trading_status():
    global is_trading_enabled
    with trading_status_lock:
        is_trading_enabled = not is_trading_enabled
        status_msg = "مُفعّل" if is_trading_enabled else "مُعطّل"
        log_and_notify('warning', f"🚨 تم تغيير حالة التداول الحقيقي إلى: {status_msg}", "TRADING_STATUS_CHANGE")
        return jsonify({"message": f"Trading status set to {status_msg}"})

@app.route('/api/settings/update', methods=['POST'])
def update_settings():
    global RISK_PER_TRADE_PERCENT, ORDER_BOOK_MIN_BID_ASK_RATIO, VOLUME_FILTER_MULTIPLIER, \
           MIN_PROFIT_PERCENT, USE_BB_STOCH_STRATEGY, USE_MACD_EMA_STRATEGY, USE_EMA_RSI_STRATEGY, \
           USE_PULLBACK_STRATEGY, USE_BB_SQUEEZE_STRATEGY, USE_BULLISH_MOMENTUM_STRATEGY, USE_SR_BREAKOUT_STRATEGY
    try:
        data = request.get_json()
        
        with risk_per_trade_lock: RISK_PER_TRADE_PERCENT = float(data.get('risk_percent', RISK_PER_TRADE_PERCENT))
        with order_book_ratio_lock: ORDER_BOOK_MIN_BID_ASK_RATIO = float(data.get('ob_ratio', ORDER_BOOK_MIN_BID_ASK_RATIO))
        with volume_filter_lock: VOLUME_FILTER_MULTIPLIER = float(data.get('vol_multiplier', VOLUME_FILTER_MULTIPLIER))
        MIN_PROFIT_PERCENT = float(data.get('min_profit', MIN_PROFIT_PERCENT))

        with bb_stoch_strategy_lock: USE_BB_STOCH_STRATEGY = bool(data.get('use_bb_stoch_strategy', USE_BB_STOCH_STRATEGY))
        with macd_ema_strategy_lock: USE_MACD_EMA_STRATEGY = bool(data.get('use_macd_ema_strategy', USE_MACD_EMA_STRATEGY))
        with ema_rsi_strategy_lock: USE_EMA_RSI_STRATEGY = bool(data.get('use_ema_rsi_strategy', USE_EMA_RSI_STRATEGY))
        with pullback_strategy_lock: USE_PULLBACK_STRATEGY = bool(data.get('use_pullback_strategy', USE_PULLBACK_STRATEGY))
        with bb_squeeze_strategy_lock: USE_BB_SQUEEZE_STRATEGY = bool(data.get('use_bb_squeeze_strategy', USE_BB_SQUEEZE_STRATEGY))
        with bullish_momentum_strategy_lock: USE_BULLISH_MOMENTUM_STRATEGY = bool(data.get('use_bullish_momentum_strategy', USE_BULLISH_MOMENTUM_STRATEGY))
        with sr_breakout_strategy_lock: USE_SR_BREAKOUT_STRATEGY = bool(data.get('use_sr_breakout_strategy', USE_SR_BREAKOUT_STRATEGY))

        log_and_notify('info', f"⚙️ تم تحديث الإعدادات من لوحة التحكم.", "SETTINGS_UPDATE")
        return jsonify({"success": True, "message": "Settings updated successfully"})
    except Exception as e:
        logger.error(f"❌ [API إعدادات] فشل تحديث الإعدادات: {e}", exc_info=True)
        return jsonify({"success": False, "message": str(e)}), 400


@app.route('/api/signals/close/<int:signal_id>', methods=['POST'])
def manual_close_trade_endpoint(signal_id):
    if not redis_client or not client: return jsonify({"success": False, "message": "Services not ready"}), 503
    with signal_cache_lock:
        signal_to_close = next((s for s in open_signals_cache.values() if s['id'] == signal_id), None)
    if not signal_to_close: return jsonify({"success": False, "message": "Signal not found"}), 404
    try:
        current_price = float(redis_client.hget(REDIS_PRICES_HASH_NAME, signal_to_close['symbol']))
    except (TypeError, ValueError):
        try: current_price = float(safe_get_symbol_ticker(symbol=signal_to_close['symbol'])['price'])
        except Exception as e: return jsonify({"success": False, "message": f"Could not fetch price: {e}"}), 500

    if close_signal(signal_id, current_price, 'manual'):
        return jsonify({"success": True, "message": "Signal closed."})
    else:
        return jsonify({"success": False, "message": "Failed to close signal."}), 500

@app.route('/api/signals/update_target/<int:signal_id>', methods=['POST'])
def update_target_price(signal_id):
    if not check_db_connection() or not conn:
        return jsonify({"success": False, "message": "Database not connected"}), 503
    
    data = request.get_json()
    new_target_price = data.get('new_target')

    if not new_target_price:
        return jsonify({"success": False, "message": "New target price not provided"}), 400

    try:
        new_target_price = float(new_target_price)
        with signal_cache_lock:
            signal_to_update = next((s for s in open_signals_cache.values() if s['id'] == signal_id), None)
            
            if not signal_to_update:
                return jsonify({"success": False, "message": "Signal not found or already closed"}), 404

            symbol = signal_to_update['symbol']
            old_target = float(signal_to_update['target_price'])
            stop_loss = float(signal_to_update['stop_loss'])

            if new_target_price <= stop_loss:
                return jsonify({"success": False, "message": "Target price cannot be below stop loss"}), 400

            signal_to_update['target_price'] = new_target_price
            
            with conn.cursor() as cur:
                cur.execute("UPDATE signals SET target_price = %s WHERE id = %s", (new_target_price, signal_id))
            conn.commit()

            log_message = f"🖐️ [{symbol}] تم تحديث الهدف يدوياً من {old_target:.4f} إلى {new_target_price:.4f}"
            log_and_notify('warning', log_message, "MANUAL_TP_UPDATE")
            send_telegram_message(log_message)

            return jsonify({"success": True, "message": "Target price updated successfully"})

    except (ValueError, TypeError):
        return jsonify({"success": False, "message": "Invalid target price format"}), 400
    except Exception as e:
        logger.error(f"❌ [API تحديث الهدف] فشل تحديث الهدف لـ ID {signal_id}: {e}", exc_info=True)
        if conn: conn.rollback()
        return jsonify({"success": False, "message": "An internal error occurred"}), 500


# ---------------------- حلقات النظام ----------------------
def analyze_path_for_extension(df: pd.DataFrame) -> bool:
    if df is None or len(df) < 20: return False
    last = df.iloc[-1]
    trend_is_supportive = last.get('adx', 0) > 22
    volume_is_supportive = last.get('relative_volume', 0) > 1.1
    momentum_is_positive = last.get('close', 0) > last.get('ema_21', 0)
    
    should_extend = trend_is_supportive and volume_is_supportive and momentum_is_positive
    logger.info(f"  -> [تحليل المسار] تمديد؟ {should_extend} (الترند داعم: {trend_is_supportive}, حجم التداول داعم: {volume_is_supportive}, الزخم إيجابي: {momentum_is_positive})")
    return should_extend

def find_next_resistance(df: pd.DataFrame, from_price: float) -> Optional[float]:
    if len(df) < 20: return None
    df_slice = df.iloc[-100:]
    resistance_candidates = df_slice[df_slice['high'] == df_slice['high'].rolling(7, center=True).max()]['high']
    next_resistances = resistance_candidates[resistance_candidates > (from_price * 1.001)]
    if not next_resistances.empty:
        return next_resistances.min()
    return None

# --- [تحسين V9.8] قاطع الحماية اليومي وكاش ATR ---
def register_realized_pnl(signal: Dict, entry_price: float, closing_price: float):
    """تسجيل الربح/الخسارة المحقق لكل صفقة مغلقة لتحديث قاطع الحماية اليومي."""
    global daily_realized_pnl_usdt, daily_pnl_date, daily_loss_notified
    try:
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        qty = float(signal.get('original_quantity') or signal.get('quantity') or 0.0)
        if signal.get('is_real_trade') and qty > 0:
            notional = qty * entry_price
        else:
            notional = STATS_TRADE_SIZE_USDT
        pnl_usdt = ((closing_price - entry_price) / entry_price) * notional
        with daily_pnl_lock:
            if daily_pnl_date != today:
                daily_pnl_date, daily_realized_pnl_usdt, daily_loss_notified = today, 0.0, False
            daily_realized_pnl_usdt += pnl_usdt
    except Exception as e:
        logger.error(f"❌ [قاطع الحماية] خطأ في تسجيل الأرباح المحققة: {e}")

def is_daily_loss_limit_hit() -> bool:
    """يتوقف عن فتح صفقات جديدة إذا تجاوزت الخسارة المحققة اليومية الحد المحدد
    (DAILY_MAX_LOSS_USDT). يُعاد التصفير تلقائيًا مع بداية كل يوم UTC."""
    global daily_pnl_date, daily_loss_notified, daily_realized_pnl_usdt
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    hit = False
    with daily_pnl_lock:
        if daily_pnl_date != today:
            daily_pnl_date, daily_realized_pnl_usdt, daily_loss_notified = today, 0.0, False
        hit = daily_realized_pnl_usdt <= -abs(DAILY_MAX_LOSS_USDT)
        should_notify = hit and not daily_loss_notified
        if should_notify:
            daily_loss_notified = True
    if should_notify:
        log_and_notify('critical', (
            f"🛑 قاطع الحماية: الخسارة اليومية ({daily_realized_pnl_usdt:.2f} USDT) تجاوزت الحد "
            f"({-abs(DAILY_MAX_LOSS_USDT):.2f} USDT). تم إيقاف فتح صفقات جديدة حتى بداية اليوم القادم (UTC)."
        ), "DAILY_LOSS_LIMIT")
        send_telegram_message("🛑 *قاطع الحماية اليومي:* تم إيقاف فتح صفقات جديدة بسبب تجاوز حد الخسارة اليومي.")
    return hit

def get_cached_atr(symbol: str) -> Optional[float]:
    """[تحسين V9.8] جلب ATR مع كاش 60 ثانية لتقليل استدعاءات API في حلقة إدارة الصفقات
    (كانت الحلقة تجلب الشموع مع كل تحديث جديد للسعر)."""
    now = time.time()
    with atr_trailing_lock:
        cached = ATR_TRAILING_CACHE.get(symbol)
        if cached and now - cached[0] < 60:
            return cached[1]
    try:
        df_atr = fetch_historical_data(symbol, SIGNAL_GENERATION_TIMEFRAME, ATR_TS_PERIOD + 1)
        if df_atr is None or len(df_atr) < ATR_TS_PERIOD:
            return cached[1] if cached else None
        high_low = df_atr['high'] - df_atr['low']
        high_close_prev = (df_atr['high'] - df_atr['close'].shift()).abs()
        low_close_prev = (df_atr['low'] - df_atr['close'].shift()).abs()
        tr = pd.concat([high_low, high_close_prev, low_close_prev], axis=1).max(axis=1, skipna=False)
        latest_atr = float(tr.ewm(span=ATR_TS_PERIOD, adjust=False).mean().iloc[-1])
        with atr_trailing_lock:
            ATR_TRAILING_CACHE[symbol] = (now, latest_atr)
        return latest_atr
    except Exception as e:
        logger.error(f"❌ [{symbol}] خطأ أثناء حساب ATR للوقف المتحرك: {e}")
        return cached[1] if cached else None

def trade_management_loop():
    logger.info("✅ [مدير الصفقات] بدء حلقة إدارة الصفقات...")
    while True:
        try:
            with signal_cache_lock:
                if not open_signals_cache:
                    time.sleep(5)
                    continue
                signals_to_check = list(open_signals_cache.values())

            if not redis_client:
                time.sleep(5)
                continue

            current_prices = redis_client.hgetall(REDIS_PRICES_HASH_NAME)
            _, session_liquidity, _ = get_session_state()

            for signal in signals_to_check:
                current_price_str = current_prices.get(signal['symbol'])
                if not current_price_str: continue

                current_price = float(current_price_str)
                signal_id, symbol = signal['id'], signal['symbol']
                tp, sl, entry = float(signal['target_price']), float(signal['stop_loss']), float(signal['entry_price'])

                if current_price <= sl:
                    reason = 'atr_trailing_stop' if USE_ATR_TRAILING_STOP and sl > float(signal.get('initial_stop_loss', sl)) else 'stop_loss'
                    close_signal(signal_id, current_price, reason)
                    continue

                if current_price >= tp:
                    if USE_DYNAMIC_JOURNEY and signal.get('journey_state'):
                        journey_state = signal['journey_state']
                        if journey_state.get('is_complete'): continue
                        logger.info(f"🎉 [{symbol}] الهدف عند {tp:.4f} تحقق بسعر {current_price:.4f}")
                        
                        if not journey_state.get('partial_exit_done'):
                            rr_ratio = float(signal.get('rr_ratio', 0.0))
                            partial_exit_percent = 0.6 if rr_ratio >= 2.0 else 0.4
                            
                            # --- [إصلاح] منطق الخروج الجزئي ---
                            if signal.get('is_real_trade') and partial_exit_percent > 0:
                                try:
                                    current_db_quantity = Decimal(str(signal.get('quantity', '0')))
                                    original_quantity = Decimal(str(signal.get('original_quantity', '0')))
                                    desired_exit_quantity = original_quantity * Decimal(str(partial_exit_percent))

                                    base_asset = symbol.replace('USDT', '')
                                    balance_response = safe_get_asset_balance(base_asset)
                                    actual_free_balance = Decimal(balance_response['free'])

                                    logger.info(f"  -> [{symbol}] التحقق من الرصيد للخروج الجزئي. المطلوب: {desired_exit_quantity:.8f}, المسجل: {current_db_quantity:.8f}, الفعلي: {actual_free_balance:.8f}")

                                    quantity_to_sell = min(desired_exit_quantity, current_db_quantity, actual_free_balance)
                                    adjusted_quantity_to_sell = adjust_quantity_to_lot_size(symbol, float(quantity_to_sell))
                                    
                                    if adjusted_quantity_to_sell and adjusted_quantity_to_sell > 0:
                                        sell_order = place_order(symbol, Client.SIDE_SELL, adjusted_quantity_to_sell)
                                        if sell_order:
                                            executed_quantity = Decimal(sell_order.get('executedQty', '0'))
                                            if executed_quantity == 0: executed_quantity = adjusted_quantity_to_sell

                                            remaining_quantity = Decimal(str(signal['quantity'])) - executed_quantity
                                            signal['quantity'] = float(remaining_quantity)
                                            log_and_notify('info', f"↗️ [{symbol}] خروج جزئي ({partial_exit_percent*100}%): بيع {executed_quantity} عند {current_price:.4f}", "PARTIAL_EXIT")
                                            
                                            is_dust = False
                                            if remaining_quantity > 0:
                                                symbol_info = exchange_info_map.get(symbol)
                                                if symbol_info:
                                                    min_notional_filter = next((f for f in symbol_info['filters'] if f['filterType'] in ('MIN_NOTIONAL', 'NOTIONAL')), None)
                                                    if min_notional_filter:
                                                        min_notional = Decimal(min_notional_filter.get('minNotional', min_notional_filter.get('notional', '0')))
                                                        if (remaining_quantity * Decimal(str(current_price))) < min_notional:
                                                            is_dust = True
                                                            logger.warning(f"⚠️ [{symbol}] الكمية المتبقية ({remaining_quantity}) أقل من الحد الأدنى. سيتم إغلاق الصفقة بالكامل.")
                                            
                                            if remaining_quantity <= 0 or is_dust:
                                                close_signal(signal_id, current_price, 'take_profit_full_exit_on_small_size')
                                                continue
                                except Exception as e:
                                    logger.error(f"❌ [{symbol}] خطأ أثناء الخروج الجزئي: {e}", exc_info=True)
                                    continue
                            
                            journey_state['partial_exit_done'] = True
                        
                        journey_state['targets_hit'] = journey_state.get('targets_hit', 0) + 1
                        
                        # [تحسين V9.9.1] 30 يومًا تكفي تمامًا لتحليل امتداد المسار (كانت 100 يومًا = 55 وزنًا لكل استدعاء!)
                        df_analysis = fetch_historical_data(symbol, SIGNAL_GENERATION_TIMEFRAME, 30)
                        if df_analysis is not None:
                            df_with_features = calculate_all_features(df_analysis, None)
                            
                            if analyze_path_for_extension(df_with_features):
                                new_sl = tp
                                next_target_atr_multiplier = 1.45 if session_liquidity == 'HIGH_LIQUIDITY' else 1.25
                                
                                next_tp = find_next_resistance(df_with_features, current_price)
                                
                                if next_tp is None:
                                    last_atr = df_with_features['atr'].iloc[-1]
                                    next_tp = current_price + (last_atr * next_target_atr_multiplier)
                                    logger.info(f"  -> [{symbol}] لم يتم العثور على مقاومة، تم تحديد الهدف التالي باستخدام ATR ({next_target_atr_multiplier}x): {next_tp:.4f}")
                                else:
                                    logger.info(f"  -> [{symbol}] تم العثور على مقاومة تالية عند: {next_tp:.4f}")

                                # [تحسين V9.8] ضمان أن الهدف الجديد يحقق أدنى ربح مستهدف (MIN_PROFIT_PERCENT) ويبقى بعد السعر الحالي
                                min_target = entry * (1 + (MIN_PROFIT_PERCENT / 100.0))
                                next_tp = max(float(next_tp), min_target, current_price * 1.001)
                                
                                signal['stop_loss'] = new_sl
                                signal['target_price'] = next_tp
                                signal['journey_state'] = journey_state
                                
                                logger.info(f"🎯 [{symbol}] تمديد الرحلة! الهدف التالي: {next_tp:.4f}, وقف الخسارة الجديد: {new_sl:.4f}")
                                
                                with signal_cache_lock: open_signals_cache[symbol] = signal
                                try:
                                    if check_db_connection():
                                        with conn.cursor() as cur:
                                            cur.execute("UPDATE signals SET journey_state = %s, target_price = %s, stop_loss = %s, quantity = %s WHERE id = %s",
                                                        (json.dumps(journey_state, cls=NpEncoder), float(signal['target_price']), float(signal['stop_loss']), float(signal.get('quantity', 0)), signal_id))
                                        conn.commit()
                                except Exception as e:
                                    logger.error(f"خطأ في قاعدة البيانات عند تحديث رحلة الصفقة لـ {symbol}: {e}"); conn.rollback()
                                continue
                                
                        logger.info(f"⏹️ [{symbol}] تحليل المسار لا يدعم التمديد أو فشل جلب البيانات. إغلاق الصفقة.")
                        journey_state['is_complete'] = True
                        close_signal(signal_id, current_price, 'journey_completed')
                    else:
                        close_signal(signal_id, current_price, 'take_profit')
                
                peak_price = float(signal.get('current_peak_price', entry))
                new_peak = max(peak_price, current_price)
                if new_peak > peak_price:
                    signal['current_peak_price'] = new_peak
                    if USE_ATR_TRAILING_STOP:
                        # [تحسين V9.8] استخدام كاش ATR (60 ثانية) بدلاً من جلب الشموع مع كل تحديث للسعر
                        latest_atr = get_cached_atr(symbol)
                        if latest_atr and latest_atr > 0:
                            new_trailing_stop_price = new_peak - (latest_atr * ATR_TS_MULTIPLIER)
                            if new_trailing_stop_price > sl:
                                signal['stop_loss'] = new_trailing_stop_price

                    with signal_cache_lock: open_signals_cache[symbol] = signal
                    try:
                        if check_db_connection():
                            with conn.cursor() as cur:
                                cur.execute("UPDATE signals SET current_peak_price = %s, stop_loss = %s WHERE id = %s", (float(new_peak), float(signal['stop_loss']), signal_id))
                            conn.commit()
                    except Exception as e:
                        logger.error(f"خطأ في قاعدة البيانات عند تحديث سعر الذروة/الوقف لـ {symbol}: {e}"); conn.rollback()
            time.sleep(2)
        except Exception as e:
            logger.error(f"❌ [مدير الصفقات] خطأ في حلقة الإدارة: {e}", exc_info=True)
            time.sleep(10)


def main_loop_enhanced():
    logger.info("[الحلقة الرئيسية] انتظار اكتمال التهيئة...")
    time.sleep(15)
    if not validated_symbols_to_scan:
        log_and_notify("critical", "قائمة العملات للمسح فارغة. يرجى التحقق من ملف 'crypto_list.txt'.", "SYSTEM_ERROR")
        return
    log_and_notify("info", f"✅ بدء حلقة المسح لـ {len(validated_symbols_to_scan)} عملة.", "SYSTEM")

    while True:
        try:
            logger.info("🔄 [الحلقة الرئيسية] بدء دورة مسح جديدة...")

            # [تحسين V9.9] انتظار انتهاء الحظر المؤقت من Binance قبل بدء الدورة
            ban_remain = rate_guard.banned_until - time.time()
            if ban_remain > 0:
                logger.warning(f"🚫 [الحلقة الرئيسية] حظر API ساري — الانتظار {int(ban_remain)} ثانية...")
                time.sleep(min(ban_remain, 60.0))
                continue

            # [تحسين V9.8] قاطع الحماية اليومي: إيقاف فتح صفقات جديدة عند تجاوز حد الخسارة
            if is_daily_loss_limit_hit():
                logger.warning("🛑 [الحلقة الرئيسية] قاطع الحماية مفعّل — انتظار 5 دقائق قبل الفحص التالي...")
                time.sleep(300)
                continue

            # [تحسين V9.10] تحديث قائمة العملات الديناميكية عند موعدها (كل 30 دقيقة افتراضيًا)
            # يختار كل مرة العملات الأكثر حيوية: سيولة + تقلب + انفجارات سعرية
            refresh_universe_if_needed()

            determine_market_state_enhanced()
            btc_data = get_btc_data_for_bot()
            symbols_to_process = random.sample(validated_symbols_to_scan, len(validated_symbols_to_scan))
            total_batches = (len(symbols_to_process) + SYMBOL_PROCESSING_BATCH_SIZE - 1) // SYMBOL_PROCESSING_BATCH_SIZE

            for i in range(0, len(symbols_to_process), SYMBOL_PROCESSING_BATCH_SIZE):
                batch = symbols_to_process[i:i + SYMBOL_PROCESSING_BATCH_SIZE]
                logger.info(f"🔄 جاري معالجة الدفعة {i // SYMBOL_PROCESSING_BATCH_SIZE + 1}/{total_batches}...")

                for symbol in batch:
                    try:
                        with signal_cache_lock:
                            if symbol in open_signals_cache or len(open_signals_cache) >= MAX_OPEN_TRADES:
                                continue
                        
                        df_15m = fetch_historical_data(symbol, SIGNAL_GENERATION_TIMEFRAME, SIGNAL_GENERATION_LOOKBACK_DAYS)
                        
                        if df_15m is None or len(df_15m) < 100:
                            continue
                        
                        df_with_indicators = calculate_all_features(df_15m, btc_data)
                        df_with_indicators.name = symbol
                        if df_with_indicators.empty:
                            continue
                        
                        # --- تطبيق الفلاتر العامة أولاً ---
                        if not check_market_volatility_filter(df_with_indicators):
                            continue
                        if not check_trend_strength_filter(df_with_indicators):
                            continue

                        signal_found, strategy_used = False, None

                        strategies_to_check = []
                        with macd_ema_strategy_lock:
                            if USE_MACD_EMA_STRATEGY: strategies_to_check.append(('MACD_EMA', check_macd_ema_strategy, "MACD_EMA_Crossover"))
                        with bb_stoch_strategy_lock:
                            if USE_BB_STOCH_STRATEGY: strategies_to_check.append(('BB_STOCH', check_bb_stoch_strategy_enhanced, "BB_Stoch_Reversal_Enhanced"))
                        with ema_rsi_strategy_lock:
                            if USE_EMA_RSI_STRATEGY: strategies_to_check.append(('EMA_RSI', check_ema_rsi_strategy, "EMA_RSI_Cross"))
                        with pullback_strategy_lock:
                            if USE_PULLBACK_STRATEGY: strategies_to_check.append(('PULLBACK', check_pullback_strategy, "Pullback_MACD"))
                        with bb_squeeze_strategy_lock:
                            if USE_BB_SQUEEZE_STRATEGY: strategies_to_check.append(('BB_SQUEEZE', check_bb_squeeze_strategy, "BB_Squeeze_Breakout"))
                        with bullish_momentum_strategy_lock:
                            if USE_BULLISH_MOMENTUM_STRATEGY: strategies_to_check.append(('BULLISH_MOMENTUM', check_bullish_momentum_strategy, "Bullish_Momentum"))
                        with sr_breakout_strategy_lock:
                            if USE_SR_BREAKOUT_STRATEGY: strategies_to_check.append(('SR_BREAKOUT', check_support_resistance_strategy_enhanced, "SR_Breakout_Enhanced"))

                        for key, check_func, name in strategies_to_check:
                            if check_func(df_with_indicators):
                                signal_found, strategy_used = True, name
                                break
                        
                        if not signal_found:
                            continue

                        logger.info(f"  -> [{symbol}] إشارة ناجحة من {strategy_used}. جاري التحقق النهائي...")
                        
                        try: entry_price = float(safe_get_symbol_ticker(symbol=symbol)['price'])
                        except Exception as e: logger.error(f"❌ [{symbol}] فشل جلب سعر الدخول: {e}."); continue

                        # --- [تحسين V9.8] فلاتر تأكيد مستوى الإشارة ---
                        if strategy_used == 'BB_Stoch_Reversal_Enhanced':
                            # استراتيجية ارتدادية: تجنب الشراء عند قمة آخر 24 ساعة
                            if not check_price_peak_filter(df_with_indicators, entry_price):
                                continue
                        elif USE_HTF_CONFIRMATION:
                            # استراتيجيات الاتجاه/الاختراق: تأكيد الترند الصاعد على فريم الساعة (مع كاش 15 دقيقة)
                            if not is_htf_bullish_confirmation_cached(symbol):
                                log_rejection(symbol, "HTF Trend Confirmation Failed")
                                continue

                        if USE_SHORT_TERM_MOMENTUM_FILTER and not passes_short_term_momentum_filter(symbol, df_with_indicators):
                            log_rejection(symbol, "Short-Term Momentum Filter Failed")
                            continue

                        if not passes_final_order_book_check(symbol, entry_price):
                            continue

                        logger.info(f"  -> [{symbol}] ✅ نجح فلتر دفتر الطلبات. جاري تحضير الصفقة...")
                        tp_sl_data = calculate_dynamic_tp_sl(df_with_indicators, entry_price)
                        if not tp_sl_data: continue

                        new_signal = {
                            'symbol': symbol, 'strategy_name': strategy_used,
                            'signal_details': {**tp_sl_data},
                            'entry_price': entry_price, **tp_sl_data
                        }

                        with trading_status_lock: is_enabled = is_trading_enabled
                        if is_enabled:
                            quantity = calculate_position_size(symbol, entry_price, new_signal['stop_loss'])
                            if quantity and quantity > 0:
                                order_result = place_order(symbol, Client.SIDE_BUY, quantity)
                                if order_result:
                                    new_signal.update({'is_real_trade': True, 'quantity': float(quantity), 'order_id': order_result['orderId']})
                                else: continue
                            else: continue
                        
                        saved_signal = insert_signal_into_db(new_signal)
                        if saved_signal:
                            with signal_cache_lock: open_signals_cache[saved_signal['symbol']] = saved_signal
                            log_and_notify('info', f"إشارة: إشارة شراء جديدة لـ {symbol} من استراتيجية {strategy_used}", "NEW_SIGNAL")

                    except Exception as e:
                        logger.error(f"❌ [خطأ معالجة] للرمز {symbol}: {e}", exc_info=True)
                    finally:
                        time.sleep(0.2)
                
                gc.collect()

            _, session_liquidity, _ = get_session_state()
            sleep_duration = 45 if session_liquidity == 'HIGH_LIQUIDITY' else 60
            logger.info(f"✅ [نهاية الدورة] انتهت دورة المسح الكاملة. الانتظار {sleep_duration} ثانية...")
            time.sleep(sleep_duration)

        except (KeyboardInterrupt, SystemExit):
            log_and_notify("info", "إيقاف البوت.", "SYSTEM"); break
        except Exception as main_err:
            log_and_notify("error", f"خطأ حرج في الحلقة الرئيسية: {main_err}", "SYSTEM"); time.sleep(120)

def price_update_loop():
    if not redis_client: return
    while True:
        try:
            # [تحسين V9.9] احترام فترة الحظر المؤقت بدل تكرار الطلبات المرفوضة
            ban_remain = rate_guard.banned_until - time.time()
            if ban_remain > 0:
                time.sleep(min(ban_remain, 10.0)); continue
            if validated_symbols_to_scan and client:
                tickers = safe_get_symbol_ticker()
                prices_to_set = {t['symbol']: t['price'] for t in tickers if t['symbol'] in validated_symbols_to_scan}
                if prices_to_set: redis_client.hset(REDIS_PRICES_HASH_NAME, mapping=prices_to_set)
            time.sleep(max(2, PRICE_UPDATE_INTERVAL_SEC))
        except Exception as e: logger.error(f"خطأ في حلقة تحديث الأسعار: {e}"); time.sleep(10)

def initialize_bot_services():
    global client, validated_symbols_to_scan
    logger.info("🤖 [خدمات البوت] بدء التهيئة...")
    try:
        init_db()
        init_redis()
    except Exception as e:
        log_and_notify("critical", f"حدث خطأ حرج أثناء التهيئة: {e}", "SYSTEM"); exit(1)

    # [تحسين V9.9] حلقة إعادة محاولة غير نهائية لاتصال Binance:
    # عند الحظر المؤقت (-1003) ننتظر حتى انتهاء مدته بدل exit(1) الذي يسبب
    # حلقة إعادة تشغيل سريعة من Render تضرب API أثناء الحظر وتطيل مدته
    attempt = 0
    while True:
        attempt += 1
        try:
            client = Client(API_KEY, API_SECRET)
            get_exchange_info_map()
            load_open_signals_to_cache()
            load_notifications_to_cache()
            validated_symbols_to_scan = get_validated_symbols()
            # [تحسين V9.10] حفظ القائمة الثابتة كبديل احتياطي ثم كشف فوري للعملات الأكثر حيوية
            _static_fallback_symbols[:] = validated_symbols_to_scan
            if USE_DYNAMIC_UNIVERSE:
                refresh_universe_if_needed(force=True)
            break
        except Exception as e:
            msg = str(e)
            is_ban = _is_rate_error(e)
            if is_ban:
                m = BAN_UNTIL_RE.search(msg)
                if m: rate_guard.register_ban(int(m.group(1)))
                else: rate_guard.register_ban(fallback_sec=90.0)
                rate_guard.last_error = msg[:160]
                wait_sec = max(5.0, min(rate_guard.banned_until - time.time(), 120.0))
            else:
                wait_sec = min(30.0 * attempt, 300.0)
            logger.critical(f"⚠️ [تهيئة] فشلت محاولة رقم {attempt} للاتصال بـ Binance: {msg[:200]}")
            logger.info(f"⏳ [تهيئة] إعادة المحاولة تلقائيًا بعد {int(wait_sec)} ثانية (اللوحة تبقى تعمل)...")
            if attempt == 1:
                send_telegram_message(f"⚠️ *تعذر الاتصال بـ Binance عند التهيئة*\n{msg[:200]}\nسيتم إعادة المحاولة تلقائيًا دون إيقاف الخدمة.")
            time.sleep(wait_sec)

    Thread(target=main_loop_enhanced, daemon=True).start()
    Thread(target=price_update_loop, daemon=True).start()
    Thread(target=trade_management_loop, daemon=True).start()
    Thread(target=balance_refresh_loop, daemon=True).start()  # [تحسين V9.11.0] كاش رصيد اللوحة
    Thread(target=btc_trend_loop, daemon=True).start()        # [تحسين V9.11] بوصلة اتجاه BTC
    logger.info("✅ [خدمات البوت] تم بدء جميع الخدمات الخلفية بنجاح.")
    send_telegram_message("✅ *البوت قيد التشغيل الآن (نسخة V9.11.0 - Neon Security)*")

# ---------------------- نقطة الدخول ----------------------
if __name__ == "__main__":
    # [تحسين V9.11.0] تخفيف تجوّع CPU على خطة Render المجانية (0.1 CPU):
    # حسابات pandas لـ 20 عملة تحتجز الـ GIL — تقصير مفتاح التبديل يمنح خيوط
    # الويب فرصة تنفيذ أسرع ويمنع تراكم طابور waitress أثناء دورات المسح
    try:
        sys.setswitchinterval(0.002)
    except Exception:
        pass
    logger.info("🚀 إطلاق بوت التداول ولوحة التحكم (V9.11.0 - Neon Security) 🚀")
    Thread(target=initialize_bot_services, daemon=True).start()
    port = int(os.environ.get('PORT', 10000))
    host = "0.0.0.0"
    logger.info(f"✅ بدء لوحة التحكم على {host}:{port}")
    try:
        from waitress import serve
        # [تحسين V9.11.0] رفع الخيوط 8→12: الاستطلاع من اللوحة (6 نقاط نهاية)
        # + فحوصات صحة Render تمر حتى أثناء دورات المسح الثقيلة
        serve(app, host=host, port=port, threads=12)
    except ImportError:
        app.run(host=host, port=port)
    logger.info("👋 [إيقاف] تم إيقاف تشغيل التطبيق.")


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
from threading import Thread, Lock, current_thread, enumerate as threading_enumerate
from datetime import datetime, timezone, timedelta
from decouple import config
from typing import List, Dict, Optional, Any, Set, Tuple
from sklearn.preprocessing import StandardScaler
from collections import deque, Counter, defaultdict
# [V9.16.0] مركز بيانات WebSocket — لا يخضع لأوزان REST ويعمل أثناء الحظر
try:
    import websocket  # websocket-client
    _WEBSOCKET_AVAILABLE: bool = True
except Exception:
    websocket = None
    _WEBSOCKET_AVAILABLE = False
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
APP_VERSION: str = 'V9.25.0'  # [V9.21.0] مصدر وحيد لرقم الإصدار — نهاية سلاسل النصوص المتفرقة
logger = logging.getLogger(f'CryptoBot{APP_VERSION}')

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

# --- [معايرة احترافية V9.15.0] لكل استراتيجية فلترها المنطقي بقيم معيارية ---
# مراجع المعايرة (الكلاسيكيات الموثقة التي يستخدمها المحترفون):
#   Wilder  (مبتكر ADX): ADX<20 نطاق/بلا اتجاه، 20-25 اتجاه ناشئ، >25 اتجاه قوي
#   Raschke (Holy Grail): الارتداد يُشترى في اتجاه مثبت فقط (ADX≥25)
#   Connors (الانعكاس): اشترِ التراجعات فوق المتوسط البنيوي فقط — لا تصيد سكاكين
#   Carter  (TTM Squeeze): الانضغاط+الاختراق+الحجم هو الفلتر ذاته — صالح في النطاق
#                          وفي استمرار الاتجاه القوي على السواء (العلم/الراية)
# والاتجاه العام محمي أصلًا ببوابة سلوك القائد (V9.13) وتأكيد فريم الساعة
# الأبعاد: min/max_adx حدود قوة الاتجاه، min/max_atr_pct حدود التقلب النسبي
# (أرضية 0.30% ≳ تكلفة الدوران 0.2% — معيار سيولة التنفيذ)، min_roc دفعة الزخم،
# require_above_ema50: معيار Connors البنيوي لصفقات شراء التراجعات. None = بلا حد.
STRATEGY_FILTER_PROFILES: Dict[str, Dict[str, Optional[float]]] = {
    # التقاطعات الاتجاهية — Wilder: بلا تداول تقاطعات في غياب الاتجاه (ADX≥20)
    'MACD_EMA_Crossover':         {'min_adx': 20.0, 'max_adx': None, 'min_atr_pct': 0.30, 'max_atr_pct': 5.0, 'min_roc': None},
    'EMA_RSI_Cross':              {'min_adx': 20.0, 'max_adx': None, 'min_atr_pct': 0.30, 'max_atr_pct': 5.0, 'min_roc': None},
    # Pullback — Raschke: الارتداد يُشترى في اتجاه مثبت فقط (ADX≥25)
    'Pullback_MACD':              {'min_adx': 25.0, 'max_adx': None, 'min_atr_pct': 0.30, 'max_atr_pct': 5.0, 'min_roc': None},
    # الزخم — [V9.17.0] ROC≥0.6%: مطابقة الأزواج تختار قادة الزخم أصلًا،
    # فتكفي الأرضية بإيقاع أقل صرامة من 1% التي رفضت 155 محاولة حية
    'Bullish_Momentum':           {'min_adx': 25.0, 'max_adx': None, 'min_atr_pct': 0.50, 'max_atr_pct': 6.0, 'min_roc': 0.6},
    # الارتدادية — Connors المرن [V9.17.0]: شراء التراجعات قرب EMA50 أو فوقها (ضمن سماحية
    # ATR واحدة). الصرامة المطلقة القديمة (close>EMA50) رفضت 431 محاولة (67%) بلا نجاح واحد،
    # مع أن شراء الغرقى المقصود في الاستراتيجية يقع شرعًا تحت المتوسط بهامش ضئيل
    'BB_Stoch_Reversal_Enhanced': {'min_adx': None, 'max_adx': None, 'min_atr_pct': 0.25, 'max_atr_pct': 4.0, 'min_roc': None, 'ema50_tol_atr': 1.0},
    # الاختراقية — Carter: الانضغاط هو الفلتر ذاته، بلا قيود نظامية ADX إطلاقًا
    # (سقف ADX كان يمنع نمط الاستمرار الاحترافي في الاتجاهات القوية — 72 رفضًا حيًا)
    'BB_Squeeze_Breakout':        {'min_adx': None, 'max_adx': None, 'min_atr_pct': None, 'max_atr_pct': 5.0, 'min_roc': None},
    'SR_Breakout_Enhanced':       {'min_adx': None, 'max_adx': None, 'min_atr_pct': None, 'max_atr_pct': 5.0, 'min_roc': None},
}
# احتياط لأي استراتيجية مستقبلية غير مدرجة: بوابة عقلانية واسعة فقط
DEFAULT_STRATEGY_FILTER_PROFILE: Dict[str, Optional[float]] = {
    'min_adx': None, 'max_adx': None, 'min_atr_pct': 0.20, 'max_atr_pct': 7.0, 'min_roc': None}

# --- [V9.17.0] محرك مطابقة الأزواج بالاستراتيجيات (Strategy-Pair Matching Engine) ---
# المشكلة الحية من اللوحة: 4438 فحص → 16 نجاح → 0 صفقة، و98% من الرفضات سببها فحص
# كل استراتيجية على كل عملات القائمة (المختارة بالسيولة/التذبذب) بلا أي مطابقة نمط:
# ارتدادية على عملة اتجاهية، تقاطعات على عملة نطاق، انضغاط على عملة منفلت.
# الحل: تصنيف نمط كل زوج من شموع 15م المجلوبة أصلًا داخل حلقة المسح (صفر وزن شبكي
# إضافي) ثم تُفحص كل استراتيجية فقط على الأزواج التي يطابق نمطها السوقي شخصيتها —
# ترشيح مخصص لكل استراتيجية على حدة بدل العشوائي والسيولة.
PAIR_MATCHING_ENABLED: bool = os.environ.get('PAIR_MATCHING_ENABLED', 'true').strip().lower() in ('1', 'true', 'yes', 'on')
PAIR_MATCH_MIN_SCORE: float = float(os.environ.get('PAIR_MATCH_MIN_SCORE', '45'))
PAIR_POOL_DISPLAY_SIZE: int = int(os.environ.get('PAIR_POOL_DISPLAY_SIZE', '8'))

# --- [V9.18.0] وضع التوصيات: اجتياز الفلاتر = توصية شراء مفتوحة تُدار كأي صفقة ---
# طلب المستخدم الصريح: "العملات التي تجتاز الفلاتر تظهر في اللوحة كتوصيات شراء
# ويقوم بتتبعها وتحديث أهدافها ووقف خسارتها كأنها صفقات مفتوحة".
# التشخيص الحي (لوحة V9.17.0): 248 (زوج×استراتيجية) يجتازون سلسلة الفلاتر كاملة
# (مطابقة النمط + الفلاتر الخاصة) لكن مُطلِق الشمعة الواحدة (التقاطع اللحظي)
# لا يكتمل أبدًا في لحظة الفحص → 0 صفقة. الحل: أفضل اجتياز فلاتر في الدورة
# يُفتح فورًا كتوصية شراء ورقية تدخل مسار الإدارة الكامل (تتبع/أهداف/وقف متحرك/إغلاق).
RECOMMENDATIONS_ENABLED: bool = config('RECOMMENDATIONS_ENABLED', default=True, cast=bool)
# أقصى توصيات فلاتر جديدة في الدورة الواحدة (بجانب إشارات المُطلِقات العادية)
RECOMMENDATIONS_PER_CYCLE: int = config('RECOMMENDATIONS_PER_CYCLE', default=2, cast=int)
# أدنى درجة مطابقة زوج (0-100) تؤهل الاجتياز للتحول إلى توصية — جودة قبل الكمية
RECOMMENDATION_MIN_FIT_SCORE: float = config('RECOMMENDATION_MIN_FIT_SCORE', default=60.0, cast=float)
# تهدئة بعد إغلاق أي صفقة لرمز معين: لا توصية جديدة لنفس الرمز خلالها (منع التأرجح)
# [V9.23.0] 480د (8 ساعات) مثبتة بالباك تيست: خفض التأرجح الربحي القصير ≤4 ساعات (خاسر -0.27%/صفقة)
# وترفع الصافي مع بوابة الأدلة: -1.44U → +1.39U على 30 يومًا × 24 رمزًا (PF 0.93 → 1.16)
RECOMMENDATION_COOLDOWN_MIN: int = config('RECOMMENDATION_COOLDOWN_MIN', default=480, cast=int)

# ---------------------- [V9.23.0] محرك الأدلة — الترشيح بالبرهان لا بالتكهن ----------------------
# يعيد تشغيل منطق المنتج نفسه (مطابقة + فلاتر + مُطلِقات + خروج V9.22.0) على شموع 15م
# الأخيرة، ويقيس توقع كل خلية (استراتيجية × نمط سوقي) صافي الرسوم والانزلاق.
# لا توصية/إشارة إلا إذا أثبتت الخلية توقعًا موجبًا حقيقيًا — مغلق أمام بلا دليل (fail-closed).
EVIDENCE_ENABLED: bool = os.environ.get('EVIDENCE_ENABLED', 'true').strip().lower() in ('1', 'true', 'yes', 'on')
EVIDENCE_WINDOW_BARS: int = int(os.environ.get('EVIDENCE_WINDOW_BARS', '1440'))     # 15 يومًا شموع 15م — نافذة أوسع تمنح الخلايا النادرة (الارتداد/الانضغاط) عينات كافية
EVIDENCE_REFRESH_MIN: int = int(os.environ.get('EVIDENCE_REFRESH_MIN', '240'))      # تحديث كل 4 ساعات
EVIDENCE_STRIDE: int = int(os.environ.get('EVIDENCE_STRIDE', '4'))                  # تقييم كل ساعة (خفة)
EVIDENCE_ENTRY_SPACING: int = int(os.environ.get('EVIDENCE_ENTRY_SPACING', '4'))    # تباعد صفقات الدليل
EVIDENCE_MAX_SYMBOLS: int = int(os.environ.get('EVIDENCE_MAX_SYMBOLS', '24'))       # سقف رموز التحديث
# عتبات البوابة المثبتة بالباك تيست (V12): n≥5، توقع ≥ +0.12%/صفقة، PF ≥ 1.15
EVIDENCE_MIN_TRADES: int = int(os.environ.get('EVIDENCE_MIN_TRADES', '5'))
EVIDENCE_MIN_EXP_PCT: float = float(os.environ.get('EVIDENCE_MIN_EXP_PCT', '0.12'))
EVIDENCE_MIN_PF: float = float(os.environ.get('EVIDENCE_MIN_PF', '1.15'))
# [V9.24.0] عتبات العينة الصغيرة (3-4 صفقات): صرامة أعلى تعوّض قلة الدليل —
# بدونها تبقى الخلايا الجديدة (قاع-صيد الارتداد مثلًا) بلا فرصة إثبات حي أبديًا
EVIDENCE_MIN_EXP_PCT_SMALL: float = float(os.environ.get('EVIDENCE_MIN_EXP_PCT_SMALL', '0.30'))
EVIDENCE_MIN_PF_SMALL: float = float(os.environ.get('EVIDENCE_MIN_PF_SMALL', '1.40'))
SMART_PICKS_TOP: int = int(os.environ.get('SMART_PICKS_TOP', '8'))                  # عرض اللوحة
EVIDENCE_FEE_PCT: float = float(os.environ.get('EVIDENCE_FEE_PCT', '0.10'))         # رسوم/جانب
EVIDENCE_SLIP_PCT: float = float(os.environ.get('EVIDENCE_SLIP_PCT', '0.03'))       # انزلاق/جانب

REGIME_AR: Dict[str, str] = {
    'trend_up': 'اتجاه صاعد', 'trend_down': 'اتجاه هابط', 'range': 'نطاق مترنم',
    'squeeze': 'انضغاط سعري', 'transitional': 'انتقالي'}

# ملف المطابقة لكل استراتيجية: أي ريماً تريد + نطاقات ADX/ATR% + بنونيهات البنية.
# adx = (حد_أدنى, مثالي, سقف): سلم صعود 0→25 للتوجهية، أو سقف يتحلل فوقه ×2/وحدة للنطاقية
STRATEGY_PAIR_PROFILES: Dict[str, Dict[str, Any]] = {
    # التقاطعات الاتجاهية: اتجاه صاعد قائم بهيكل EMA سليم
    'MACD_EMA_Crossover':         {'regimes': ('trend_up',),        'adx': (18.0, 28.0, None), 'atr_pct': (0.30, 5.0), 'struct_up_bonus': True, 'pos_roc_bonus': True},
    'EMA_RSI_Cross':              {'regimes': ('trend_up',),        'adx': (18.0, 28.0, None), 'atr_pct': (0.30, 5.0), 'struct_up_bonus': True, 'pos_roc_bonus': True},
    # Pullback — Raschke: اتجاه مثبت بأعمق من التقاطعات
    'Pullback_MACD':              {'regimes': ('trend_up',),        'adx': (22.0, 32.0, None), 'atr_pct': (0.30, 5.0), 'struct_up_bonus': True, 'pos_roc_bonus': True},
    # الزخم: قادة الحركة الصاعدة — زخم سالب نقض قاسٍ (لا سكين ساقطة)
    'Bullish_Momentum':           {'regimes': ('trend_up',),        'adx': (20.0, 30.0, None), 'atr_pct': (0.40, 6.0), 'struct_up_bonus': True, 'pos_roc_bonus': True, 'require_pos_roc': True},
    # الارتدادية — Connors [V9.24.0]: النطاق جوهرها، والتراجعات في الصاعد ائتمان ثانوي،
    # والهابط مقبول شرط كاشف القاع+الارتداد (detect_bottom_bounce_setup) يصادق بشروطه
    # المشددة (تشبع أعمق RSI<30 + قاع ≤0.8×ATR + شمعة ارتداد) — المنع المطلق كان يُعطّل
    # استراتيجية القاع في السوق الوحيد الذي تكون فيه هي الفكرة الصحيحة (820 فحص/0 إشارة)
    'BB_Stoch_Reversal_Enhanced': {'regimes': ('range', 'trend_down'), 'secondary_regimes': ('trend_up', 'transitional'), 'adx': (None, None, None), 'atr_pct': (0.25, 4.0), 'range_bonus': True, 'near_ema50': True},
    # الاختراقية — Carter [V9.24.0+BT]: الانضغاط جوهرها، والاتجاه الصاعد ائتمان ثانوي.
    # (range أُزيلت من الثانوية بالباك تيست: كل صفقات الاختراق في رموز مترنمة (flips≥5)
    # كانت انفجارات زائفة — 4/4 خاسرة، الاختراق يحتاج انضغاطًا حقيقيًا لا رملًا)
    'BB_Squeeze_Breakout':        {'regimes': ('squeeze',), 'secondary_regimes': ('trend_up',), 'adx': (None, None, 35.0), 'atr_pct': (None, 5.0), 'low_bbwp_bonus': True},
    'SR_Breakout_Enhanced':       {'regimes': ('range', 'squeeze'), 'secondary_regimes': ('trend_up',), 'adx': (None, None, 34.0), 'atr_pct': (None, 5.0), 'low_bbwp_bonus': True},
}

# --- [V9.24.0] خريطة السوق العام → الاستراتيجيات المسموحة (حتمية لا تكهن) ---
# طلب المستخدم الصريح: "كل استراتيجية تفحص الرموز التي يمكن تحقق الاستراتيجية بها على حسب الاستراتيجية".
# في سوق هابط حاد لا يُفتح الزخم/الاختراق الصاعد ضد الاتجاه (كان مصدر كل خسائر V9.21→V9.23:
# 10 توصيات BB_Squeeze في STRONG_DOWNTREND كلها stop_loss) — بل قاع-صيد الارتداد واختراق الدعوم.
# المفاتيح نفسها المستخدمة في حلقة المسح. القيمة None = بلا تقييد (سقوط آمن عند حالة غير معروفة).
MARKET_STATE_STRATEGY_ALLOW: Dict[str, Optional[Tuple[str, ...]]] = {
    'STRONG_DOWNTREND': ('BB_STOCH', 'SR_BREAKOUT'),
    'DOWNTREND':        ('BB_STOCH', 'SR_BREAKOUT', 'BB_SQUEEZE'),
    'RANGING':          ('BB_STOCH', 'SR_BREAKOUT', 'BB_SQUEEZE', 'MACD_EMA', 'EMA_RSI', 'PULLBACK'),
    'UNCERTAIN':        ('BB_STOCH', 'SR_BREAKOUT', 'BB_SQUEEZE', 'MACD_EMA', 'EMA_RSI', 'PULLBACK', 'BULLISH_MOMENTUM'),
    'UPTREND':          ('BB_STOCH', 'SR_BREAKOUT', 'BB_SQUEEZE', 'MACD_EMA', 'EMA_RSI', 'PULLBACK', 'BULLISH_MOMENTUM'),
    'STRONG_UPTREND':   ('BB_STOCH', 'SR_BREAKOUT', 'BB_SQUEEZE', 'MACD_EMA', 'EMA_RSI', 'PULLBACK', 'BULLISH_MOMENTUM'),
}
# ثوابت كاشف القاع والارتداد الحتمي [V9.24.0] — طلب المستخدم: "عملات في قاع سعرها
# أعطت مؤشرات على ارتدادها تفحص هذه الرموز باستراتيجية الارتداد".
# في الهبوط الحاد تشتد الصرامة (تشبع أعمق + قاع أقرب) لأن السكين الساقطة تكرر القيعان.
BOTTOM_WINDOW_BARS: int = 96                  # قاع 24 ساعة على فريم 15م
BOTTOM_DIST_LOW_ATR: float = 1.2              # القاع: السعر ضمن 1.2×ATR من أدنى قاع النافذة
BOTTOM_DIST_LOW_ATR_DOWNTREND: float = 0.8    # وفي الهبوط: أقرب للقاع (≤0.8×ATR)
BOTTOM_RSI_MAX_RANGE: float = 35.0            # تشبع بيعي في السوق العادي
BOTTOM_RSI_MAX_DOWNTREND: float = 30.0        # تشبع أعمق إلزامي في الهبوط الحاد
BOTTOM_STOCH_MAX: float = 40.0                # ستوكاستك RSI في المنطقة السفلية بدوران صاعد
BOTTOM_WICK_MIN_RATIO: float = 0.45           # ذيل سفلي ≥45% من مدى الشمعة = دليل رفض بيع
SQUEEZE_BBWP_MAX: float = 0.30                # انضغاط فعلي: عرض بولنجر في أدنى 30% من تاريخه الحديث
# [V9.24.0] فلتر ثبات الريم العام للاستراتيجيات الاستمرارية (مستوحى من الباك تيست):
# في السوق المنشاري يتنقل الريم بين UPTREND و DOWNTREND بسرعة — شراء الزخم/الاختراق
# في ارتداد دببة "قوي صاعد" كان يخسر -0.33%/صفقة. القاعدة: لا استراتيجيات استمرارية
# إلا بعد ثبات عائلة الريم الصاعد MARKET_REGIME_PERSIST_MIN دقيقة متصلة (الحتمية لا التكهن).
MARKET_REGIME_PERSIST_MIN: int = int(os.environ.get('MARKET_REGIME_PERSIST_MIN', '480'))  # 8 ساعات
TREND_CONTINUATION_KEYS: Tuple[str, ...] = ('MACD_EMA', 'EMA_RSI', 'PULLBACK', 'BULLISH_MOMENTUM', 'BB_SQUEEZE')
# سجل تاريخ الريم العام للحية (تُملأ في determine_market_state_enhanced) لحساب الثبات
MARKET_REGIME_HISTORY: deque = deque(maxlen=400)   # ~20 ساعة بعينات كل 3 دقائق

# ترشيحات آخر دورة مسح (للعرض في اللوحة عبر /api/strategy_pairs) + طابع زمني
strategy_pair_pools: Dict[str, List[Dict[str, Any]]] = {}
pair_pools_updated_at: Optional[str] = None
pair_pools_lock = Lock()

# --- [تحسين V9.9.1] إعدادات حماية الحظر من Binance (خطأ -1003) ---
# حد Binance الرسمي 6000 وزن/دقيقة لكل IP — وعلى Render المجاني الـ IP مشترك مع خدمات أخرى،
# لذا الميزانية الافتراضية متحفظة (1500) وتنخفض تلقائيًا 40% عند كل حظر ثم تتعافى تدريجيًا
RATE_LIMIT_BUDGET_PER_MIN: int = config('RATE_LIMIT_BUDGET_PER_MIN', default=500, cast=int)
# [V9.22.0] 1500→500: المراقبة أثبتت أن استهلاكنا <2% من الميزانية، والتعافي حتى 1410
# على IP مسموم من الجيران بلا فائدة — سقف متحفظ يمنع أي انفجار وزن من جانبنا
# الفاصل الأدنى بالثواني بين أي طلبين REST متتاليين
API_MIN_SPACING_SEC: float = config('API_MIN_SPACING_SEC', default=0.15, cast=float)
# تبريد إضافي (ثوانٍ) بعد انتهاء الحظر قبل استئناف الطلبات — يمنع انفجار الخيوط
# فور انتهاء الحظر (Thundering Herd) الذي كان يسبب التصعيد 30 ثانية → 16 دقيقة
BAN_RESUME_COOLDOWN_SEC: int = config('BAN_RESUME_COOLDOWN_SEC', default=90, cast=int)
# بعد الاستئناف: الفاصل الأدنى يتضاعف ×4 لمدة (ثوانٍ) ثم يعود تدريجيًا للطبيعي
BAN_RESUME_RAMP_SEC: int = config('BAN_RESUME_RAMP_SEC', default=120, cast=int)
# [V9.15.1] عتبة "الحظر الطويل" بالثواني: حظر أطول منها = تصعيد (إما تراكم وزن من
# جلسة سابقة أو — الأشهر — جيران Render المجاني على نفس IP الخروج المشترك).
# الاستجابة: الميزانية تهبط للأرضية مباشرة + تبريد استئناف ممتد + إشعار تليجرام
# بموعد النهاية الفعلي بدل الضرب المتكرر أثناء الحظر
BAN_LONG_THRESHOLD_SEC: int = config('BAN_LONG_THRESHOLD_SEC', default=600, cast=int)
# [V9.21.0] استقلال طائرة البيانات عن حظر باينانس: المسح والبوصلة وخريطة القادة تُغذّى
# من مركز WebSocket والمزودين البديلين (محصنة ضد الحظر) فتواصل العمل أثناء الحظر،
# ويُؤجَّل التنفيذ الحقيقي فقط (الأوامر/الرصيد). False = السلوك القديم (تجميد كامل).
SCAN_DURING_BAN: bool = config('SCAN_DURING_BAN', default=True, cast=bool)
# فاصل تحديث أسعار Redis بالثواني — [V9.19.0] 5→2ث: النشر من مركز WebSocket داخل
# العملية (صفر وزن شبكي) فالزمن الحقيقي أصبح مجانيًا — اللوحة والإدارة يقرآن أسعارًا بعمر ≤2ث
PRICE_UPDATE_INTERVAL_SEC: int = config('PRICE_UPDATE_INTERVAL_SEC', default=2, cast=int)

# --- [V9.16.0] مركز بيانات WebSocket — الشفاء الجذري لحظر -1003 على IP مشترك ---
# الشموع والأسعار عبر تدفقات Binance العامة (بلا مفاتيح وبلا أوزان REST وتعمل أثناء الحظر)،
# ويبقى REST للتهيئة/الأوامر/تعبئة أولى لكل رمز — بصمة وزن شبه معدومة على IP الخروج المشترك.
USE_STREAM_HUB: bool = config('USE_STREAM_HUB', default=True, cast=bool)
STREAM_HUB_BASE_URL: str = config('STREAM_HUB_BASE_URL', default='wss://stream.binance.com:443/stream')
# عمق المخازن (شمعة مكتملة): 15م يغطي lookback المسح (920 ساعة = 3680 شمعة) + هامش
STREAM_HUB_BUFFER_15M: int = config('STREAM_HUB_BUFFER_15M', default=4200, cast=int)
STREAM_HUB_BUFFER_1H: int = config('STREAM_HUB_BUFFER_1H', default=1300, cast=int)
STREAM_HUB_BUFFER_DEFAULT: int = config('STREAM_HUB_BUFFER_DEFAULT', default=500, cast=int)
# فاصل التعبئة العميقة (رمز واحد كل مرة — يحترم الحارس) وعتبة الطلب الجامد
STREAM_HUB_BACKFILL_DELAY_SEC: float = config('STREAM_HUB_BACKFILL_DELAY_SEC', default=3.0, cast=float)
STREAM_HUB_STALE_SEC: int = config('STREAM_HUB_STALE_SEC', default=90, cast=int)
STREAM_HUB_MAINTAIN_SEC: int = config('STREAM_HUB_MAINTAIN_SEC', default=60, cast=int)

# --- [تحسين V9.11.0] منع اختناق خيوط الويب (waitress queue depth) ---
# السبب الجذري: /api/market_status كان يستدعي Binance مباشرة (وزن 5) في كل استطلاع
# من المتصفح كل 5 ثوانٍ — وأثناء انشغال الحارس أو الحظر يبقى خيط waitress محجوزًا
# في acquire() لثوانٍ إلى دقائق، فتتراكم الطابور (Task queue depth 1..15+).
# الحل: خيط خلفي يحدّث رصيد USDT في كاش، واللوحة تقرأ الكاش فورًا بلا أي نداء شبكي.
DASHBOARD_BALANCE_REFRESH_SEC: int = config('DASHBOARD_BALANCE_REFRESH_SEC', default=45, cast=int)

# --- [إصلاح V9.12.0] مهلة عميل Binance — قاتل التعليق الدائم ---
# python-binance افتراضيًا يرسل طلبات HTTP بلا timeout إطلاقًا: أي اتصال نصف مفتوح
# (شائع على Render) يجعل النداء يتدلى للأبد ويحبس الخيوط والأقفال خلفه.
# مهلة 25 ثانية تقطع النداء المعلق وتسمح لمنطق إعادة المحاولة في safe_api_call بالعمل.
BINANCE_CLIENT_TIMEOUT_SEC: int = config('BINANCE_CLIENT_TIMEOUT_SEC', default=25, cast=int)

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

# --- [V9.25.0] دائرة الترشيح الموسعة: 10 عملات للفحص × كل استراتيجية (طلب المستخدم الصريح) ---
# "قم بتوسيع دائرة العملات التي تفحص بحيث ترشح 10 عملات للفحص مناسبة لكل استراتيجية
#  اي العدد الكلي 10×عدد الاستراتيجيات"
# الكون الواسع: يُبنى من نفس طلب التكه المجاني الواحد (صفر وزن إضافي) — دائرة الرصد
# تتوسع من حجم القائمة العميقة (~20) إلى WIDE_UNIVERSE_SIZE ليتوفر لكل استراتيجية
# مخزون كافٍ يرشح منه عملاتها العشر الأقرب لظروفها.
WIDE_UNIVERSE_SIZE: int = config('WIDE_UNIVERSE_SIZE', default=120, cast=int)
# عدد العملات المرشحة للفحص لكل استراتيجية في الدورة — العدد الكلي = 10 × عدد الاستراتيجيات
NOMINEES_PER_STRATEGY: int = config('NOMINEES_PER_STRATEGY', default=10, cast=int)
# [V9.25.0] أرضية سيولة الدائرة الواسعة — أخف من أرضية القائمة العميقة حتى لا يفرغ
# التوسع في سوق خامل (حياً: 10M أهلّت 20 عملة فقط على Bybit — 3M تفتح الدائرة دون قمامة)
WIDE_UNIVERSE_MIN_QUOTE_VOLUME: float = config('WIDE_UNIVERSE_MIN_QUOTE_VOLUME', default=3000000.0, cast=float)

# --- [تحسين V9.13.0] خريطة القيادة: أي قائد سيادي تتبعه كل عملة (BTC/ETH/SOL)؟ ---
# الأصل: العملات الأصغر تتحرك تحت مظلة قادة السوق. نحسب ارتباط عوائد كل عملة مع كل
# قائد من شموع 15م (المجلوبة أصلًا للمسح مقابل كاش القادة — صفر نداء إضافي للمسح)
# ثم نأخذ القرار حسب سلوك القائد: لا شراء تابع إذا كان قائده هابطًا.
USE_LEADER_FILTER: bool = config('USE_LEADER_FILTER', default=True, cast=bool)
# القادة السياديون الثلاثة (طلب المستخدم: البتكوين والإثريوم والصول)
LEADER_SYMBOLS: List[str] = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']
# نافذة الارتباط: 192 شمعة 15م = آخر 48 ساعة (توازن بين الاستجابة والضجيج)
LEADER_CORR_WINDOW: int = config('LEADER_CORR_WINDOW', default=192, cast=int)
# أدنى ارتباط يُعتد به لتصنيف العملة "تابعة" لقائد — تحته تُصنف "مستقلة"
LEADER_CORR_MIN: float = config('LEADER_CORR_MIN', default=0.30, cast=float)

# --- [V9.20.0] طبقة البيانات متعددة المصادر: وداع ابتلاع وزن باينانس في جلب البيانات ---
# بيانات السوق (شموع/تكه 24س/عمق السوق) تُجلب من منصات أسواقها مطابقة تقريبًا لباينانس
# بحدود طلبات سخية: Bybit → OKX → Gate.io، وباينانس احتياط أخير عبر المسارات القديمة.
# التنفيذ (أوامر/أرصدة/حالة الأوامر) يبقى على باينانس حصرًا دون أي تغيير.
# 'multi' = تناوب المزودين مع تبريد تلقائي للمعطوب، 'binance_only' = السلوك القديم كاملًا.
DATA_FEED_MODE: str = config('DATA_FEED_MODE', default='multi', cast=str)
DATA_FEED_PROVIDERS: str = config('DATA_FEED_PROVIDERS', default='bybit,okx,gate', cast=str)
DATA_FEED_TIMEOUT_SEC: int = config('DATA_FEED_TIMEOUT_SEC', default=8, cast=int)
# عدد الإخفاقات المتتالية قبل تبريد المزود مؤقتًا (يفسح المجال لغيره)
DATA_FEED_FAIL_THRESHOLD: int = config('DATA_FEED_FAIL_THRESHOLD', default=4, cast=int)
DATA_FEED_COOLDOWN_SEC: int = config('DATA_FEED_COOLDOWN_SEC', default=300, cast=int)
# عتبة رفض الشراء: درجة سلوك القائد تحتها (هابط/هابط قوي) تُرفض إشارات التابع
LEADER_BEARISH_SCORE: float = config('LEADER_BEARISH_SCORE', default=-18.0, cast=float)
# فترة تحديث بيانات القادة (شموع 15م لكل قائد — وزن 2 للقائد)
LEADER_REFRESH_SEC: int = config('LEADER_REFRESH_SEC', default=300, cast=int)


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
# [V9.22.0] باك تيست 60 يومًا × 24 رمزًا (نفس منطق المنتج حرفيًا):
# الأساس (2.2 من أول قمة): صافي -85%، PF 0.94 — التفعيل بعد +1.5% + مضاعف 2.8: صافي +181%، PF 1.18
# موجب في الشرائح الزمنية الثلاث ويصمد مع رسوم 0.3%. التفاصيل: scripts/bt_run.py
ATR_TS_MULTIPLIER: float = 2.8
# [V9.22.0] حد ربح أدنى لتفعيل الوقف المتحرك: لا يُرفع الوقف قبل أن تبلوغ القمة
# هذا الحد فوق سعر الدخول (0 = السلوك القديم). كان الرفع من أول قمة غبارية
# يُغلق الرابحين مبكرًا (+0.03→+0.74) ويستنزف بالدوران والرسوم
ATR_TRAIL_ACTIVATE_PROFIT_PCT: float = config('ATR_TRAIL_ACTIVATE_PROFIT_PCT', default=1.5, cast=float)

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
# [V9.25.0] الكون الواسع (دائرة الرصد الموسعة) + مرشحو كل استراتيجية للفحص
# wide_universe_rows: مقاييس التكه المجانية لكل رمز في الدائرة الواسعة (120) —
# مصدر الترشيح الحتمي لكل استراتيجية. STRATEGY_NOMINEES: أفضل 10 لكل استراتيجية.
wide_universe_rows: Dict[str, Dict[str, float]] = {}
nominees_lock = Lock()
STRATEGY_NOMINEES: Dict[str, List[Dict[str, Any]]] = {}
# [V9.25.0] مخزون الترتيب الكامل لكل استراتيجية (قبل قصّ العشرة) — يسمح باستبعاد
# الصفقات المفتوحة مع التزويد التلقائي من بقية الترتيب لتبقى القائمة عشرة كلما أمكن
_NOMINEE_RANKED_POOL: Dict[str, List[Dict[str, Any]]] = {}
strategy_nominees_built_at: float = 0.0
strategy_nominees_updated_at: Optional[str] = None
_static_fallback_symbols: List[str] = []
open_signals_cache: Dict[str, Dict] = {}
signal_cache_lock = Lock()
# [إصلاح V9.12.0] حارس منع الإغلاق المزدوج: بعد نقل العمل الشبكي خارج القفل،
# قد يستدعي خيطان نفس الصفقة (حلقة الإدارة + إغلاق يدوي) فيبيع مرتين —
# هذا المجموعة يضمن عملية إغلاق واحدة فقط لكل signal_id
_closing_signal_ids: set = set()
notifications_cache = deque(maxlen=50)
notifications_lock = Lock()
rejection_logs_cache = deque(maxlen=100)
rejection_logs_lock = Lock()
# [تحسين V9.12.0] عدادات تحليل أسباب الرفض التراكمية (منذ الإقلاع):
# 1) كاش الرفض محدود بـ 100 عنصر يفيض خلال ثوانٍ في الدورة النشطة — العدادات تحفظ الصورة الكاملة
# 2) خمس استراتيجيات من سبع كانت تفشل بصمت تام بلا تسجيل — الآن تُحصى كل الفحوصات والنجاحات
_filter_reject_stats: Counter = Counter()          # [V9.14.0] بوابة العقلانية العامة فقط
_strategy_scan_stats: Dict[str, Counter] = defaultdict(lambda: Counter({'checks': 0, 'passes': 0}))
# [V9.14.0] رفضات الفلتر الخاص بكل استراتيجية: {الاستراتيجية: {اسم الفلتر: عدد}}
_strategy_filter_stats: Dict[str, Counter] = defaultdict(Counter)
_scan_stats_lock = Lock()
# [V9.18.0] إحصاء التوصيات المفتوحة من اجتياز الفلاتر (لللوحة):
# opened = فُتحت فعليًا | gate_rejected = رفضتها بوابات التأكيد النهائية
# cooldown_skipped = الرمز ضمن تهدئة ما بعد الإغلاق | below_min_score = درجة المطابقة دون الحد
_recommendation_stats: Counter = Counter({'opened': 0, 'gate_rejected': 0, 'cooldown_skipped': 0, 'below_min_score': 0, 'evidence_rejected': 0})
# [V9.23.0] علم إعادة تشغيل الدليل — يُصمت به سجل الرفضات والمحاسبة أثناء الباك تيست الداخلي
_evidence_replaying: bool = False
_recent_close_ts: Dict[str, float] = {}   # رمز -> طابع زمني إغلاقه الأخير (تهدئة سريعة الذاكرة)
_last_cache_reconcile: float = 0.0        # [V9.19.1] آخر مصالحة كاش الصفقات الفارغ
current_market_state: Dict[str, Any] = {"overall_regime": "INITIALIZING", "trend_details_by_tf": {}, "last_updated": None}
market_state_lock = Lock()
last_market_state_check = 0
technical_signals_cache: Dict[str, Dict] = {}
TECHNICAL_SIGNAL_CACHE_DURATION: int = 60 * 5
technical_signals_lock = Lock()

# --- [تحسين V9.13.0] حالة خريطة القيادة (بيانات القادة + تصنيف التابعين) ---
_leader_data: Dict[str, Dict[str, Any]] = {}   # قائد -> {'closes': [...], 'trend': {...}, 'ts': float}
_leader_data_lock = Lock()
_leader_map: Dict[str, Dict[str, Any]] = {}    # عملة -> {'leader', 'corr', 'correlations', 'is_leader', 'updated'}
_leader_map_lock = Lock()
_leader_veto_stats: Counter = Counter()        # عداد رفضات بوابة القائد لكل قائد (منذ الإقلاع)

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
    def __init__(self, budget_per_min: Optional[int] = None, min_spacing: float = 0.15):
        # [V9.22.0] الافتراضي من الإعداد لا قيمة قديمة مضمّنة (كانت 1500)
        self.configured_budget = max(400, int(budget_per_min if budget_per_min is not None else RATE_LIMIT_BUDGET_PER_MIN))
        self.budget = self.configured_budget
        self.budget_floor = max(300, int(self.configured_budget * 0.25))
        self.min_spacing = max(0.0, float(min_spacing))
        self.resume_cooldown = max(0, int(BAN_RESUME_COOLDOWN_SEC))
        self._active_cooldown = self.resume_cooldown  # [V9.15.1] التبريد الفعلي المطبق للحظر الحالي
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
            wait = self.banned_until - now + float(self._active_cooldown)
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
        """[V9.15.1] تسجيل الحظر مع إلغاء الازدواج (Dedup):
        قبل الإصلاح: التهيئة + safe_api_call (حتى 4 محاولات) + الحلقات الخلفية كانت
        تلتقط نفس الحظر الواحد وتسجّله 6 مرات (شُوهد حيًا: ban_count=6 لحظر واحد)
        → الميزانية تنخفض 40% ست مرات متتالية والعدّاد يتضخم زورًا.
        الآن:
        - نفس الحظر (موعد انتهاء قريب من المسجل أو أقدم) → تمديد فقط بلا عدّاد ولا خفض
        - حظر جديد أطول فعليًا (+60ث فوق المسجل) → عدّاد + خفض ميزانية 40%
        - حظر طويل > BAN_LONG_THRESHOLD_SEC → الميزانية للأرضية مباشرة +
          تبريد استئناف ممتد (حتى 300ث) + إشعار تليجرام واحد بموعد النهاية الفعلي."""
        now = time.time()
        is_new = False
        long_ban = False
        with self._lock:
            until = (until_ms / 1000.0) if until_ms else (now + float(fallback_sec))
            until += 5.0  # هامش أمان فوق موعد Binance (توقيتات الخادم قد تختلف ثوانٍ)
            prev = self.banned_until
            is_new = (prev <= 0.0) or (until > prev + 60.0)
            self.banned_until = max(prev, until)
            long_ban = (self.banned_until - now) > max(60, int(BAN_LONG_THRESHOLD_SEC))
            if is_new:
                self.ban_count += 1
                self._last_ban_ts = now
                self._ramp_until = 0.0
                self._resumed = False  # سيُفعّل التدرج عند لحظة الاستئناف
                old_budget = self.budget
                if long_ban:
                    self.budget = self.budget_floor
                    self._active_cooldown = min(300, int(self.resume_cooldown * 2))
                else:
                    self.budget = max(self.budget_floor, int(self.budget * 0.6))
                    self._active_cooldown = self.resume_cooldown
            else:
                old_budget = self.budget
        end_utc = datetime.fromtimestamp(self.banned_until, tz=timezone.utc)
        end_alg = datetime.fromtimestamp(self.banned_until, tz=timezone(timedelta(hours=1)))
        if is_new:
            logger.warning(f"🚫 [حارس الطلبات] {'حظر طويل (تصعيد)' if long_ban else 'حظر مؤقت'} من Binance — الانتظار حتى: "
                           f"{end_utc.strftime('%H:%M:%S')} UTC ({end_alg.strftime('%H:%M:%S')} الجزائر) "
                           f"| الميزانية التكيفية: {old_budget} → {self.budget} وزن/دقيقة | تبريد الاستئناف: {self._active_cooldown}ث")
            if long_ban:
                try:
                    send_telegram_message(
                        f"🚫 *حظر API طويل من Binance*\n"
                        f"الانتظار حتى `{end_alg.strftime('%H:%M:%S')}` بتوقيت الجزائر — البوت في وضع سكون آمن "
                        f"(صفر طلبات) وسيستأنف تلقائيًا.\n"
                        f"السبب المرجح: IP مشترك على Render المجاني. الميزانية خُفّضت إلى {self.budget} وزن/دقيقة.")
                except Exception:
                    pass
        else:
            logger.info(f"🔁 [حارس الطلبات] تأكيد لنفس الحظر الساري — الامتداد حتى "
                        f"{end_utc.strftime('%H:%M:%S')} UTC (بلا خفض إضافي للميزانية أو العدّاد)")

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
                'ban_remaining_sec': round(self.banned_until - now + self._active_cooldown, 0) if banned else 0,
                'resume_cooldown_sec': int(self._active_cooldown),
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
            # [V9.16.0] أغلاق BTC من مركز WebSocket أولًا — REST احتياطي فقط
            closes = stream_hub.get_closes(BTC_SYMBOL, tf, 150) if stream_hub is not None else None
            if not closes and data_feed is not None:
                # [V9.20.0] شموع القائد من المزودين البديلين — صفر وزن باينانس
                kl = data_feed.get_klines(BTC_SYMBOL, tf, limit=150)
                if kl:
                    closes = [float(k[4]) for k in kl]
            if not closes:
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
    """خيط خلفي: يحدّث بوصلة BTC دوريًا — [V9.21.0] لا يتوقف عند حظر باينانس:
    مصادر البيانات (مركز WS + المزودون البديلون) محصنة ضد الحظر، وREST احتياط أخير فقط."""
    time.sleep(5)  # مهلة تهيئة العميل
    while True:
        try:
            if client:
                fetch_btc_trend_matrix(force=True)
        except Exception as e:
            logger.debug(f"[بوصلة BTC] خطأ: {e}")
        time.sleep(max(20, BTC_TREND_REFRESH_SEC))

# ============================================================
# [تحسين V9.13.0] خريطة القيادة: تصنيف العملات حسب القائد الذي تتبعه
# (BTC / ETH / SOL) واتخاذ قرار الشراء وفق سلوك القائد المتبوع.
# تكلفة الشبكة: 3 قادة × شموع 15م (وزن 2) كل 5 دقائق ≈ 1.2 وزن/دقيقة فقط.
# ============================================================
def _fetch_leader_closes(symbol: str) -> List[float]:
    """شموع إغلاق 15م للقائد — [V9.16.0] من مركز WebSocket أولًا (REST احتياطي، وزن 2)."""
    closes = stream_hub.get_closes(symbol, '15m', 300) if stream_hub is not None else None
    if closes:
        return closes
    if data_feed is not None:
        # [V9.20.0] شموع القادة من المزودين البديلين أولًا
        try:
            kl = data_feed.get_klines(symbol, '15m', limit=300)
            if kl:
                return [float(k[4]) for k in kl]
        except Exception:
            pass
    klines = safe_api_call(client.get_klines, symbol=symbol, interval='15m', limit=300, weight=2)
    if not klines:
        return []
    return [float(k[4]) for k in klines]

def fetch_leader_data(force: bool = False) -> None:
    """يحدّث كاش القادة (أسعار الإغلاق + سلوك كل قائد من compute_tf_trend)
    خلال نافذة LEADER_REFRESH_SEC ما لم force=True — عند الفشل تبقى آخر بيانات صالحة."""
    if not client:
        return
    now = time.time()
    with _leader_data_lock:
        fresh = (len(_leader_data) >= len(LEADER_SYMBOLS)
                 and all(now - d.get('ts', 0) < LEADER_REFRESH_SEC for d in _leader_data.values()))
    if fresh and not force:
        return
    for sym in LEADER_SYMBOLS:
        try:
            closes = _fetch_leader_closes(sym)
            if len(closes) < 60:
                continue
            trend = compute_tf_trend(closes[-150:])
            with _leader_data_lock:
                _leader_data[sym] = {'closes': closes, 'trend': trend, 'ts': time.time()}
        except Exception as e:
            logger.debug(f"[خريطة القيادة] تعذر تحديث {sym}: {e}")

def leader_data_loop():
    """خيط خلفي: بيانات القادة كل LEADER_REFRESH_SEC — [V9.21.0] لا يتوقف عند حظر
    باينانس: الأغلاق من مركز WS والمزودين البديلين (محصنة ضد الحظر)."""
    time.sleep(8)  # مهلة تهيئة العميل
    while True:
        try:
            if client:
                fetch_leader_data(force=True)
        except Exception as e:
            logger.debug(f"[خريطة القيادة] خطأ: {e}")
        time.sleep(max(30, LEADER_REFRESH_SEC))

def compute_leader_correlations(coin_closes: List[float]) -> Dict[str, float]:
    """ارتباط عوائد العملة مع كل قائد على نافذة LEADER_CORR_WINDOW شمعة 15م.
    محاذاة الذيل كافية (فرق دقائق بين لحظتي الجلب لا يغيّر الارتباط عمليًا).
    سلاسل ثابتة (std=0) تُعامل كارتباط صفر لتجنب قسمة صفر."""
    try:
        coin = pd.Series([float(c) for c in coin_closes[-LEADER_CORR_WINDOW:]], dtype=float).pct_change().dropna()
        with _leader_data_lock:
            snaps = {sym: list(d.get('closes') or []) for sym, d in _leader_data.items()}
        out: Dict[str, float] = {}
        for sym, closes in snaps.items():
            lead = pd.Series([float(c) for c in closes[-LEADER_CORR_WINDOW:]], dtype=float).pct_change().dropna()
            n = min(len(coin), len(lead))
            if n < 50 or float(coin.tail(n).std()) == 0 or float(lead.tail(n).std()) == 0:
                out[sym] = 0.0
                continue
            out[sym] = float(np.corrcoef(coin.tail(n).values, lead.tail(n).values)[0, 1])
        return out
    except Exception as e:
        logger.debug(f"[خريطة القيادة] خطأ حساب ارتباط: {e}")
        return {}

def update_leader_classification(symbol: str, coin_closes: List[float]) -> None:
    """يُستدعى من حلقة المسح لكل عملة عند كل دورة (بلا شبكة إطلاقًا):
    القائد = صاحب أعلى ارتباط إذا تجاوز LEADER_CORR_MIN، وإلا فالعملة "مستقلة".
    القادة أنفسهم يُوسمون 'قائد سيادي' ولا يخضعون لبوابة السلوك."""
    if symbol in LEADER_SYMBOLS:
        with _leader_map_lock:
            _leader_map[symbol] = {'leader': None, 'corr': 1.0, 'correlations': {},
                                   'is_leader': True, 'updated': time.time()}
        return
    corrs = compute_leader_correlations(coin_closes)
    if not corrs:
        return
    best_sym = max(corrs, key=corrs.get)
    best_corr = float(corrs.get(best_sym, 0.0))
    with _leader_map_lock:
        _leader_map[symbol] = {
            'leader': best_sym if best_corr >= LEADER_CORR_MIN else None,
            'corr': round(best_corr, 3),
            'correlations': {k: round(v, 3) for k, v in corrs.items()},
            'is_leader': False, 'updated': time.time()}

def passes_leader_behavior_filter(symbol: str) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """بوابة سلوك القائد (V9.13.0): القرار حسب سلوك ما تتبعه العملة.
    - تابعة لقائد هابط (score <= LEADER_BEARISH_SCORE) → رفض الشراء:
      فالتابع يتحرك عادة مع قائده، وشراء تابع مقابل قائد هابط = مواجهة السوق الحاكم.
    - قائد صاعد/محايد، أو عملة مستقلة، أو قائد نفسه → سماح.
    fail-open: بلا بيانات/تصنيف مؤقت نسمح — لا نحبس البوت بسبب نقص بيانات عابر."""
    if not USE_LEADER_FILTER:
        return True, None
    with _leader_map_lock:
        info = dict(_leader_map.get(symbol) or {})
    if not info:
        return True, None
    if info.get('is_leader'):
        return True, {**info, 'decision': 'leader_self'}
    leader = info.get('leader')
    corr = float(info.get('corr') or 0.0)
    if not leader or corr < LEADER_CORR_MIN:
        return True, {**info, 'decision': 'independent'}
    with _leader_data_lock:
        ld = _leader_data.get(leader) or {}
        trend = dict(ld.get('trend') or {})
    score = float(trend.get('score') or 0.0)
    out = {**info, 'leader_trend_score': score, 'leader_trend_label': trend.get('label', ''),
           'leader_trend_arrow': trend.get('arrow', ''), 'decision': 'pass'}
    if score <= LEADER_BEARISH_SCORE:
        out['decision'] = 'veto'
        with _scan_stats_lock:
            _leader_veto_stats[leader] += 1
        return False, out
    return True, out

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

def get_entry_price_ban_aware(symbol: str, exec_blocked: bool = False) -> Optional[float]:
    """[V9.21.0] سعر الدخول دون أن يحبس الحظر خيط المسح:
    1) باينانس REST عند سماح الحالة (السعر المرجعي لمنصة التنفيذ)
    2) مركز WebSocket — سعر باينانص حي (~2ث) بلا وزن ويعمل أثناء الحظر
    3) المزودون البديلون (Bybit/OKX/Gate) — إغلاق شمعة 1م الجارية
    يعيد None إن فشل الجميع فيتخطى الإشارة بدل التعلّق."""
    if not exec_blocked:
        try:
            return float(safe_get_symbol_ticker(symbol=symbol)['price'])
        except Exception as e:
            logger.warning(f"⚠️ [{symbol}] فشل سعر باينانس ({str(e)[:80]}) — التحويل للمصادر البديلة...")
    if stream_hub is not None:
        try:
            p = (stream_hub.get_prices([symbol.upper()]) or {}).get(symbol.upper())
            if p and float(p) > 0:
                return float(p)
        except Exception:
            pass
    if data_feed is not None:
        try:
            kl = data_feed.get_klines(symbol, '1m', limit=2)
            if kl:
                return float(kl[-1][4])
        except Exception:
            pass
    return None

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

# ============================================================
# [V9.20.0] طبقة البيانات متعددة المصادر — نهاية ابتلاع وزن باينانس
# ------------------------------------------------------------
# الفكرة: بيانات السوق (شموع/تكه 24س/عمق السوق) لا علاقة لها بالتنفيذ،
# فتُجلب من منصات أسواقها مطابقة تقريبًا لباينانس وحدود طلباتها سخية:
#   Bybit → OKX → Gate.io — وباينانس احتياط أخير عبر المسارات القديمة.
# كل مزود يطبّع مخرجاته إلى صيغة Binance الأصلية (12 عمودًا للشموع،
# مفاتيح ticker القياسية، {'bids','asks'} للعمق) فلا يتغير أي مستهلك لاحق.
# عقد الواجهة: get_* تُعيد None عند تعذر كل المزودين → يستدعي الموقع
# مسار باينانس القديم (تدهور رشيق دائمًا دون استثناءات).
# ============================================================
def lookback_to_candles(lookback_str: str, interval: str) -> int:
    """[V9.20.0] يحوّل نص lookback ('800 hour') إلى عدد شموع لطلبات المزودين البديلين."""
    interval_min = _INTERVAL_MINUTES.get(str(interval).lower(), 15)
    m = re.match(r'\s*(\d+)\s*([a-zA-Z]+)', str(lookback_str))
    if not m:
        return 500
    qty = int(m.group(1))
    unit_min = _LOOKBACK_UNIT_MINUTES.get(m.group(2).lower(), 1440)
    # [V9.22.0] السقف 1000→5000: المزود يدعم الآن الترقيم الرجعي — العمق الكامل يُطلب فعلًا
    return max(50, min(5000, int(math.ceil(qty * unit_min / float(interval_min)))))

class DataProvider:
    """أساس مزود بيانات سوق خارجي — التطبيع إلى صيغة Binance هنا."""
    name: str = 'base'
    base_url: str = ''
    # خريطة فريم البوت → فريم المنصة (الفريمات غير المدعومة تُحذف فيقرَ التالي)
    INTERVAL_MAP: Dict[str, str] = {}

    def to_symbol(self, symbol: str) -> Optional[str]:
        return symbol

    def supports(self, symbol: str, interval: str) -> bool:
        return (self.to_symbol(symbol) is not None and str(interval).lower() in self.INTERVAL_MAP)

    def _get_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Optional[Any]:
        try:
            r = requests.get(self.base_url + path, params=params or {},
                             timeout=max(3, int(DATA_FEED_TIMEOUT_SEC)),
                             headers={'User-Agent': 'crypto-bot-datafeed/9.20'})
            if r.status_code != 200:
                logger.debug(f"[تغذية البيانات:{self.name}] HTTP {r.status_code} من {path}")
                return None
            return r.json()
        except Exception as e:
            logger.debug(f"[تغذية البيانات:{self.name}] فشل {path}: {str(e)[:120]}")
            return None

    @staticmethod
    def _interval_ms(interval: str) -> int:
        return int(_INTERVAL_MINUTES.get(str(interval).lower(), 15)) * 60 * 1000

    @staticmethod
    def _f(v) -> Optional[float]:
        try:
            x = float(v)
            return x if x == x and abs(x) != float('inf') else None
        except (TypeError, ValueError):
            return None

    @classmethod
    def _binance_row(cls, ts_ms, o, h, l, c, vol, qvol, interval: str) -> Optional[list]:
        """صف شمعة موحّد بصيغة Binance (12 عمودًا) — يستقبله كل الكود القائم دون تعديل."""
        try:
            ts = int(ts_ms)
        except (TypeError, ValueError):
            return None
        if ts <= 0:
            return None
        fo, fh, fl, fc, fv = (cls._f(x) for x in (o, h, l, c, vol))
        if None in (fo, fh, fl, fc) or fv is None or min(fo, fh, fl, fc) <= 0:
            return None
        qv = cls._f(qvol)
        return [ts, fo, fh, fl, fc, fv, ts + cls._interval_ms(interval) - 1,
                qv if qv is not None else 0.0, 0, 0.0, 0.0, '0']

    @staticmethod
    def _ob_pairs(side) -> List[List[float]]:
        out: List[List[float]] = []
        for e in side or []:
            try:
                out.append([float(e[0]), float(e[1])])
            except (TypeError, ValueError, IndexError):
                continue
        return out

    # --- واجهة الجلب (تُطبق في كل مزود) ---
    def fetch_klines(self, symbol: str, interval: str, limit: int = 300) -> Optional[List[list]]:
        raise NotImplementedError

    def fetch_all_24h_tickers(self) -> Optional[List[dict]]:
        raise NotImplementedError

    def fetch_order_book(self, symbol: str, limit: int = 25) -> Optional[Dict[str, list]]:
        raise NotImplementedError

    def fetch_symbols(self) -> Optional[Set[str]]:
        raise NotImplementedError

class BybitProvider(DataProvider):
    """Bybit API v5 (spot) — أقرب أسواق لباينانس، حدود عامة سخية جدًا."""
    name = 'bybit'
    base_url = 'https://api.bybit.com'
    INTERVAL_MAP = {'1m': '1', '3m': '3', '5m': '5', '15m': '15', '30m': '30',
                    '1h': '60', '2h': '120', '4h': '240', '6h': '360', '12h': '720', '1d': 'D'}

    def to_symbol(self, symbol: str) -> Optional[str]:
        s = str(symbol)
        return s if s.endswith('USDT') and len(s) > 4 else None

    # [V9.22.0] سقف الصفحة الواحدة لدى Bybit — الأعمق من ذلك يُجلب بترقيم صفحات رجعي
    _PAGE_MAX = 1000

    def fetch_klines(self, symbol: str, interval: str, limit: int = 300) -> Optional[List[list]]:
        bsym, biv = self.to_symbol(symbol), self.INTERVAL_MAP.get(str(interval).lower())
        if not bsym or not biv:
            return None
        lim = max(25, min(5000, int(limit)))
        out: List[list] = []
        seen: Set[int] = set()
        end_ms: Optional[int] = None
        # [V9.22.0] كان القص الصامت عند 1000 يمنع بلوغ هدف تدفئة مركز WebSocket
        # (3680 شمعة 15م = عمق 30 يومًا) فتعُلّق buffers_warm عند 1/37 وتتحول كل
        # القراءات إلى REST احتياطي (~119/ساعة). الترقيم الرجعي يعيد العمق الكامل
        for _page in range(max(1, -(-lim // self._PAGE_MAX))):
            params: Dict[str, Any] = {'category': 'spot', 'symbol': bsym, 'interval': biv,
                                      'limit': max(1, min(self._PAGE_MAX, lim - len(out)))}
            if end_ms is not None:
                params['end'] = int(end_ms)
            data = self._get_json('/v5/market/kline', params)
            if not isinstance(data, dict) or data.get('retCode') != 0:
                break
            rows = (data.get('result') or {}).get('list') or []
            if not rows:
                break
            page_oldest = None
            for r in rows:
                try:
                    ot = int(r[0])
                    if ot in seen:
                        continue
                    seen.add(ot)
                    br = self._binance_row(r[0], r[1], r[2], r[3], r[4], r[5], r[6], interval)
                    if br:
                        out.append(br)
                    page_oldest = ot if page_oldest is None else min(page_oldest, ot)
                except (TypeError, ValueError, IndexError):
                    continue
            if page_oldest is None or len(out) >= lim:
                break
            end_ms = page_oldest - 1
        if not out:
            return None
        out.sort(key=lambda k: k[0])
        return out[-lim:]

    def fetch_all_24h_tickers(self) -> Optional[List[dict]]:
        data = self._get_json('/v5/market/tickers', {'category': 'spot'})
        if not isinstance(data, dict) or data.get('retCode') != 0:
            return None
        out: List[dict] = []
        for t in (data.get('result') or {}).get('list') or []:
            try:
                last = self._f(t.get('lastPrice'))
                if not last or last <= 0:
                    continue
                out.append({'symbol': t.get('symbol'),
                            'lastPrice': last,
                            'highPrice': self._f(t.get('highPrice24h')) or 0.0,
                            'lowPrice': self._f(t.get('lowPrice24h')) or 0.0,
                            'quoteVolume': self._f(t.get('turnover24h')) or 0.0,
                            'priceChangePercent': (self._f(t.get('price24hPcnt')) or 0.0) * 100.0})
            except (TypeError, ValueError):
                continue
        return out or None

    def fetch_order_book(self, symbol: str, limit: int = 25) -> Optional[Dict[str, list]]:
        bsym = self.to_symbol(symbol)
        if not bsym:
            return None
        lim = max(1, min(200, int(limit)))
        data = self._get_json('/v5/market/orderbook',
                              {'category': 'spot', 'symbol': bsym, 'limit': lim})
        if not isinstance(data, dict) or data.get('retCode') != 0:
            return None
        res = data.get('result') or {}
        # Bybit v5 يعيد مفاتيح مختصرة 'b'/'a' (مدقق حيًا) — نقبل الصيغتين
        bids = res.get('bids') or res.get('b')
        asks = res.get('asks') or res.get('a')
        if not bids or not asks:
            return None
        return {'bids': self._ob_pairs(bids), 'asks': self._ob_pairs(asks)}

    def fetch_symbols(self) -> Optional[Set[str]]:
        out: Set[str] = set()
        cursor = ''
        for _ in range(6):  # حتى ~6000 رمز
            params: Dict[str, Any] = {'category': 'spot', 'limit': 1000}
            if cursor:
                params['cursor'] = cursor
            data = self._get_json('/v5/market/instruments-info', params)
            if not isinstance(data, dict) or data.get('retCode') != 0:
                break
            res = data.get('result') or {}
            for s in res.get('list') or []:
                if str(s.get('status', 'Trading')).lower() == 'trading' and s.get('symbol'):
                    out.add(s['symbol'])
            cursor = res.get('nextPageCursor') or ''
            if not cursor:
                break
        return out or None

class OKXProvider(DataProvider):
    """OKX API v5 (spot) — موثوقة بشموع دقيقة؛ تعليم الرموز BTC-USDT."""
    name = 'okx'
    base_url = 'https://www.okx.com'
    INTERVAL_MAP = {'1m': '1m', '3m': '3m', '5m': '5m', '15m': '15m', '30m': '30m',
                    '1h': '1H', '2h': '2H', '4h': '4H', '6h': '6H', '12h': '12H', '1d': '1D'}

    def to_symbol(self, symbol: str) -> Optional[str]:
        s = str(symbol)
        return f"{s[:-4]}-USDT" if s.endswith('USDT') and len(s) > 4 else None

    def fetch_klines(self, symbol: str, interval: str, limit: int = 300) -> Optional[List[list]]:
        inst, bar = self.to_symbol(symbol), self.INTERVAL_MAP.get(str(interval).lower())
        if not inst or not bar:
            return None
        lim = max(25, min(1000, int(limit)))
        # endpoint الحديث يعطي حتى 300 شمعة؛ النواقص تُستكمل من history-candles (100/صفحة)
        data = self._get_json('/api/v5/market/candles',
                              {'instId': inst, 'bar': bar, 'limit': min(300, lim)})
        rows: list = []
        if isinstance(data, dict) and str(data.get('code')) == '0' and isinstance(data.get('data'), list):
            rows = list(data['data'])
        tries = 0
        while rows and len(rows) < lim and tries < 4:
            tries += 1
            try:
                oldest = min(int(r[0]) for r in rows)
            except (TypeError, ValueError):
                break
            hdata = self._get_json('/api/v5/market/history-candles',
                                   {'instId': inst, 'bar': bar, 'after': oldest, 'limit': 100})
            if not (isinstance(hdata, dict) and str(hdata.get('code')) == '0'
                    and isinstance(hdata.get('data'), list) and hdata['data']):
                break
            rows.extend(hdata['data'])
        if not rows:
            return None
        out: List[list] = []
        for r in rows:
            try:
                # r = [ts, o, h, l, c, vol(base), volCcy, volCcyQuote, confirm]
                qv = self._f(r[7])
                if qv is None:
                    qv = self._f(r[6]) or 0.0
                br = self._binance_row(r[0], r[1], r[2], r[3], r[4], r[5], qv, interval)
                if br:
                    out.append(br)
            except (TypeError, ValueError, IndexError):
                continue
        if not out:
            return None
        uniq = {k[0]: k for k in out}
        return [uniq[k] for k in sorted(uniq)][-lim:]

    def fetch_all_24h_tickers(self) -> Optional[List[dict]]:
        data = self._get_json('/api/v5/market/tickers', {'instType': 'SPOT'})
        if not (isinstance(data, dict) and str(data.get('code')) == '0'):
            return None
        out: List[dict] = []
        for t in data.get('data') or []:
            try:
                last, open24 = self._f(t.get('last')), self._f(t.get('open24h'))
                if not last or last <= 0:
                    continue
                pct = ((last - open24) / open24 * 100.0) if (open24 and open24 > 0) else 0.0
                out.append({'symbol': str(t.get('instId', '')).replace('-', ''),
                            'lastPrice': last,
                            'highPrice': self._f(t.get('high24h')) or 0.0,
                            'lowPrice': self._f(t.get('low24h')) or 0.0,
                            'quoteVolume': self._f(t.get('volCcy24h')) or 0.0,
                            'priceChangePercent': pct})
            except (TypeError, ValueError, ZeroDivisionError):
                continue
        return out or None

    def fetch_order_book(self, symbol: str, limit: int = 25) -> Optional[Dict[str, list]]:
        inst = self.to_symbol(symbol)
        if not inst:
            return None
        lim = max(1, min(400, int(limit)))
        data = self._get_json('/api/v5/market/books', {'instId': inst, 'sz': lim})
        if not (isinstance(data, dict) and str(data.get('code')) == '0' and data.get('data')):
            return None
        d = (data['data'] or [{}])[0]
        if not d.get('bids') or not d.get('asks'):
            return None
        return {'bids': self._ob_pairs(d['bids']), 'asks': self._ob_pairs(d['asks'])}

    def fetch_symbols(self) -> Optional[Set[str]]:
        data = self._get_json('/api/v5/public/instruments', {'instType': 'SPOT'})
        if not (isinstance(data, dict) and str(data.get('code')) == '0'):
            return None
        out = {str(t['instId']).replace('-', '') for t in data.get('data') or []
               if t.get('state') == 'live' and t.get('instId')}
        return out or None

class GateProvider(DataProvider):
    """Gate.io API v4 (spot) — أوسع تغطية أزواج؛ تعليم الرموز BTC_USDT.
    لا يدعم 3m/2h/6h/12h — تُتخطى تلقائيًا لصالح غيره."""
    name = 'gate'
    base_url = 'https://api.gateio.ws/api/v4'
    INTERVAL_MAP = {'1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m',
                    '1h': '1h', '4h': '4h', '8h': '8h', '1d': '1d'}

    def to_symbol(self, symbol: str) -> Optional[str]:
        s = str(symbol)
        return f"{s[:-4]}_USDT" if s.endswith('USDT') and len(s) > 4 else None

    def fetch_klines(self, symbol: str, interval: str, limit: int = 300) -> Optional[List[list]]:
        pair, giv = self.to_symbol(symbol), self.INTERVAL_MAP.get(str(interval).lower())
        if not pair or not giv:
            return None
        lim = max(25, min(1000, int(limit)))
        data = self._get_json('/spot/candlesticks',
                              {'currency_pair': pair, 'interval': giv, 'limit': lim})
        if not isinstance(data, list) or not data:
            return None
        out: List[list] = []
        for r in data:
            try:
                if isinstance(r, dict):
                    # صيغة كائن (احتياط): حقول مسماة — sum=اقتباسي، volume=أساسي
                    ts = int(r.get('time') or r.get('current') or 0)
                    o, h, l, c = r.get('open'), r.get('high'), r.get('low'), r.get('close')
                    qv_raw, bv_raw = r.get('sum'), r.get('volume')
                else:
                    # الصيغة الفعلية (مدققة حيًا): [t(ث), quote_vol, o, c, h, l, base_vol, done]
                    ts = int(r[0])
                    o, c, h, l = r[2], r[3], r[4], r[5]
                    qv_raw, bv_raw = r[1], r[6]
                if 0 < ts < 10**12:  # Gate يعيد ثوانٍ
                    ts *= 1000
                qv = self._f(qv_raw)
                if qv is None:
                    qv = self._f(bv_raw) or 0.0
                br = self._binance_row(ts, o, h, l, c, bv_raw, qv, interval)
                if br:
                    out.append(br)
            except (TypeError, ValueError, IndexError):
                continue
        if not out:
            return None
        uniq = {k[0]: k for k in out}
        return [uniq[k] for k in sorted(uniq)][-lim:]

    def fetch_all_24h_tickers(self) -> Optional[List[dict]]:
        data = self._get_json('/spot/tickers')
        if not isinstance(data, list) or not data:
            return None
        out: List[dict] = []
        for t in data:
            try:
                last = self._f(t.get('last'))
                if not last or last <= 0:
                    continue
                out.append({'symbol': str(t.get('currency_pair', '')).replace('_', ''),
                            'lastPrice': last,
                            'highPrice': self._f(t.get('high_24h')) or 0.0,
                            'lowPrice': self._f(t.get('low_24h')) or 0.0,
                            'quoteVolume': self._f(t.get('quote_volume')) or 0.0,
                            'priceChangePercent': self._f(t.get('change_percentage')) or 0.0})
            except (TypeError, ValueError):
                continue
        return out or None

    def fetch_order_book(self, symbol: str, limit: int = 25) -> Optional[Dict[str, list]]:
        pair = self.to_symbol(symbol)
        if not pair:
            return None
        allowed = (5, 10, 20, 50, 100)
        lim = min([x for x in allowed if x >= int(limit)] or [100])
        data = self._get_json('/spot/order_book', {'currency_pair': pair, 'limit': lim})
        if not isinstance(data, dict) or not data.get('bids') or not data.get('asks'):
            return None
        return {'bids': self._ob_pairs(data['bids']), 'asks': self._ob_pairs(data['asks'])}

    def fetch_symbols(self) -> Optional[Set[str]]:
        data = self._get_json('/spot/currency_pairs')
        if not isinstance(data, list) or not data:
            return None
        out = {str(p['id']).replace('_', '') for p in data
               if p.get('trade_status') == 'tradable' and p.get('id')}
        return out or None

_PROVIDER_REGISTRY: Dict[str, type] = {'bybit': BybitProvider, 'okx': OKXProvider, 'gate': GateProvider}

class MultiSourceDataFeed:
    """موجّه البيانات: يجرب المزودين بالترتيب المُعدّ، وعند فشلهم كلهم يُعيد None
    ليمرّ الموقع القديم إلى مسار باينانس المعروف (الحارس/الميزانية/إعادة المحاولة)."""
    def __init__(self):
        self.providers: List[DataProvider] = []
        self.coverage: Dict[str, Set[str]] = {}
        self.health: Dict[str, Dict[str, Any]] = {}
        self.stats: Counter = Counter()
        self.lock = Lock()
        self.last_source: Dict[str, str] = {'klines': 'binance', 'tickers': 'binance', 'orderbook': 'binance'}
        for nm in [x.strip().lower() for x in str(DATA_FEED_PROVIDERS).split(',') if x.strip()]:
            cls = _PROVIDER_REGISTRY.get(nm)
            if cls is None:
                logger.warning(f"🌐 [تغذية البيانات] مزود مجهول في الإعداد: {nm}")
                continue
            p = cls()
            self.providers.append(p)
            self.health[p.name] = {'ok': 0, 'fail': 0, 'consec_fail': 0,
                                   'cooldown_until': 0.0, 'last_error': ''}
            logger.info(f"🌐 [تغذية البيانات] مزود مفعّل: {p.name} ({p.base_url})")

    def _enabled(self) -> bool:
        return DATA_FEED_MODE == 'multi' and bool(self.providers)

    def start_coverage_worker(self) -> None:
        """خيط خلفي: يبني خريطة رموز كل مزود مرة واحدة عند الإقلاع (طلب لكل صفحة)."""
        if not self._enabled():
            return
        Thread(target=self._coverage_loop, daemon=True).start()

    def _coverage_loop(self) -> None:
        time.sleep(4.0)  # مهلة اهتداء الشبكة
        for attempt in range(4):
            for p in self.providers:
                if p.name in self.coverage:
                    continue
                try:
                    syms = p.fetch_symbols()
                except Exception as e:
                    logger.debug(f"[تغذية البيانات] خريطة {p.name} فشلت: {e}")
                    syms = None
                if syms:
                    with self.lock:
                        self.coverage[p.name] = syms
                    logger.info(f"🌐 [تغذية البيانات] خريطة رموز {p.name}: {len(syms)} زوج")
            if len(self.coverage) >= len(self.providers):
                return
            time.sleep(45)
        logger.warning("🌐 [تغذية البيانات] بعض خرائط الرموز غير مكتملة — وضع تفاؤلي (التجربة عند الطلب)")

    def _record_ok(self, name: str) -> None:
        with self.lock:
            h = self.health[name]
            h['ok'] += 1
            h['consec_fail'] = 0
            h['last_error'] = ''
        self.stats[f'{name}_ok'] += 1

    def _record_fail(self, name: str, err: str = '') -> None:
        with self.lock:
            h = self.health[name]
            h['fail'] += 1
            h['consec_fail'] += 1
            h['last_error'] = str(err)[:140]
            if h['consec_fail'] >= max(1, DATA_FEED_FAIL_THRESHOLD):
                h['cooldown_until'] = time.time() + max(30, DATA_FEED_COOLDOWN_SEC)
                h['consec_fail'] = 0
                logger.warning(f"🌐 [تغذية البيانات] تبريد {name} لمدة {DATA_FEED_COOLDOWN_SEC}ث بعد فشل متكرر")
        self.stats[f'{name}_fail'] += 1

    def _healthy(self, name: str) -> bool:
        with self.lock:
            return time.time() >= self.health.get(name, {}).get('cooldown_until', 0.0)

    def _covered(self, provider: DataProvider, bsym: str) -> bool:
        with self.lock:
            cov = self.coverage.get(provider.name)
        if cov is None:
            return True  # تفاؤل قبل اكتمال الخريطة
        return bsym in cov

    def _candidates(self, symbol: str, interval: Optional[str] = None) -> List[DataProvider]:
        out: List[DataProvider] = []
        for p in self.providers:
            if not self._healthy(p.name):
                continue
            bsym = p.to_symbol(symbol)
            if bsym is None:
                continue
            if interval is not None and str(interval).lower() not in p.INTERVAL_MAP:
                continue
            if not self._covered(p, bsym):
                continue
            out.append(p)
        return out

    def get_klines(self, symbol: str, interval: str, limit: int = 300) -> Optional[List[list]]:
        """شموع بصيغة Binance (12 عمودًا) من أول مزود ينجح — None = احتياط باينانس."""
        if not self._enabled():
            return None
        for p in self._candidates(symbol, interval):
            try:
                rows = p.fetch_klines(symbol, interval, limit)
            except Exception as e:
                rows = None
                self._record_fail(p.name, f'klines استثناء: {e}')
            if rows:
                self._record_ok(p.name)
                self.last_source['klines'] = p.name
                self.stats['klines_served'] += 1
                return rows
            else:
                self._record_fail(p.name, f'klines {symbol} {interval} فارغة/غير متاحة')
        self.stats['klines_binance_fallback'] += 1
        return None

    def get_24h_tickers(self) -> Optional[List[dict]]:
        """تكه 24 ساعة لكل السوق في طلب واحد مجاني (بديل وزن 80 لدى باينانس)."""
        if not self._enabled():
            return None
        for p in self._candidates('BTCUSDT'):
            try:
                tk = p.fetch_all_24h_tickers()
            except Exception as e:
                tk = None
                self._record_fail(p.name, f'tickers استثناء: {e}')
            if tk:
                self._record_ok(p.name)
                self.last_source['tickers'] = p.name
                self.stats['tickers_served'] += 1
                return tk
            else:
                self._record_fail(p.name, 'tickers فارغة')
        self.stats['tickers_binance_fallback'] += 1
        return None

    def get_order_book(self, symbol: str, limit: int = 25) -> Optional[Dict[str, list]]:
        """عمق السوق بصيغة Binance — النِسَب النسبية للعرض/الطلب صالحة عبر المنصات المتشابهة."""
        if not self._enabled():
            return None
        for p in self._candidates(symbol):
            try:
                ob = p.fetch_order_book(symbol, limit)
            except Exception as e:
                ob = None
                self._record_fail(p.name, f'orderbook استثناء: {e}')
            if ob and ob.get('bids') and ob.get('asks'):
                self._record_ok(p.name)
                self.last_source['orderbook'] = p.name
                self.stats['orderbook_served'] += 1
                return ob
            else:
                self._record_fail(p.name, f'orderbook {symbol} فارغ')
        self.stats['orderbook_binance_fallback'] += 1
        return None

    def status_snapshot(self) -> Dict[str, Any]:
        with self.lock:
            provs = []
            for p in self.providers:
                h = self.health.get(p.name, {})
                provs.append({'name': p.name, 'base_url': p.base_url,
                              'intervals': sorted(p.INTERVAL_MAP.keys()),
                              'symbols_mapped': len(self.coverage.get(p.name, []) or []),
                              'ok': h.get('ok', 0), 'fail': h.get('fail', 0),
                              'consec_fail': h.get('consec_fail', 0),
                              'cooling_down': time.time() < h.get('cooldown_until', 0.0),
                              'last_error': h.get('last_error', '')})
        return {'mode': DATA_FEED_MODE, 'enabled': self._enabled(),
                'providers': provs, 'stats': dict(self.stats),
                'last_source': dict(self.last_source)}

data_feed: Optional[MultiSourceDataFeed] = MultiSourceDataFeed()

def safe_get_asset_balance(asset: str):
    return safe_api_call(client.get_asset_balance, asset=asset, weight=5)

def safe_get_order(symbol: str, order_id):
    return safe_api_call(client.get_order, symbol=symbol, orderId=order_id, weight=2)

def safe_create_order(**params):
    """الأوامر الحقيقية: نحجز الوزن فقط ولا نعيد المحاولة تلقائيًا لتجنب ازدواجية الأوامر."""
    rate_guard.acquire(weight=1)
    return client.create_order(**params)

# ============================================================
# [V9.16.0] مركز بيانات WebSocket — الشفاء الجذري لحظر IP (-1003)
# ------------------------------------------------------------
# المشكلة: IP الخروج المشترك على Render المجاني يُحظر من Binance REST
# (وزن الطلبات يتراكم من جيران الخادم أيضًا) — وأي إعادة محاولة REST مهما
# تحسنت تبقى أسيرة الحظر نفسه. رسالة Binance نفسها تحدد الحل:
# "Please use WebSocket Streams for live updates to avoid bans".
# الحل: تدفقات Binance العامة (wss://stream.binance.com:9443) لا تحتاج
# مفاتيح ولا تخضع لأوزان REST إطلاقًا وتعمل أثناء الحظر — فتصبح الشموع
# والأسعار تصل عبر WebSocket، ويبقى REST فقط للتهيئة/الأوامر/التعبئة
# الأولى العميقة (بصمة وزن شبه معدومة).
# الفلسفة كما هي: خيوط الويب تقرأ كاشًا فقط، والمركز كله خيوط خلفية.
# تعطل المركز أو نقص مخزون → مسار REST القديم يعمل كما هو (تدهور رشيق).
# ============================================================
class MarketStreamHub:
    _KLINE_COLUMNS = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'taker_buy_base']
    # أعمدة صف REST kline [0..11] المستخدمة: 0=وقت الفتح 1..5=OHLCV 7=quote_volume 9=taker_buy_base
    _REST_IDX = (0, 1, 2, 3, 4, 5, 7, 9)

    def __init__(self):
        self._lock = Lock()
        self._closed: Dict[Tuple[str, str], Dict[int, tuple]] = {}
        self._forming: Dict[Tuple[str, str], tuple] = {}
        self._desired: Set[Tuple[str, str]] = set()
        self._subscribed: Set[Tuple[str, str]] = set()
        self._backfill_state: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._last_msg_ts: float = 0.0
        self._connected: bool = False
        self._reconnects: int = 0
        self._served_frames: int = 0
        self._subscribe_id: int = 1
        self._ws = None
        self._stop: bool = False
        self._pinned: Set[str] = set([BTC_SYMBOL] + list(LEADER_SYMBOLS))
        # أهداف الاكتمال — مطابقة تمامًا لما يطلبه مسار REST (شرط الخدمة من المخزون)
        htf_days = 40  # is_htf_bullish_confirmation يستدعي days=40 ثابتًا
        self._target_count: Dict[str, int] = {
            '15m': (SIGNAL_GENERATION_LOOKBACK_DAYS * 24 + 200) * 4,
            '1h': (htf_days * 24 + 200),
            '4h': 320,  # [V9.22.0] 200→320: بوصلة BTC تستدعي days=20 ← (20*24+200)*60/240
        }
        self._backfill_lookback: Dict[str, str] = {
            '15m': f"{SIGNAL_GENERATION_LOOKBACK_DAYS * 24 + 200} hour",
            '1h': f"{htf_days * 24 + 200} hour",
            '4h': '1300 hour',  # [V9.22.0] 800→1300: يغطي هدف 320 شمعة
        }
        self._maxlen: Dict[str, int] = {'15m': STREAM_HUB_BUFFER_15M, '1h': STREAM_HUB_BUFFER_1H}

    # ---------------- الاشتراكات ----------------
    def _pinned_streams(self) -> Set[Tuple[str, str]]:
        # الرموز المثبتة لا تُلغى اشتراكاتها أبدًا: BTC + القادة + الصفقات المفتوحة
        pins: Set[Tuple[str, str]] = set()
        for s in self._pinned:
            pins.add((s, '15m')); pins.add((s, '1h'))
        pins.add((BTC_SYMBOL, '4h'))  # بوصلة BTC تحتاج فريم 4h
        try:
            with signal_cache_lock:
                for s in list(open_signals_cache.keys()):
                    su = str(s).upper()
                    pins.add((su, '15m')); pins.add((su, '1h'))
        except Exception:
            pass
        return pins

    def set_universe(self, symbols: List[str]) -> None:
        """تحديث قائمة الاشتراك المطلوبة مع إرسال الفرق فقط (SUBSCRIBE/UNSUBSCRIBE)."""
        desired = set(self._pinned_streams())
        for s in symbols or []:
            su = str(s).upper()
            desired.add((su, '15m')); desired.add((su, '1h'))
        with self._lock:
            new_keys = desired - self._desired
            gone_keys = self._desired - desired
            self._desired = desired
            for k in new_keys:
                st = self._backfill_state.get(k)
                if st is None or st.get('status') == 'done' and not self._closed.get(k):
                    self._backfill_state[k] = {'status': 'pending', 'attempts': 0, 'next_try': 0.0}
            self._subscribe_id += 1
            sid = self._subscribe_id
            ws = self._ws
            connected = self._connected
            to_sub = sorted(new_keys)
            to_unsub = sorted(k for k in gone_keys if k in self._subscribed)
            self._subscribed |= set(to_sub)
            self._subscribed -= set(to_unsub)
        if connected and ws is not None and (to_sub or to_unsub):
            try:
                if to_sub:
                    ws.send(json.dumps({'method': 'SUBSCRIBE', 'params': [f"{s.lower()}@kline_{iv}" for s, iv in to_sub], 'id': sid}))
                if to_unsub:
                    ws.send(json.dumps({'method': 'UNSUBSCRIBE', 'params': [f"{s.lower()}@kline_{iv}" for s, iv in to_unsub], 'id': sid}))
                logger.info(f"🛰️ [مركز البيانات] تحديث الاشتراكات: +{len(to_sub)} / -{len(to_unsub)}")
            except Exception as e:
                logger.warning(f"🛰️ [مركز البيانات] فشل إرسال تحديث الاشتراك: {e}")

    def _current_universe(self) -> List[str]:
        try:
            with universe_lock:
                return list(validated_symbols_to_scan)
        except Exception:
            return []

    def subscribed_count(self) -> int:
        with self._lock:
            return len(self._subscribed)

    def is_connected(self) -> bool:
        with self._lock:
            return self._connected

    # ---------------- حلقة الاتصال ----------------
    def start(self) -> None:
        # [إصلاح عاجل V9.16.0] target= صريح — النسخة الأولى كانت تمرر الأسلوب موضعيًا
        # فيدخل معامل group فيرفع TypeError بصمت ويُبطل المركز كله
        websocket.setdefaulttimeout(15)  # مهلة اتصال TCP/TLS بدل التعليق الافتراضي
        Thread(target=self._run_loop, daemon=True).start()
        Thread(target=self._backfill_loop, daemon=True).start()
        Thread(target=self._maintain_loop, daemon=True).start()
        logger.info("🛰️ [مركز البيانات] انطلق — WebSocket لقنوات الشموع الحية (يعمل حتى أثناء حظر REST)")

    def _run_loop(self) -> None:
        backoff = 5.0
        while not self._stop:
            try:
                self._connect_once()
                backoff = 5.0
            except Exception as e:
                logger.warning(f"🛰️ [مركز البيانات] انقطع الاتصال: {str(e)[:120]} — إعادة المحاولة بعد {int(backoff)}ث")
            with self._lock:
                self._connected = False
                self._reconnects += 1
            time.sleep(backoff)
            backoff = min(60.0, backoff * 1.5)

    def _connect_once(self) -> None:
        with self._lock:
            desired = sorted(self._desired)
        streams_q = '/'.join(f"{s.lower()}@kline_{iv}" for s, iv in desired)
        url = STREAM_HUB_BASE_URL + (f"?streams={streams_q}" if streams_q else '')
        logger.info(f"🛰️ [مركز البيانات] الاتصال بـ Binance WebSocket — {len(desired)} تدفق شموع...")
        ws = websocket.WebSocketApp(
            url,
            on_open=self._on_open, on_message=self._on_message,
            on_error=self._on_error, on_close=self._on_close,
        )
        with self._lock:
            self._ws = ws
        ws.run_forever(ping_interval=180, ping_timeout=20)

    def _on_open(self, ws) -> None:
        with self._lock:
            self._connected = True
            desired = sorted(self._desired)
            self._subscribe_id += 1
            sid = self._subscribe_id
        try:
            if desired:
                ws.send(json.dumps({'method': 'SUBSCRIBE', 'params': [f"{s.lower()}@kline_{iv}" for s, iv in desired], 'id': sid}))
            with self._lock:
                self._subscribed = set(desired)
            logger.info(f"🛰️ [مركز البيانات] WebSocket متصل — {len(desired)} تدفق شموع حية (بلا أوزان REST)")
        except Exception as e:
            logger.warning(f"🛰️ [مركز البيانات] فشل الاشتراك الأولي: {e}")

    def _on_message(self, ws, message) -> None:
        try:
            payload = json.loads(message)
            d = payload.get('data') or {}
            k = d.get('k')
            if not k:
                return
            key = (str(d.get('s', '')).upper(), str(k.get('i', '')).lower())
            tup = (int(k['t']), float(k['o']), float(k['h']), float(k['l']), float(k['c']),
                   float(k['v']), float(k['q']), float(k['V']))
            with self._lock:
                self._last_msg_ts = time.time()
                if key in self._subscribed:
                    self._forming[key] = tup
                    if k.get('x'):
                        buf = self._closed.setdefault(key, {})
                        buf[tup[0]] = tup
                        mx = self._maxlen.get(key[1], STREAM_HUB_BUFFER_DEFAULT)
                        if len(buf) > mx:
                            for ot in sorted(buf)[:len(buf) - mx]:
                                buf.pop(ot, None)
        except Exception:
            pass  # رسالة تالفة/غير متوقعة — لا تسمح لها بقتل الخيط

    def _on_error(self, ws, error) -> None:
        logger.debug(f"🛰️ [مركز البيانات] خطأ WebSocket: {str(error)[:120]}")

    def _on_close(self, ws, code, reason) -> None:
        logger.info(f"🛰️ [مركز البيانات] أُغلق الاتصال (code={code}) — سيعاد الاتصال تلقائيًا")

    # ---------------- التعبئة العميقة (Backfill) ----------------
    def _scan_backfill_needs(self) -> None:
        now = time.time()
        with self._lock:
            for key in list(self._desired):
                st = self._backfill_state.setdefault(key, {'status': 'pending', 'attempts': 0, 'next_try': 0.0})
                if st.get('status') == 'done' and self._closed.get(key):
                    iv_min = _INTERVAL_MINUTES.get(key[1], 15)
                    # فحص الفجوة: مخزون كان مكتملًا وتوقّف تدفقه (انقطاع طويل) → إعادة تعبئة
                    if now - (max(self._closed[key]) / 1000.0) > 3 * iv_min * 60:
                        st['status'] = 'pending'; st['next_try'] = 0.0
                elif st.get('status') == 'failed' and now >= st.get('next_try', 0.0):
                    st['status'] = 'pending'

    def _backfill_loop(self) -> None:
        while not self._stop:
            key = None
            with self._lock:
                now = time.time()
                for k, st in self._backfill_state.items():
                    if st.get('status') != 'pending' or now < st.get('next_try', 0.0):
                        continue
                    # اكتمل المخزون عبر WebSocket أصلًا؟ علّمه منجزًا دون REST
                    have = len(self._closed.get(k) or {}) + (1 if self._forming.get(k) else 0)
                    if have >= self._target_count.get(k[1], 10**9):
                        st['status'] = 'done'; continue
                    st['status'] = 'running'; key = k; break
            if key is None:
                time.sleep(max(1.0, STREAM_HUB_BACKFILL_DELAY_SEC)); continue
            try:
                self._do_backfill(key)
                with self._lock:
                    st = self._backfill_state.get(key)
                    if st is not None:
                        st['status'] = 'done'
                        st['attempts'] = st.get('attempts', 0) + 1
                with self._lock:
                    cnt = len(self._closed.get(key) or {})
                logger.info(f"🛰️ [مركز البيانات] اكتملت تعبئة {key[0]} {key[1]} ({cnt} شمعة مكتملة)")
            except Exception as e:
                with self._lock:
                    st = self._backfill_state.get(key)
                    if st is not None:
                        st['attempts'] = st.get('attempts', 0) + 1
                        if st['attempts'] >= 5:
                            st['status'] = 'failed'; st['next_try'] = time.time() + 600
                        else:
                            st['status'] = 'pending'; st['next_try'] = time.time() + 60 * st['attempts']
                logger.warning(f"🛰️ [مركز البيانات] فشلت تعبئة {key[0]} {key[1]}: {str(e)[:120]}")
            time.sleep(max(0.5, STREAM_HUB_BACKFILL_DELAY_SEC))

    def _do_backfill(self, key: Tuple[str, str]) -> None:
        sym, iv = key
        lookback = self._backfill_lookback.get(iv)
        if not lookback:
            raise ValueError(f"لا lookback معرّف للفريم {iv}")
        klines = None
        if data_feed is not None:
            # [V9.20.0] تعبئة المركز من المزودين البديلين — باينانس احتياط أخير عبر الحارس
            try:
                klines = data_feed.get_klines(sym, iv, limit=lookback_to_candles(lookback, iv))
            except Exception:
                klines = None
        if not klines:
            # safe_get_klines يمر عبر الحارس (يحترم الحظر والميزانية) — نفس مسار المسح القديم
            klines = safe_get_klines(sym, iv, lookback)
        if not klines:
            raise RuntimeError('REST أعاد بيانات فارغة')
        merged: Dict[int, tuple] = {}
        for k in klines:
            try:
                tup = (int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]),
                       float(k[5]), float(k[7]), float(k[9]))
                merged[tup[0]] = tup
            except Exception:
                continue
        if not merged:
            raise RuntimeError('REST أعاد صفوفًا غير قابلة للتحليل')
        with self._lock:
            buf = self._closed.setdefault(key, {})
            buf.update(merged)
            mx = self._maxlen.get(iv, STREAM_HUB_BUFFER_DEFAULT)
            if len(buf) > mx:
                for ot in sorted(buf)[:len(buf) - mx]:
                    buf.pop(ot, None)

    def _maintain_loop(self) -> None:
        # صيانة دورية: مزامنة الاشتراكات مع (القائمة الديناميكية + المثبتة) وفحص فجوات التعبئة
        while not self._stop:
            try:
                self.set_universe(self._current_universe())
            except Exception:
                pass
            try:
                self._scan_backfill_needs()
            except Exception:
                pass
            time.sleep(max(20, STREAM_HUB_MAINTAIN_SEC))

    # ---------------- واجهات القراءة (بلا شبكة) ----------------
    def _snapshot_rows(self, key: Tuple[str, str]) -> Optional[List[tuple]]:
        now = time.time()
        with self._lock:
            if self._last_msg_ts <= 0 or (now - self._last_msg_ts) > STREAM_HUB_STALE_SEC:
                return None  # بيانات جامدة/لا رسائل — دع REST يتكفل
            buf = self._closed.get(key)
            forming = self._forming.get(key)
            rows = sorted(buf.values()) if buf else []
        if forming and (not rows or forming[0] > rows[-1][0]):
            rows = rows + [forming]  # الشمعة الجارية آخر صف — مطابق لسلوك REST تمامًا
        return rows or None

    def build_dataframe(self, symbol: str, interval: str, days: int) -> Optional[pd.DataFrame]:
        """يبني DataFrame مطابقًا بايتًا لمسار fetch_historical_data REST عند اكتمال المخزون، وإلا None."""
        iv = str(interval).lower()
        key = (str(symbol).upper(), iv)
        iv_min = _INTERVAL_MINUTES.get(iv)
        target = self._target_count.get(iv)
        if iv_min is None or target is None:
            return None
        needed = max(target, ((int(days) * 24 + 200) * 60) // iv_min)
        rows = self._snapshot_rows(key)
        if not rows or len(rows) < needed:
            return None
        df = pd.DataFrame(rows, columns=self._KLINE_COLUMNS)
        df = df.astype({'open': 'float', 'high': 'float', 'low': 'float', 'close': 'float',
                        'volume': 'float', 'quote_volume': 'float', 'taker_buy_base': 'float'})
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        df.set_index('timestamp', inplace=True)
        self._served_frames += 1
        return df.dropna()

    def get_closes(self, symbol: str, interval: str, n: int) -> Optional[List[float]]:
        """آخر n سعر إغلاق (شامل الشمعة الجارية كما يفعل REST) — None إن لم تكتمل الكمية."""
        rows = self._snapshot_rows((str(symbol).upper(), str(interval).lower()))
        if not rows or len(rows) < int(n):
            return None
        return [r[4] for r in rows[-int(n):]]

    def get_prices(self, symbols: List[str]) -> Dict[str, float]:
        """أسعار حية من الشمعة الجارية (تحديث كل ~2ث بلا أي وزن REST)."""
        out: Dict[str, float] = {}
        now = time.time()
        with self._lock:
            if self._last_msg_ts <= 0 or (now - self._last_msg_ts) > STREAM_HUB_STALE_SEC:
                return out
            for s in symbols or []:
                su = str(s).upper()
                f = self._forming.get((su, '15m'))
                if f:
                    out[s] = f[4]
                    continue
                buf = self._closed.get((su, '15m'))
                if buf:
                    out[s] = buf[max(buf)][4]
        return out

    def mark_served(self, symbol: str, interval: str) -> None:
        pass  # العدّاد يُحدَّث في build_dataframe مباشرة

    def note_rest_fallback(self) -> None:
        self._rest_fallbacks = getattr(self, '_rest_fallbacks', 0) + 1

    def snapshot(self) -> Dict[str, Any]:
        now = time.time()
        with self._lock:
            warm = 0; total = 0; prices_fresh = 0
            for key in self._desired:
                total += 1
                buf = self._closed.get(key) or {}
                forming = self._forming.get(key)
                have = len(buf) + (1 if forming else 0)
                if have >= self._target_count.get(key[1], 10**9):
                    warm += 1
            for key in list(self._forming.keys()):
                prices_fresh += 1
            pending = sum(1 for st in self._backfill_state.values() if st.get('status') == 'pending')
            failed = sum(1 for st in self._backfill_state.values() if st.get('status') == 'failed')
            return {'enabled': True, 'connected': self._connected,
                    'subscribed': len(self._subscribed), 'desired': len(self._desired),
                    'buffers_warm': warm, 'buffers_total': total,
                    'last_msg_age_sec': round(now - self._last_msg_ts, 1) if self._last_msg_ts else None,
                    'reconnects': self._reconnects, 'served_frames': self._served_frames,
                    'rest_fallbacks': getattr(self, '_rest_fallbacks', 0),
                    'backfill_pending': pending, 'backfill_failed': failed,
                    'prices_fresh': prices_fresh}

# --- [V9.16.0] التمثيل الواحد للمركز — يُعطَّل كليًا بـ USE_STREAM_HUB=False أو غياب websocket-client
stream_hub: Optional[MarketStreamHub] = None
if USE_STREAM_HUB:
    if _WEBSOCKET_AVAILABLE:
        try:
            stream_hub = MarketStreamHub()
            logger.info("🛰️ [مركز البيانات] MarketStreamHub جاهز (WebSocket — يعمل حتى أثناء حظر REST)")
        except Exception as hub_init_err:
            logger.warning(f"🛰️ [مركز البيانات] فشل التهيئة — سن عمل بـ REST فقط: {hub_init_err}")
            stream_hub = None
    else:
        logger.warning("🛰️ [مركز البيانات] مكتبة websocket-client غير مثبتة — سن عمل بـ REST فقط (ثبّتها: pip install websocket-client)")

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
    "Leader Behavior Veto": "سلوك القائد معاكس: القائد هابط والعملة تابعة له",
    "Market Sanity Filter Failed": "بوابة العقلانية: سوق ميت أو فوضوي بدرجة قصوى",
    "Strategy Prefilter Failed": "الفلتر الخاص بالاستراتيجية رفض الدخول (ملف فلترة منطقي لكل استراتيجية)",
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
    if _evidence_replaying:
        return  # [V9.23.0] صمت أثناء إعادة تشغيل الدليل — لا ضجيج ولا منافسة على الكاشات
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
    # [V9.20.0] تكه 24 ساعة من المزودين البديلين (طلب مجاني واحد) — باينانس (وزن 80) احتياطًا
    all_tickers = data_feed.get_24h_tickers() if data_feed is not None else None
    if not all_tickers:
        all_tickers = safe_get_24h_stats()
    for t in all_tickers:
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
            # [V9.25.0] التغير المُوقّع يُحفظ أيضًا — الترشيح لكل استراتيجية يحتاج اتجاه
            # الحركة (هابط للارتداد / صاعد للاختراق) لا مقدارها المطلق فقط
            chg_signed = float(t.get('priceChangePercent') or 0)
            chg = abs(chg_signed)
        except (TypeError, ValueError):
            continue
        if last <= 0 or high <= 0 or low <= 0 or high < low:
            continue
        # [V9.25.0] أرضية الدائرة الواسعة أخف من العميقة — القائمة العميقة تُقتطع لاحقًا
        # بأرضية السيولة الكاملة، والدائرة الواسعة تحتفظ بمخزون أوسع للترشيح
        if qvol < min(DYNAMIC_UNIVERSE_MIN_QUOTE_VOLUME, WIDE_UNIVERSE_MIN_QUOTE_VOLUME):
            continue
        range_pct = (high - low) / low * 100.0
        if range_pct < DYNAMIC_UNIVERSE_MIN_RANGE_PCT:
            continue
        # [V9.25.0] موقع السعر داخل مدى 24 ساعة: 0 = عند القاع تمامًا، 1 = عند القمة
        pos_24h = (last - low) / (high - low)
        rows.append({'symbol': sym, 'qvol': qvol, 'range_pct': range_pct, 'chg': chg,
                     'chg_signed': chg_signed, 'pos_24h': pos_24h,
                     'last': last, 'high': high, 'low': low})
    if not rows:
        return [], {'reason': 'لا مرشحين بعد الفلترة (سيولة/تقلب)'}
    def _ranks(key: str) -> Dict[int, float]:
        order = sorted(rows, key=lambda r: r[key], reverse=True)
        n = max(1, len(order) - 1)
        return {id(r): (n - i) / n for i, r in enumerate(order)}

    def _ranks_on(pool: List[Dict[str, Any]], key: str) -> Dict[int, float]:
        """[V9.25.0] رتب مئوية داخل مجموعة فرعية (الدائرة الواسعة) بدل الكون الكامل."""
        order = sorted(pool, key=lambda r: r[key], reverse=True)
        n = max(1, len(order) - 1)
        return {id(r): (n - i) / n for i, r in enumerate(order)}
    vol_r, rng_r, chg_r = _ranks('qvol'), _ranks('range_pct'), _ranks('chg')
    for r in rows:
        r['score'] = round(100 * (0.45 * vol_r[id(r)] + 0.35 * rng_r[id(r)] + 0.20 * chg_r[id(r)]), 2)
    rows.sort(key=lambda r: r['score'], reverse=True)
    # [V9.25.0] القائمة العميقة بأرضية السيولة الكاملة (سلوك V9.10 الأصلي محفوظ حرفيًا)
    # والدائرة الواسعة تضم ما تبقى بأرضيتها الأخف — التوسع بلا تضحية بجودة العمق
    deep_rows = [r for r in rows if r['qvol'] >= DYNAMIC_UNIVERSE_MIN_QUOTE_VOLUME]
    top = deep_rows[:size]
    picked = [r['symbol'] for r in top]
    # [V9.25.0] الدائرة الواسعة: نفس الترتيب الحيوي حتى WIDE_UNIVERSE_SIZE — مخزون
    # الترشيح لكل استراتيجية (رتبة السيولة تُحسب داخل الدائرة الواسعة نفسها لعدل المقارنة)
    wide = rows[:max(size, WIDE_UNIVERSE_SIZE)]
    _wide_vol_r = _ranks_on(wide, 'qvol')
    wide_rows: Dict[str, Dict[str, float]] = {}
    for r in wide:
        wide_rows[r['symbol']] = {'last': r['last'], 'high': r['high'], 'low': r['low'],
                                  'chg_signed': r['chg_signed'], 'range_pct': r['range_pct'],
                                  'qvol': r['qvol'], 'pos_24h': r['pos_24h'],
                                  'qvol_rank': round(_wide_vol_r[id(r)] * 100.0, 1)}
    meta = {
        'candidates': len(rows),
        'wide_size': len(wide_rows),
        'wide_rows': wide_rows,
        'top_preview': [{'symbol': r['symbol'], 'score': r['score'],
                         'qvol_musd': round(r['qvol'] / 1e6, 1),
                         'range_pct': round(r['range_pct'], 2),
                         'chg24h': round(r['chg'], 2)} for r in top[:8]],
    }
    logger.info(f"⚡ [القائمة الديناميكية] رُشِّح {len(rows)} عملة حيوية — عميق {len(picked)} + دائرة واسعة {len(wide_rows)} (V9.25.0): {picked}")
    try:
        logger.info("⚡ [الأعلى حيوية] " + " | ".join(
            f"{p['symbol']} (نقاط {p['score']}, سيولة {p['qvol_musd']}M, مدى {p['range_pct']}%, تغير {p['chg24h']}%)"
            for p in meta['top_preview'][:5]))
    except Exception:
        pass
    return picked, meta

def refresh_universe_if_needed(force: bool = False) -> None:
    """يحدّث قائمة المسح للعملات الأكثر حيوية كل DYNAMIC_UNIVERSE_REFRESH_MIN دقيقة.
    عند أي فشل: تبقى القائمة الحالية كما هي، وإن لم توجد أصلًا يُستخدم البديل الثابت.
    [V9.25.0] يخزّن أيضًا الدائرة الواسعة (WIDE_UNIVERSE_SIZE) ويعيد بناء ترشيح
    كل استراتيجية (10 عملات × عدد الاستراتيجيات) من نفس التكه المجاني."""
    global validated_symbols_to_scan, universe_last_refresh, universe_source, universe_meta
    global wide_universe_rows
    if not USE_DYNAMIC_UNIVERSE:
        return
    with universe_lock:
        if not force and time.time() - universe_last_refresh < DYNAMIC_UNIVERSE_REFRESH_MIN * 60:
            return
    reason = ''
    try:
        picked, meta = compute_dynamic_universe()
        if picked:
            wide_rows = meta.pop('wide_rows', {}) or {}
            with universe_lock:
                validated_symbols_to_scan = picked
                universe_last_refresh = time.time()
                universe_source = 'dynamic'
                universe_meta = meta
                wide_universe_rows = wide_rows
            # [V9.25.0] إعادة بناء ترشيح الاستراتيجيات من الدائرة الواسعة الجديدة
            try:
                nominate_strategy_candidates(force=True)
            except Exception as nom_err:
                logger.warning(f"⚠️ [دائرة الترشيح] فشل إعادة البناء: {nom_err}")
            # [V9.16.0 + V9.25.0] مزامنة اشتراكات WebSocket مع اتحاد (العميق + المرشحين)
            if stream_hub is not None:
                try:
                    with nominees_lock:
                        union = list(validated_symbols_to_scan) + [n['symbol'] for lst in STRATEGY_NOMINEES.values() for n in lst]
                    stream_hub.set_universe(sorted(set(union)))
                except Exception:
                    pass
            return
        reason = meta.get('reason', 'غير معروف') if isinstance(meta, dict) else str(meta)
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
    # [V9.16.0] مزامنة اشتراكات WebSocket حتى في وضع البديل الثابت
    if stream_hub is not None:
        try:
            with universe_lock:
                stream_hub.set_universe(list(validated_symbols_to_scan))
        except Exception:
            pass


# ============================================================
# [V9.25.0] الترشيح الحتمي لكل استراتيجية: 10 عملات للفحص × عدد الاستراتيجيات
# طلب المستخدم الحرفي: "قم بتوسيع دائرة العملات التي تفحص بحيث ترشح 10 عملات
# للفحص مناسبة لكل استراتيجية اي العدد الكلي 10×عدد الاستراتيجيات"
# المنهج (حتمي لا تكهن): لكل استراتيجية بصمة ظروف رقمية من مقاييس التكه المجاني
# (موقع السعر في مدى 24س + اتجاه الحركة + ضيق المدى + رتبة السيولة داخل الدائرة)،
# بها تُرتب كل عملات الدائرة الواسعة (120) وتُختار الأعلى عشر لتفحصها هي فقط:
#   BB_STOCH (الارتداد):     عند القاع + هابط اليوم (بلا سكين متطرفة) + مدى يسمح بالارتداد
#   BB_SQUEEZE (الانضغاط):   مدى 24س ضيق + سعر في منتصف النطاق (زنبرك ملفوف)
#   SR_BREAKOUT:             عند القمة + صاعد اليوم (يختبر مقاومة محددة)
#   MACD_EMA / EMA_RSI:      صاعدة اليوم + في النصف العلوي من النطاق (هيكل صاعد)
#   BULLISH_MOMENTUM:        زخم صاعد قوي + قرب القمة (استمرارية)
#   PULLBACK (التراجع):      صاعدة على 24س + تراجعت لمنتصف النطاق (شراء الغور في الصاعد)
# ============================================================

def _clamp_0_100(v: float, lo: float, hi: float) -> float:
    """تحويل v من المدى [lo, hi] إلى نسبة 0-100 مقيدة (أداة البصمة الرقمية)."""
    if hi <= lo:
        return 0.0
    return max(0.0, min(100.0, (float(v) - lo) / (hi - lo) * 100.0))


def _nominee_score_for(key: str, m: Dict[str, float]) -> Tuple[float, str]:
    """درجة قرب الرمز (0-100) من ظروف الاستراتيجية + سبب عربي موثق.
    المقاييس كلها من تكه 24 ساعة المجاني — نفس الأرقام تعطي نفس الترشيح (حتمية)."""
    pos = float(m.get('pos_24h', 0.5))
    chg = float(m.get('chg_signed', 0.0))
    rng = float(m.get('range_pct', 0.0))
    liq = float(m.get('qvol_rank', 50.0))
    if key == 'BB_STOCH':
        bottom = (1.0 - pos) * 100.0
        fell = _clamp_0_100(-chg, 0.0, 15.0)      # هبوط اليوم = وقود الارتداد (سقف 15% لاستبعاد السكاكين)
        room = _clamp_0_100(rng, 0.0, 10.0)        # مدى كافٍ للارتداد داخل اليوم
        score = 0.40 * bottom + 0.25 * fell + 0.20 * liq + 0.15 * room
        why = f"قرب قاع 24س ({bottom:.0f}% من المسافة إليه) بتغير {chg:+.1f}% — قاع صيد الارتداد"
    elif key == 'BB_SQUEEZE':
        tight = 100.0 - _clamp_0_100(rng, 0.0, 8.0)
        mid = max(0.0, (1.0 - abs(pos - 0.5) * 2.0)) * 100.0
        score = 0.45 * tight + 0.30 * mid + 0.25 * liq
        # [V9.25.0] صك حي: يوم متحرك بالفعل (±10%) ليس انضغاطًا — الانضغاط حبس قبل الانفجار
        if abs(chg) > 10.0:
            score -= 15.0
        why = f"مدى 24س {rng:.1f}% (انضغاط) والسعر في منتصف النطاق — زنبرك ملفوف"
    elif key == 'SR_BREAKOUT':
        near_high = pos * 100.0
        rising = _clamp_0_100(chg, 0.0, 10.0)
        # [V9.25.0] صك حي: اختبار المقاومة = السعر مضغوط على القمة — الموقع يتقدم على الزخم
        # (درس حي: عملة +30% في منتصف النطاق ليست مرشح اختراق مقاومة)
        score = 0.55 * near_high + 0.20 * rising + 0.25 * liq
        why = f"عند {near_high:.0f}% من قمة 24س بتغير {chg:+.1f}% — يختبر مقاومة"
    elif key in ('MACD_EMA', 'EMA_RSI'):
        rising = _clamp_0_100(chg, 0.0, 10.0)
        upper = pos * 100.0
        score = 0.40 * rising + 0.35 * upper + 0.25 * liq
        why = f"صاعدة {chg:+.1f}% على 24س عند {upper:.0f}% من النطاق — هيكل صاعد"
    elif key == 'BULLISH_MOMENTUM':
        rising = _clamp_0_100(chg, 0.0, 12.0)
        upper = pos * 100.0
        score = 0.50 * rising + 0.25 * upper + 0.25 * liq
        why = f"زخم صاعد {chg:+.1f}% على 24س عند {upper:.0f}% من النطاق — استمرارية"
    elif key == 'PULLBACK':
        rising = _clamp_0_100(chg, 0.0, 8.0)
        retr = max(0.0, (1.0 - abs(pos - 0.45) / 0.55)) * 100.0
        score = 0.35 * rising + 0.40 * retr + 0.25 * liq
        why = f"صاعدة على 24س ({chg:+.1f}%) مع تراجع لمنتصف النطاق — شراء التراجع في الصاعد"
    else:
        # استراتيجية مستقبلية بلا بصمة خاصة: حيوية عامة + سيولة
        score = 0.50 * liq + 0.30 * _clamp_0_100(abs(chg), 0.0, 10.0) + 0.20 * _clamp_0_100(rng, 1.5, 12.0)
        why = 'ترشيح عام (بلا بصمة خاصة لهذه الاستراتيجية بعد)'
    return round(float(score), 2), why


def nominate_strategy_candidates(force: bool = False) -> Dict[str, List[Dict[str, Any]]]:
    """يبني ترشيح الفحص لكل استراتيجية: أفضل NOMINEES_PER_STRATEGY (10) عملة من
    الدائرة الواسعة — حتمي (نفس المقاييس ⇒ نفس الترشيح) ويستبعد الصفقات المفتوحة
    مع التزويد تلقائيًا من بقية الترتيب لتبقى القائمة 10 كلما أمكن.
    يعاد البناء عند كل تحديث للقائمة الديناميكية (كل 30د) أو قسرًا؛ بينهما تُستخدم
    القائمة نفسها وتُحدّث استثناءاتها فقط — فتظل دائرة الفحص مستقرة داخل النافذة."""
    global STRATEGY_NOMINEES, _NOMINEE_RANKED_POOL
    global strategy_nominees_built_at, strategy_nominees_updated_at
    with universe_lock:
        rows_snapshot = dict(wide_universe_rows)
        built_marker = universe_last_refresh
    if not rows_snapshot:
        try:
            refresh_universe_if_needed(force=True)
            with universe_lock:
                rows_snapshot = dict(wide_universe_rows)
                built_marker = universe_last_refresh
        except Exception as uni_err:
            logger.warning(f"⚠️ [دائرة الترشيح] تعذر بناء الدائرة الواسعة: {uni_err}")
    if not rows_snapshot:
        return {}
    with nominees_lock:
        if (not force and _NOMINEE_RANKED_POOL and strategy_nominees_built_at == built_marker):
            pass  # الدائرة لم تتغير — القوائم المبنية تصلح
        else:
            ranked: Dict[str, List[Dict[str, Any]]] = {}
            for sym, m in rows_snapshot.items():
                for key in STRATEGY_SETUP_SCANNERS:
                    sc, why = _nominee_score_for(key, m)
                    ranked.setdefault(key, []).append({
                        'symbol': sym, 'score': sc, 'why_ar': why,
                        'pos_24h': round(float(m.get('pos_24h', 0.0)), 3),
                        'chg_signed': round(float(m.get('chg_signed', 0.0)), 2),
                        'range_pct': round(float(m.get('range_pct', 0.0)), 2),
                        'qvol_rank': round(float(m.get('qvol_rank', 50.0)), 1)})
            for key, lst in ranked.items():
                # حتمية كاملة: الدرجة تنازليًا ثم الرمز أبجديًا (كسر تعادل مستقر)
                lst.sort(key=lambda x: (-x['score'], x['symbol']))
            _NOMINEE_RANKED_POOL = ranked
            strategy_nominees_built_at = built_marker
            strategy_nominees_updated_at = datetime.now(timezone.utc).isoformat()
            logger.info(f"🎯 [دائرة الترشيح] بُني ترتيب الترشيح لـ {len(ranked)} استراتيجية من {len(rows_snapshot)} عملة (10 لكل استراتيجية = 10×{len(ranked)})")
    try:
        with signal_cache_lock:
            open_syms = {str(s).upper() for s in open_signals_cache.keys()}
    except Exception:
        open_syms = set()
    final: Dict[str, List[Dict[str, Any]]] = {}
    with nominees_lock:
        for key, pool in _NOMINEE_RANKED_POOL.items():
            final[key] = [dict(n) for n in pool
                          if str(n['symbol']).upper() not in open_syms][:NOMINEES_PER_STRATEGY]
        STRATEGY_NOMINEES = final
        return {k: list(v) for k, v in STRATEGY_NOMINEES.items()}


# --- دوال جلب البيانات وحساب المؤشرات ---
def fetch_historical_data(symbol: str, interval: str, days: int) -> Optional[pd.DataFrame]:
    if not client: return None
    # [V9.16.0] المحاولة الأولى: مركز بيانات WebSocket — صفر وزن REST عند اكتمال المخزون.
    # أي نقص/تعطل = None → مسار REST القديم يعمل كما هو (تدهور رشيق دائمًا).
    if stream_hub is not None:
        try:
            df_hub = stream_hub.build_dataframe(symbol, interval, days)
            if df_hub is not None:
                return df_hub
            stream_hub.note_rest_fallback()
        except Exception as _hub_err:
            logger.debug(f"[مركز البيانات] تجاوز إلى REST لـ {symbol} {interval}: {_hub_err}")
    try:
        lookback_str = f"{days + 50} day" if 'd' in interval.lower() else f"{days * 24 + 200} hour"
        
        klines = None
        if data_feed is not None:
            # [V9.20.0] الشموع التاريخية للاستراتيجيات من المزودين البديلين أولًا
            try:
                if 'd' in interval.lower():
                    n_candles = days + 50
                else:
                    # [V9.22.0] العمق الحقيقي بالشموع: كان يُطلب 920 شمعة فقط لفريم 15م
                    # (الخلط بين الساعات والشموع) بينما العمق المصمم ~3680 = 30 يومًا —
                    # مع الترقيم الرجعي في BybitProvider استُعيد العمق الكامل بمطابقة المركز
                    iv_min = _INTERVAL_MINUTES.get(str(interval).lower(), 15)
                    n_candles = ((days * 24 + 200) * 60) // iv_min
                klines = data_feed.get_klines(symbol, interval, limit=max(50, min(5000, n_candles)))
            except Exception:
                klines = None
        if not klines and (rate_guard.banned_until - time.time()) <= 0:
            # [V9.21.0] احتياط REST فقط حين لا حظر — أثناء الحظر يمنع تعلّق خيط المسح
            # في acquire() لساعات: يُعيد None فيُتخطى الرمز وتواصل الدورة عملها
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

def load_open_signals_to_cache(retries: int = 4, delay: int = 10):
    # [إصلاح V9.12.1] كانت تعود صامتة إذا كانت قاعدة البيانات باردة عند الإقلاع
    # (Neon/Supabase المجانية تستيقظ ببطء) فيبقى كاش الصفقات فارغًا حتى إعادة التشغيل —
    # والآن تعيد المحاولة عدة مرات قبل الاستسلام.
    for attempt in range(1, retries + 1):
        if not check_db_connection() or not conn:
            logger.warning(f"[تحميل] قاعدة البيانات غير جاهزة (محاولة {attempt}/{retries})...")
            time.sleep(delay); continue
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM signals WHERE status IN ('open', 'updated');")
                open_signals = cur.fetchall()
                with signal_cache_lock:
                    open_signals_cache.clear()
                    for signal in open_signals: open_signals_cache[signal['symbol']] = dict(signal)
                logger.info(f"✅ [تحميل] تم تحميل {len(open_signals)} صفقة مفتوحة إلى الذاكرة المؤقتة.")
                return
        except Exception as e:
            logger.error(f"❌ [تحميل] فشل تحميل الصفقات المفتوحة (محاولة {attempt}/{retries}): {e}")
            if attempt < retries: time.sleep(delay)

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

# --- [إعادة تصميم V9.14.0] بوابة عقلانية عامة + فلتر خاص لكل استراتيجية ---
def _count_strategy_filter_reject(strategy_name: str, filter_label: str):
    """محاسبة رفضات الفلتر الخاص بكل استراتيجية (لتحليل لوحة التحكم)."""
    if _evidence_replaying:
        return  # [V9.23.0] لا محاسبة أثناء إعادة تشغيل الدليل
    with _scan_stats_lock:
        _strategy_filter_stats[strategy_name][filter_label] += 1

def passes_market_sanity_filter(df: pd.DataFrame) -> bool:
    """[V9.14.0] بوابة العقلانية العامة — واسعة جدًا وبلا أي حساسية اتجاهية.
    تحل محل فلتري "تقلب السوق" و"قوة الاتجاه" الشاملين اللذين كانا يخنقان التوصيات.
    تمنع فقط الحالات القصوى غير القابلة للتداول: السوق الميت (ATR% < 0.10)
    والفوضى العارمة (ATR% > 8.0). كل أنماط السوق المشروعة (اتجاه/نطاق/انضغاط/
    انفجار) تمر منها إلى استراتيجياتها، وكل فلترة أدق صارت ملكًا للاستراتيجية نفسها."""
    if len(df) < 50:
        return False
    last = df.iloc[-1]
    # التأكد من وجود الأعمدة المطلوبة
    if 'atr' not in last or 'close' not in last or last['close'] == 0:
        return False  # لا يمكن الحساب، نفترض أنه غير صالح
    atr_percent = (float(last['atr']) / float(last['close'])) * 100
    if atr_percent < 0.10 or atr_percent > 8.0:
        log_rejection(getattr(df, 'name', 'UNKNOWN'), "Market Sanity Filter Failed", {"atr_percent": f"{atr_percent:.2f}"})
        return False
    return True

def passes_strategy_prefilters(df: pd.DataFrame, strategy_name: str) -> bool:
    """[V9.15.0] الفلتر الخاص بكل استراتيجية حسب ملفها المعياري الاحترافي.
    الفلسفة: الفلتر يخدم نمط الاستراتيجية ولا يحاربه — بقيم الكلاسيكيات:
    - التقاطعات (Wilder): ADX≥20 — بلا تقاطعات في غياب الاتجاه.
    - Pullback (Raschke): ADX≥25 — الارتداد في اتجاه مثبت فقط.
    - الزخم: اتجاه قوي + تقلب حي + دفعة ROC≥1%.
    - الارتدادية (Connors): شراء التراجعات فوق EMA50 فقط — بلا سقف ADX
      (شراء الغرقى في الاتجاه الصاعد القوي أفضل صفقات الانعكاس).
    - الاختراقية (Carter/TTM): الانضغاط+الاختراق+الحجم هو الفلتر ذاته —
      صالح في النطاق وفي استمرار الاتجاه، فلا قيود ADX ولا أرضية تقلب.
    None = بلا حد لهذا البعد. كل رفض يُحاسب في _strategy_filter_stats للتحليل."""
    if len(df) < 50:
        return False
    profile = STRATEGY_FILTER_PROFILES.get(strategy_name) or DEFAULT_STRATEGY_FILTER_PROFILE
    last = df.iloc[-1]

    def _fail(label: str, details: Dict[str, Any]) -> bool:
        _count_strategy_filter_reject(strategy_name, label)
        log_rejection(getattr(df, 'name', 'UNKNOWN'), "Strategy Prefilter Failed",
                      {'strategy': strategy_name, 'filter': label, **details})
        return False

    # 1) حدود ADX (قوة الاتجاه): حد أدنى للاتجاهية، سقف للارتدادية/الاختراقية
    adx_val = float(last['adx']) if ('adx' in last and pd.notna(last['adx'])) else None
    if adx_val is not None:
        min_adx, max_adx = profile.get('min_adx'), profile.get('max_adx')
        if min_adx is not None and adx_val < min_adx:
            return _fail(f"ADX أدنى من {min_adx:g}", {'adx': f"{adx_val:.2f}"})
        if max_adx is not None and adx_val > max_adx:
            return _fail(f"ADX أعلى من {max_adx:g}", {'adx': f"{adx_val:.2f}"})

    # 2) حدود التقلب النسبي ATR%: لكل نمط نطاقه المناسب (الاختراقية بلا حد أدنى)
    if 'atr' in last and 'close' in last and last['close']:
        atr_percent = (float(last['atr']) / float(last['close'])) * 100
        min_atr, max_atr = profile.get('min_atr_pct'), profile.get('max_atr_pct')
        if min_atr is not None and atr_percent < min_atr:
            return _fail(f"تقلب أدنى من {min_atr:g}%", {'atr_percent': f"{atr_percent:.2f}"})
        if max_atr is not None and atr_percent > max_atr:
            return _fail(f"تقلب أعلى من {max_atr:g}%", {'atr_percent': f"{atr_percent:.2f}"})

    # 3) حد أدنى للزخم المطلق ROC: فقط لمن يحتاجه في ملفه (الزخمية)
    roc_key = f'roc_{MOMENTUM_PERIOD}'
    min_roc = profile.get('min_roc')
    if min_roc is not None and roc_key in last and pd.notna(last[roc_key]):
        roc_abs = abs(float(last[roc_key]))
        if roc_abs < min_roc:
            return _fail(f"زخم ROC أدنى من {min_roc:g}%", {roc_key: f"{roc_abs:.2f}"})

    # 4) [V9.17.0] معيار Connors المرن لصفقات شراء التراجعات: السعر قرب EMA50 أو فوقها
    # (سماحية ATR واحدة تحت المتوسط) — الصرامة المطلقة رفضت 431 محاولة بلا نجاح واحد،
    # لأن شراء الغرقى المقصود في الاستراتيجية يقع شرعًا تحت المتوسط بهامش ضئيل.
    # الغرقى الحقيقي (أبعد من ATR واحد) يبقى مرفوضًا — بل يمنعه الآن نقض الريم أيضًا
    tol_atr = profile.get('ema50_tol_atr')
    if tol_atr is not None and 'ema_50' in last and 'atr' in last and pd.notna(last['ema_50']) and last['close'] and pd.notna(last['atr']) and float(last['atr']) > 0:
        floor_price = float(last['ema_50']) - tol_atr * float(last['atr'])
        if float(last['close']) < floor_price:
            return _fail(f"السعر أبعد من EMA50 بأكثر من {tol_atr:g}×ATR (Connors المرن)",
                         {'close': f"{float(last['close']):.6g}", 'ema_50': f"{float(last['ema_50']):.6g}", 'floor': f"{floor_price:.6g}"})

    return True


# --- [V9.17.0] محرك مطابقة الأزواج: تصنيف النمط + درجة التوافق ---
def compute_symbol_regime(df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """[V9.17.0] ملف النمط السوقي للزوج من شموع 15م المجلوبة أصلًا في حلقة المسح
    (صفر وزن شبكي إضافي). يصنف الزوج إلى: اتجاه صاعد/هابط، نطاق مترنم، انضغاط
    سعري، أو انتقالي — أساس ترشيح الأزواج لكل استراتيجية بدل العشوائي والسيولة.
    المكونات: كفاءة كوفمان ER (اتجاهية الحركة)، انقلابات الجهة حول EMA50 (ترنم
    النطاق)، مرتبة عرض بولنجر (انضغاط)، ADX، ATR%، ROC، وهيكل EMA21/EMA50.
    يُرجع None عند تعذر الحساب — والبوابة تمرره بدل أن تخنق (درس V9.14)."""
    try:
        if df is None or len(df) < 100:
            return None
        last = df.iloc[-1]
        for c in ('close', 'ema_50', 'atr', 'adx', 'bb_width'):
            if c not in df.columns:
                return None
        adx = float(last['adx']) if pd.notna(last['adx']) else None
        atr = float(last['atr']) if pd.notna(last['atr']) else None
        close = float(last['close']) if pd.notna(last['close']) else None
        if not adx or not atr or not close or atr <= 0 or close <= 0:
            return None
        atr_pct = (atr / close) * 100.0
        # كفاءة كوفمان: صافي الحركة ÷ مجموع مقاطع الحركة (آخر 20 شمعة) — 0 فوضى، 1 سهم
        w = closes_w = df['close'].astype(float).iloc[-21:]
        net = abs(float(closes_w.iloc[-1]) - float(closes_w.iloc[0]))
        gross = float(closes_w.diff().abs().sum())
        er = (net / gross) if gross > 0 else 0.0
        # انقلابات الجهة حول EMA50 على آخر 100 شمعة: كثرة الانقلاب = ترنم نطاق
        side = np.sign(df['close'].astype(float) - df['ema_50'].astype(float)).iloc[-101:]
        flips = int((side.diff().fillna(0) != 0).sum())
        # مرتبة عرض بولنجر الحالي مقابل التاريخ الحديث مستبعدًا آخر 20 شمعة
        # (الاستبعاد يمنع أن "يسابق" الهدوء الحالي نفسه فيصبح مئينه متوسطًا زورًا)
        bbw_all = df['bb_width'].astype(float)
        cur_bbw = float(bbw_all.iloc[-1]) if pd.notna(bbw_all.iloc[-1]) else None
        bbw_hist = bbw_all.iloc[-220:-20]
        bbwp = float((bbw_hist <= cur_bbw).mean()) if (cur_bbw is not None and len(bbw_hist) > 20) else None
        ema50 = float(last['ema_50']) if pd.notna(last['ema_50']) else None
        ema21 = float(last['ema_21']) if ('ema_21' in last and pd.notna(last['ema_21'])) else None
        struct_up = bool(ema21 is not None and ema50 is not None and ema21 > ema50)
        ema50_dist_atr = ((close - ema50) / atr) if ema50 is not None else None
        roc_key = f'roc_{MOMENTUM_PERIOD}'
        roc = float(last[roc_key]) if (roc_key in last and pd.notna(last[roc_key])) else None
        # التصنيف: الانضغاط أولًا (أضيق بولنجر تاريخيًا + بلا اتجاه)، ثم النطاق
        # بكثرة الانقلابات حول EMA50 (ER قصير الأمد يخدع في النطاق الناعم الدوري)،
        # ثم الاتجاه بكفاءة + ADX، وأخيرًا الانتقالي لما لم يستقر
        if bbwp is not None and bbwp <= 0.20 and adx < 25:
            regime = 'squeeze'
        elif flips >= 5 or er <= 0.18:
            regime = 'range'
        elif er >= 0.30 and adx >= 18:
            regime = 'trend_up' if struct_up else 'trend_down'
        else:
            regime = 'transitional'
        return {'regime': regime, 'er': er, 'flips': flips, 'bbwp': bbwp, 'adx': adx,
                'atr_pct': atr_pct, 'roc': roc, 'struct_up': struct_up,
                'ema50_dist_atr': ema50_dist_atr}
    except Exception as reg_err:
        logger.warning(f"⚠️ [ريم الزوج] فشل الحساب: {reg_err}")
        return None


def score_strategy_pair_fit(ri: Optional[Dict[str, Any]], strategy_name: str) -> Optional[float]:
    """[V9.17.0] درجة مطابقة الزوج (0-100) لملف الاستراتيجية:
    40 نقطة مطابقة الريم + 25 لـ ADX + 20 للتقلب ATR% + 15 للبنية.
    نقض قاسٍ (0): ريم معاكس صراحةً (تقاطعات على نطاق، ارتدادية على هابط،
    اختراق على اتجاه قائم) أو زخم سالب للاستراتيجية الزخمية.
    None = تعذر التقييم — والمتصل يمرره بدل أن يرفض (لا اختناق جديد)."""
    if not ri:
        return None
    prof = STRATEGY_PAIR_PROFILES.get(strategy_name)
    if not prof:
        return None
    regime = ri.get('regime')
    adx = ri.get('adx')
    atr_pct = ri.get('atr_pct')
    # القواعد القاسية أولًا
    if prof.get('require_pos_roc') and (ri.get('roc') is None or ri['roc'] <= 0):
        return 0.0
    if regime in prof['regimes']:
        score = 40.0
    elif prof.get('secondary_regimes') and regime in prof['secondary_regimes']:
        score = 25.0  # ائتمان ثانوي: بيئة مقبولة لكن ليست جوهر الاستراتيجية
    elif regime == 'transitional':
        score = 15.0  # ائتمان جزئي: الريم لم يستقر بعد
    else:
        return 0.0  # ريم معاكس صراحةً — لا تُجبر استراتيجية على بيئة تكرهها
    # (ADX: 0-25) سلم صعود للتوجهية أو سقف متحلل للنطاقية
    mn, ideal, mx = prof.get('adx', (None, None, None))
    if adx is not None and (ideal is not None or mx is not None):
        if mx is not None:
            score += 25.0 if adx <= mx else max(0.0, 25.0 - (adx - mx) * 2.0)
        else:
            lo = (mn or 0.0) * 0.6
            score += 25.0 * min(1.0, max(0.0, (adx - lo) / max(ideal - lo, 1e-9)))
    # (التقلب النسبي: 0-20) داخل النطاق كاملة، وتتحلل خطيًا خارجه
    lo_a, hi_a = prof.get('atr_pct', (None, None))
    if atr_pct is not None:
        if lo_a is not None and atr_pct < lo_a:
            score += 20.0 * min(1.0, max(0.0, atr_pct / lo_a))
        elif hi_a is not None and atr_pct > hi_a:
            score += 20.0 * min(1.0, max(0.0, 1.0 - (atr_pct - hi_a) / hi_a))
        else:
            score += 20.0
    # (البنية: 0-15) بنونيهات حسب شخصية الاستراتيجية
    d = 0.0
    if prof.get('struct_up_bonus') and ri.get('struct_up'):
        d += 8.0
    if prof.get('pos_roc_bonus') and (ri.get('roc') or 0) > 0:
        d += 7.0
    if prof.get('low_bbwp_bonus') and ri.get('bbwp') is not None and ri['bbwp'] <= 0.20:
        d += 15.0
    if prof.get('range_bonus') and ri.get('flips') is not None:
        d += min(8.0, ri['flips'] * 2.0)
    if prof.get('near_ema50') and ri.get('ema50_dist_atr') is not None:
        if ri['ema50_dist_atr'] >= -1.0:
            d += 7.0
    score += min(15.0, d)
    return round(min(100.0, score), 1)


# --- [V9.24.0] كواشف التجهيز الشرطي لكل استراتيجية (Deterministic Setup Scanners) ---
# الفلسفة الجديدة بطلب المستخدم: لا توصية بلا "تجهيز" حتمي مكتشف على الرمز نفسه —
# مثال: استراتيجية الارتداد تُفحص فقط على عملات في قاع سعرها أعطت مؤشرات ارتداد فعلية.
# الكاشف يجيب سؤال "هل ظروف هذه الاستراتيجية متجسدة في هذا الرمز الآن؟" بأرقام صريحة،
# وتوثق الأدلة الرقمية في تفاصيل الإشارة (مسافة عن القاع، RSI، دوران الستوك، شمعة الارتداد).

def _market_regime_is_downtrend(market_regime: str) -> bool:
    return 'DOWNTREND' in str(market_regime or '').upper()


def detect_bottom_bounce_setup(df: pd.DataFrame, market_regime: str = '') -> Tuple[bool, Dict[str, Any]]:
    """[V9.24.0] كاشف القاع والارتداد الحتمي — قلب طلب المستخدم.
    شروط صارمة قابلة للتوثيق:
      (1) القاع: السعر ضمن bottom_dist×ATR من أدنى قاع آخر 96 شمعة (24 ساعة على 15م)
      (2) لمس بولنجر السفلي بذيل أو جسم
      (3) تشبع بيعي RSI: <35 عاديًا، <30 إلزاميًا في الهبوط الحاد (سكين أعمق)
      (4) ستوكاستك RSI في القاع (K<40) بدوران صاعد (K فوق قيمته السابقة أو فوق D)
      (5) شمعة ارتداد فعلية: خضراء أو ذيل سفلي ≥45% من مدى الشمعة
    في الهبوط الحاد تُشترط (1)+(3)+(4)+(5) كلها بلا تنازل، وفي العادي (1)+(3)+(4)+(2 أو 5).
    تعيد (قرار، أدلة رقمية للتوثيق في اللوحة)."""
    ev: Dict[str, Any] = {'setup': 'bottom_bounce'}
    if len(df) < max(110, BOTTOM_WINDOW_BARS + 10):
        ev['reason'] = 'شموع غير كافية'
        return False, ev
    last, prev = df.iloc[-1], df.iloc[-2]
    try:
        atr = float(last['atr']) if pd.notna(last['atr']) else 0.0
        close = float(last['close'])
    except Exception:
        return False, ev
    if not (atr > 0 and close > 0):
        ev['reason'] = 'ATR/سعر غير صالحين'
        return False, ev
    downtrend = _market_regime_is_downtrend(market_regime)
    dist_cap = BOTTOM_DIST_LOW_ATR_DOWNTREND if downtrend else BOTTOM_DIST_LOW_ATR
    rsi_cap = BOTTOM_RSI_MAX_DOWNTREND if downtrend else BOTTOM_RSI_MAX_RANGE
    # (1) القاع
    window = df.iloc[-BOTTOM_WINDOW_BARS:]
    low_win = float(window['low'].min())
    dist_low_atr = (close - low_win) / atr
    at_bottom = dist_low_atr <= dist_cap
    ev['dist_low_atr'] = round(dist_low_atr, 2)
    ev['low_window'] = low_win
    # (2) لمس بولنجر السفلي
    bb_lower = float(last['bb_lower']) if ('bb_lower' in last and pd.notna(last['bb_lower'])) else None
    touched_bb = bool(bb_lower is not None and float(last['low']) <= bb_lower * 1.002)
    if bb_lower is not None:
        ev['bb_lower'] = round(bb_lower, 6)
    ev['touched_bb'] = touched_bb
    # (3) تشبع بيعي
    rsi = float(last['rsi']) if ('rsi' in last and pd.notna(last['rsi'])) else 50.0
    oversold = rsi < rsi_cap
    ev['rsi'] = round(rsi, 1)
    # (4) ستوك في القاع بدوران صاعد
    k = float(last['stoch_rsi_k']) if ('stoch_rsi_k' in last and pd.notna(last['stoch_rsi_k'])) else 100.0
    d = float(last['stoch_rsi_d']) if ('stoch_rsi_d' in last and pd.notna(last['stoch_rsi_d'])) else 100.0
    k_prev = float(prev['stoch_rsi_k']) if ('stoch_rsi_k' in prev and pd.notna(prev['stoch_rsi_k'])) else 100.0
    stoch_low_turn = (k < BOTTOM_STOCH_MAX) and ((k > k_prev) or (k > d))
    ev['stoch_k'] = round(k, 1)
    ev['stoch_turn'] = stoch_low_turn
    # (5) شمعة الارتداد
    candle_range = max(float(last['high']) - float(last['low']), 1e-12)
    lower_wick = (min(float(last['open']), close) - float(last['low'])) / candle_range
    green = close > float(last['open'])
    bounce_candle = green or (lower_wick >= BOTTOM_WICK_MIN_RATIO)
    ev['bounce_candle'] = bool(bounce_candle)
    ev['lower_wick_pct'] = round(lower_wick * 100.0, 1)
    ev['downtrend_strict'] = downtrend
    if downtrend:
        ok = at_bottom and oversold and stoch_low_turn and bounce_candle
    else:
        ok = at_bottom and oversold and stoch_low_turn and (touched_bb or bounce_candle)
    if not ok:
        ev['reason'] = 'شروط القاع/الارتداد غير مكتملة'
    return ok, ev


def detect_squeeze_setup(df: pd.DataFrame, market_regime: str = '') -> Tuple[bool, Dict[str, Any]]:
    """[V9.24.0] تجهيز الاختراقية: انضغاط فعلي + نقض هيكلي للهبوط.
    (أ) الانضغاط: عرض بولنجر الحالي في أدنى SQUEEZE_BBWP_MAX من آخر 220 شمعة (مستبعدين آخر 20)
    (ب) النقض الهيكلي: لا اختراق صاعد يُشترى ضد هيكل هابط (EMA21<EMA50 مع السعر تحت EMA50)
        — هذا بالضبط نمط "ارتداد الموتى" الذي خسر به البوت 10 صفقات متتالية في الهبوط الحاد."""
    ev: Dict[str, Any] = {'setup': 'squeeze'}
    if len(df) < 240:
        ev['reason'] = 'شموع غير كافية للانضغاط'
        return False, ev
    last = df.iloc[-1]
    bbw_all = df['bb_width'].astype(float)
    cur_bbw = float(bbw_all.iloc[-1]) if pd.notna(bbw_all.iloc[-1]) else None
    if cur_bbw is None:
        return False, ev
    hist = bbw_all.iloc[-220:-20]
    bbwp = float((hist <= cur_bbw).mean()) if len(hist) > 20 else 1.0
    ev['bbwp'] = round(bbwp, 2)
    squeezed = bbwp <= SQUEEZE_BBWP_MAX
    ema50 = float(last['ema_50']) if pd.notna(last['ema_50']) else None
    ema21 = float(last['ema_21']) if ('ema_21' in last and pd.notna(last['ema_21'])) else None
    close = float(last['close'])
    # [V9.24.0 +باك تيست] تشديد: الاختراق الصاعد استمرارية — يشترط هيكل صاعد كامل
    # (EMA21 فوق EMA50 والسعر فوق EMA50). الإصدار الأول (نقض الهابط الصريح فقط)
    # خسر -13% في السوق المنشاري: اختراقات زائفة في نطاق رملي بلا اتجاه حقيقي
    bullish_struct = bool(ema50 is not None and ema21 is not None and ema21 > ema50 and close > ema50)
    ev['bullish_structure'] = bullish_struct
    # [V9.24.0+BT] قوة 24 ساعة: الرمز نفسه صاعد على نافذة القاع (96 شمعة) —
    # اختراق انضغاط في رمز هابط يوميًا = سكين ملتفة لا استمرارية
    mom_24h = None
    if len(df) > BOTTOM_WINDOW_BARS:
        ref_close = float(df['close'].iloc[-BOTTOM_WINDOW_BARS])
        if ref_close > 0:
            mom_24h = (close - ref_close) / ref_close * 100.0
            ev['mom_24h_pct'] = round(mom_24h, 2)
    rising_24h = bool(mom_24h is not None and mom_24h > 0.0)
    ev['rising_24h'] = rising_24h
    ok = squeezed and bullish_struct and rising_24h
    if not ok:
        ev['reason'] = ('انضغاط غير فعّال' if not squeezed
                        else 'هيكل غير صاعد — الاختراق استمرارية لا انعكاس' if not bullish_struct
                        else 'الرمز هابط على 24 ساعة — لا اختراق ضد الموجة اليومية')
    return ok, ev


def detect_trend_structure_setup(df: pd.DataFrame, market_regime: str = '') -> Tuple[bool, Dict[str, Any]]:
    """[V9.24.0] تجهيز الاستراتيجيات الاتجاهية (تقاطعات/زخم/تراجع): هيكل صاعد سليم.
    السعر فوق EMA50 + EMA21 فوق EMA50 (أو على الأقل السعر فوق الاثنتين مع ازدياد ADX).
    هذا يمنع شراء التقاطعات داخل القنوات الهابطة مهما بدا التقاطع جميلًا."""
    ev: Dict[str, Any] = {'setup': 'trend_structure'}
    if len(df) < 60:
        ev['reason'] = 'شموع غير كافية'
        return False, ev
    last = df.iloc[-1]
    close = float(last['close'])
    ema50 = float(last['ema_50']) if pd.notna(last['ema_50']) else None
    ema21 = float(last['ema_21']) if ('ema_21' in last and pd.notna(last['ema_21'])) else None
    adx = float(last['adx']) if ('adx' in last and pd.notna(last['adx'])) else 0.0
    if ema50 is None:
        return False, ev
    above_ema50 = close > ema50
    above_ema21 = (ema21 is not None and close > ema21)
    struct_up = (ema21 is not None and ema21 > ema50)
    ok = above_ema50 and (above_ema21 or struct_up) and adx >= 18.0
    ev['above_ema50'] = above_ema50
    ev['struct_up'] = struct_up
    ev['adx'] = round(adx, 1)
    if not ok:
        ev['reason'] = 'هيكل صاعد غير سليم (تقاطعات في قناة هابطة مرفوضة)'
    return ok, ev


# خريطة الكواشف: مفتاح الاستراتيجية (كما في حلقة المسح) → دالة التجهيز الشرطي
STRATEGY_SETUP_SCANNERS: Dict[str, Any] = {
    'BB_STOCH': detect_bottom_bounce_setup,        # الارتداد: قاع + مؤشرات ارتداد (طلب المستخدم)
    'BB_SQUEEZE': detect_squeeze_setup,            # الاختراقية: انضغاط + نقض الهيكل الهابط
    'SR_BREAKOUT': None,                            # اختراق الدعوم: الفحص نفسه شرطي (كسر مقاومة محدد سلفًا)
    'MACD_EMA': detect_trend_structure_setup,
    'EMA_RSI': detect_trend_structure_setup,
    'PULLBACK': detect_trend_structure_setup,
    'BULLISH_MOMENTUM': detect_trend_structure_setup,
}


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
    # [V9.24.0] إلغاء النقض العالمي للهبوط هنا — منطق الريم انتقل إلى:
    # كاشف القاع+الارتداد (detect_bottom_bounce_setup) الذي يشتد في الهبوط بدل أن يمنع،
    # وخريطة السوق العام (MARKET_STATE_STRATEGY_ALLOW) التي تحسم الاستراتيجيات المسموحة.
    # النتيجة الحية قبل الإلغاء: 820 فحصًا صفرًا إشارةً في STRONG_DOWNTREND — استراتيجية
    # القاع الوحيدة كانت معطلة في السوق الوحيد الذي تكون فيه هي الفكرة الصحيحة.
    
    # فلتر: تجنب الإشارات في النطاق الضيق جدًا
    bb_width_ok = last['bb_width'] > 0.02
    
    # فلتر: السكين الساقطة العمياء (RSI<25 = غرق حقيقي غير قابل للاقتناص)
    price_not_oversold = last['rsi'] > 25
    
    conditions = {
        "price_touch_bb": price_touch_bb,
        "stoch_cross_up": stoch_cross_up,
        "oversold_area": oversold_area,
        "volume_spike": volume_spike,
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
    
    # [V9.24.0] نقض هيكلي دائم: لا اختراق صاعد يُشترى ضد هيكل هابط
    # (EMA21 تحت EMA50 والسعر تحت EMA50) — نمط "ارتداد الموتى" الذي
    # خسر به V9.21→V9.23 كل صفقاته العشر في STRONG_DOWNTREND.
    # كاشف التجهيز detect_squeeze_setup يفرض النقض ذاته في مسار التوصيات،
    # وهذا هنا يحمي مسار المُطلِقات المباشرة أيضًا.
    falling_structure = (last['ema_21'] < last['ema_50']) and (last['close'] < last['ema_50'])
    if falling_structure:
        return False
    
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

        # [V9.20.0] عمق السوق من المزودين البديلين — النسب النسبية للعرض/الطلب صالحة عبر المنصات المتشابهة
        order_book = data_feed.get_order_book(symbol, ORDER_BOOK_DEPTH_LIMIT) if data_feed is not None else None
        if not order_book and (rate_guard.banned_until - time.time()) <= 0:
            # [V9.21.0] احتياط REST فقط حين لا حظر — أثناء الحظر يُرفض الإشارة
            # بدل تعليق خيط المسح في acquire() حتى نهاية الحظر
            order_book = safe_get_order_book(symbol, ORDER_BOOK_DEPTH_LIMIT)
        if not order_book:
            log_rejection(symbol, "Order Book Fetch Failed", {"error": "لا مصدر متاح (مركز WS/البدائل/REST أثناء الحظر)"})
            return False
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
    # [إصلاح V9.12.0 — قاتل تجمد اللوحة] كان القفل ممسكًا طوال العملية كاملة:
    # نداء رصيد Binance (ينام دقائق أثناء الحظر عبر rate_guard) + أمر البيع +
    # UPDATE/commit على PostgreSQL + رسالة تليجرام. أي تعليق شبكي (والعميل سابقًا
    # بلا timeout) يبقي signal_cache_lock محتجزًا للأبد فتموت نقاط اللوحة الثلاث
    # (market_status / system_status / signals) ويتجمد المتصفح.
    # الآن: القفل للبحث والنسخ والحارس فقط (ميكروثوانٍ) وكل العمل خارج القفل.
    with signal_cache_lock:
        if signal_id in _closing_signal_ids:
            logger.info(f"ℹ️ [إغلاق] الصفقة (ID: {signal_id}) قيد الإغلاق بالفعل من خيط آخر — تجاهل الطلب المزدوج.")
            return False
        signal_to_close, symbol_to_close = None, None
        for symbol, signal_data in open_signals_cache.items():
            if signal_data['id'] == signal_id:
                signal_to_close, symbol_to_close = dict(signal_data), symbol
                break
        if not signal_to_close:
            logger.warning(f"⚠️ [إغلاق] محاولة إغلاق صفقة غير موجودة في الكاش (ID: {signal_id}). ربما أغلقت بالفعل.")
            return False
        _closing_signal_ids.add(signal_id)
    try:
        entry_price = float(signal_to_close['entry_price'])
        profit_percentage = ((closing_price - entry_price) / entry_price) * 100

        # --- [إصلاح] منطق البيع عند الإغلاق (خارج القفل تمامًا) ---
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

            # حذف نهائي من الكاش — تحت القفل لحظة واحدة
            with signal_cache_lock:
                open_signals_cache.pop(symbol_to_close, None)
                # [V9.18.0] توثيق الإغلاق لتهدئة الرمز (لا توصية فورية بنفس الرمز)
                _recent_close_ts[symbol_to_close] = time.time()

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
    finally:
        # تحرير حارس الازدواجية في كل الحالات (نجاح/فشل/استثناء)
        with signal_cache_lock:
            _closing_signal_ids.discard(signal_id)


def _symbol_recently_closed(symbol: str) -> bool:
    """[V9.18.0] هل أُغلقت صفقة لهذا الرمز خلال نافذة التهدئة؟
    توصيات الفلاتر بلا مُطلِق لحظي قد تعيد شراء الرمز نفسه فور إغلاقه (SL/TP)
    فتتأرجح حول نفس السعر — التهدئة تمنع ذلك. الذاكرة أولًا (فورية)، وقاعدة
    البيانات احتياط يصمد لإعادة التشغيل. الفشل الشبكي لا يمنع الدخول (يُسجل فقط)."""
    cooldown_sec = RECOMMENDATION_COOLDOWN_MIN * 60
    ts = _recent_close_ts.get(symbol)
    if ts and (time.time() - ts) < cooldown_sec:
        return True
    try:
        if check_db_connection() and conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT EXTRACT(EPOCH FROM (NOW() - closed_at)) AS sec "
                    "FROM signals WHERE symbol = %s AND status = 'closed' AND closed_at IS NOT NULL "
                    "ORDER BY closed_at DESC LIMIT 1", (symbol,))
                row = cur.fetchone()
                if row and row[0] is not None and float(row[0]) < cooldown_sec:
                    return True
    except Exception as cd_err:
        logger.debug(f"[تهدئة الرمز] تعذر فحص قاعدة البيانات لـ {symbol}: {cd_err}")
    return False

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
            # [V9.18.0] سطر المصدر: مُطلِق استراتيجية أو اجتياز فلاتر (توصية مفتوحة)
            _src = signal_data['signal_details'].get('source')
            _fit = signal_data['signal_details'].get('fit_score')
            source_line = ("\n*المصدر:* 💡 اجتياز فلاتر الاستراتيجية (توصية مفتوحة)"
                           + (f" — مطابقة {_fit:.0f}/100" if _fit is not None else "")) \
                if _src == 'filter_recommendation' else "\n*المصدر:* مُطلِق الاستراتيجية"
            # [تحسين V9.13.0] سطر القائد التابع له في رسالة التوصية
            leader_info = signal_data['signal_details'].get('leader_info') or {}
            leader_line = (f"\n*القائد التابع له:* `{leader_info['leader']}` (ارتباط {float(leader_info.get('corr') or 0):.2f})"
                           if leader_info.get('leader') else "")
            telegram_message = (
                f"💡 *توصية شراء {trade_type} جديدة*\n\n"
                f"*العملة:* `{signal_data['symbol']}`\n*الاستراتيجية:* `{signal_data['strategy_name'].replace('_', ' ')}`\n"
                f"*سعر الدخول:* `{entry_price:.4f}`\n*الهدف الأول:* `{target_price:.4f}`\n"
                f"*وقف الخسارة:* `{stop_loss:.4f}`\n*RR Ratio:* `{rr_ratio:.2f}`"
                f"{source_line}{leader_line}\n\n"
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
        # [V9.24.0] تسجيل تاريخ الريم العام لحساب ثبات الاستمرارية في فلتر الثبات
        try:
            with market_state_lock:
                MARKET_REGIME_HISTORY.append((time.time(), current_market_state.get('overall_regime', 'UNCERTAIN')))
        except Exception:
            pass
        logger.info(f"✅ [حالة السوق] الحالة العامة المحددة: {overall_regime}")
    except Exception as e:
        logger.error(f"❌ [حالة السوق] خطأ في التحديث: {e}", exc_info=True)


def market_up_persistence_minutes() -> float:
    """[V9.24.0] كم دقيقة الريم العام متصلًا في عائلة الصاعد (UPTREND/STRONG_UPTREND)؟
    يُحسب من سجل MARKET_REGIME_HISTORY المملوء دوريًا. 0.0 = ليس صاعدًا الآن."""
    try:
        with market_state_lock:
            hist = list(MARKET_REGIME_HISTORY)
        if not hist:
            return 0.0
        last_ts, last_reg = hist[-1]
        if 'UPTREND' not in str(last_reg).upper():
            return 0.0
        for ts, reg in reversed(hist):
            if 'UPTREND' not in str(reg).upper():
                return max(0.0, (last_ts - ts) / 60.0)
        return max(0.0, (time.time() - hist[0][0]) / 60.0)
    except Exception:
        return 0.0

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
    html = """
<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
    <meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CryptoBot {__VER__} // NEON TERMINAL</title>
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
                <h1 class="text-2xl md:text-3xl font-extrabold flicker"><span class="text-accent-green neon-text">لوحة تحكم</span> <span class="font-mono text-text-secondary text-lg md:text-xl" dir="ltr">{__VER__}//NEON</span> <span id="df-chip" class="font-mono text-xs px-2 py-0.5 rounded border border-gray-600 text-text-secondary" dir="ltr">FEED:…</span></h1>
            </div>
            <div id="trend-lights-container" class="flex items-center gap-x-6 bg-black/40 px-4 py-2 rounded-lg border border-border-color"></div>
        </header>
        <section class="mb-6 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-5">
            <div class="card p-4"><h3 class="font-bold mb-3 text-lg text-text-secondary">حالة السوق</h3><div id="overall-regime" class="text-2xl font-bold text-center">...</div></div>
            <div class="card p-4"><h3 class="font-bold mb-3 text-lg text-text-secondary">الجلسات النشطة</h3><div id="active-sessions-list" class="flex flex-wrap gap-2 items-center justify-center pt-2">...</div></div>
            <div class="card p-4"><h3 class="font-bold mb-3 text-lg text-text-secondary">الصفقات المفتوحة</h3><div id="open-trades-count" class="text-2xl font-bold text-center">...</div></div>
            <div class="card p-4 flex flex-col justify-center items-center"><h3 class="font-bold text-lg text-text-secondary mb-2">التداول الحقيقي</h3><div class="flex items-center space-x-3 space-x-reverse"><span id="trading-status-text" class="font-bold text-lg"></span><label class="flex items-center cursor-pointer"><div class="relative"><input type="checkbox" id="trading-toggle" class="sr-only" onchange="toggleTrading()"><div class="toggle-bg block bg-gray-600 w-12 h-7 rounded-full"></div></div></label></div><div class="mt-2 text-xs text-text-secondary">رصيد USDT: <span id="usdt-balance" class="font-mono">...</span></div></div>
        </section>
        <!-- [V9.23.0] الترشيح الذكي — برهان لا تكهن -->
        <section class="card p-4 mb-6">
            <div class="flex items-center justify-between mb-3 flex-wrap gap-2">
                <h3 class="font-bold text-lg text-text-secondary">🎯 الترشيح الذكي — برهان باك تيست حي لا تكهن</h3>
                <div class="text-xs text-text-secondary font-mono" id="smart-picks-updated" dir="ltr">--</div>
            </div>
            <div id="smart-picks-container" class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
                <div class="text-text-secondary text-sm text-center py-4">جاري تحميل الأدلة...</div>
            </div>
        </section>
        <!-- [V9.25.0] دائرة الفحص الموسعة — 10 عملات × كل استراتيجية (ترشيح حتمي لا تكهن) -->
        <section class="card p-4 mb-6">
            <div class="flex items-center justify-between mb-3 flex-wrap gap-2">
                <h3 class="font-bold text-lg text-text-secondary">🔍 دائرة الفحص الموسعة — 10 عملات × كل استراتيجية</h3>
                <div class="text-xs text-text-secondary font-mono" id="candidates-meta" dir="ltr">--</div>
            </div>
            <div id="strategy-candidates-container" class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
                <div class="text-text-secondary text-sm text-center py-4">بناء الدائرة الواسعة جارٍ...</div>
            </div>
            <div class="text-[11px] text-text-secondary mt-2">كل استراتيجية تفحص فقط عملاتها العشر الأقرب لظروفها الحتمية (موقع السعر في مدى 24س + اتجاه الحركة + ضيق المدى + السيولة) — مرّر على العملة لترى سبب ترشيحها. الباهتة = ممنوعة حاليًا بخريطة حالة السوق.</div>
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
        <!-- [تحسين V9.13.0] خريطة القيادة: أي قائد سيادي تتبعه كل عملة + سلوك القادة الآن -->
        <section class="card p-4 mb-6">
            <div class="flex items-center justify-between mb-3 flex-wrap gap-2">
                <h3 class="font-bold text-lg text-text-secondary">👑 خريطة القيادة — من يتبع من؟</h3>
                <div class="text-xs text-text-secondary font-mono" id="leader-summary" dir="ltr">--</div>
            </div>
            <div class="grid grid-cols-1 md:grid-cols-3 gap-3 mb-3" id="leader-cards">
                <div class="text-text-secondary text-sm text-center py-4">جاري أول جلب لبيانات القادة (BTC / ETH / SOL)...</div>
            </div>
            <div class="overflow-x-auto rounded-lg border border-border-color">
                <table class="min-w-full text-sm text-right">
                    <thead class="border-b border-border-color bg-black/20"><tr>
                        <th class="p-2 font-semibold">العملة</th>
                        <th class="p-2 font-semibold">القائد التابع له</th>
                        <th class="p-2 font-semibold">قوة الارتباط (48س)</th>
                        <th class="p-2 font-semibold">سلوك القائد الآن</th>
                        <th class="p-2 font-semibold">قرار البوابة</th>
                    </tr></thead>
                    <tbody id="leader-table"><tr><td colspan="5" class="text-center text-text-secondary py-4">لم يبدأ التصنيف بعد — يحدث تلقائيًا مع أول دورة مسح</td></tr></tbody>
                </table>
            </div>
            <div class="mt-2 text-xs text-text-secondary flex flex-wrap gap-x-4">
                <span>حد الارتباط للتبعية: <span id="leader-corr-min" class="font-mono text-accent-blue">--</span></span>
                <span>عتبة رفض القائد الهابط: <span id="leader-bearish" class="font-mono text-accent-red">--</span></span>
                <span>رفضات بوابة القائد منذ الإقلاع: <span id="leader-vetoes" class="font-mono text-accent-yellow">0</span></span>
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
                <div class="flex items-center gap-2"><span class="text-text-secondary">التنفيذ:</span><span id="sys-exec" class="font-mono">--</span></div>
                <div class="flex items-center gap-2"><span class="text-text-secondary">PnL اليوم:</span><span id="sys-pnl" class="font-mono">--</span></div>
                <div class="flex items-center gap-2"><span class="text-text-secondary">قاطع الحماية:</span><span id="sys-lossguard" class="font-mono">--</span></div>
                <div class="flex items-center gap-2"><span class="text-text-secondary">التخزين:</span><span id="sys-storage" class="font-mono text-accent-blue">--</span></div>
                <div class="flex items-center gap-2"><span class="text-text-secondary">البيانات الحية:</span><span id="sys-ws" class="font-mono">--</span></div>
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
            <div id="rejections-tab" class="tab-content hidden"><div id="strategy-pairs" class="mb-3"></div><div id="rejections-summary" class="mb-3"></div><div id="rejections-list" class="card p-4 max-h-[55vh] overflow-y-auto space-y-2"></div></div>
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
// [إصلاح V9.12.0] مهلة 15 ثانية لكل استطلاع — كان الطلب بلا timeout يبقى معلقًا
// للأبد عند تعلق نقطة نهاية خلف قفل محتجز، فتتجمد اللوحة بلا أي مؤشر
async function fetchData(url, timeoutMs = 15000) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeoutMs);
    try { const r = await fetch(url, { signal: ctrl.signal }); return r.ok ? await r.json() : null; }
    catch (e) { console.error('Fetch Error:', e); return null; }
    finally { clearTimeout(timer); }
}

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
                <td class="p-4 font-bold">${s.symbol}<br><span class="text-xs text-text-secondary">${s.strategy_name.replace(/_/g, ' ')}</span><br>${(() => { const src = s.signal_details && s.signal_details.source; const fit = s.signal_details && s.signal_details.fit_score; return src === 'filter_recommendation' ? `<span class="text-[10px] px-1.5 py-0.5 rounded border border-accent-green/40 text-accent-green">💡 توصية فلاتر${fit ? ' · ' + fit : ''}</span>` : `<span class="text-[10px] px-1.5 py-0.5 rounded border border-accent-blue/40 text-accent-blue">إشارة استراتيجية</span>`; })()}</td>
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
function updateStrategyPairs() {
    // [V9.17.0] الأزواج المرشحة لكل استراتيجية — مطابقة النمط السوقي بدل العشوائي
    fetchData('/api/strategy_pairs').then(sp => {
        if (!sp || sp.error) return;
        const box = document.getElementById('strategy-pairs');
        if (!box) return;
        if (!sp.enabled) { box.innerHTML = ''; return; }
        const rows = Object.entries(sp.pools || {}).sort((a, b) => (b[1][0]?.score || 0) - (a[1][0]?.score || 0)).map(([name, arr]) => {
            const chips = (arr && arr.length)
                ? arr.map(p => `<span class="inline-block px-2 py-0.5 m-0.5 rounded-full text-xs font-mono ${p.score >= 60 ? 'bg-accent-green/20 text-accent-green' : 'bg-accent-yellow/20 text-accent-yellow'}" title="النمط: ${p.regime_ar} — الدرجة ${p.score}">${p.symbol} ${p.score}%</span>`).join('')
                : '<span class="text-xs text-text-secondary">لا أزواج مطابقة في هذا الريم حاليًا — لا فحص عبثًا</span>';
            return `<div class="mb-2"><div class="text-xs font-bold text-neon mb-1 font-mono">${name}</div><div>${chips}</div></div>`;
        }).join('');
        const upd = sp.updated_at ? new Date(sp.updated_at).toLocaleTimeString('ar-EG') : '—';
        box.innerHTML = `<div class="card p-4">
            <div class="flex flex-wrap items-center justify-between gap-2 mb-2">
                <h4 class="text-sm font-bold text-neon">🎯 الأزواج المرشحة لكل استراتيجية (مطابقة النمط السوقي)</h4>
                <div class="font-mono text-xs text-text-secondary">حد الدرجة: <span class="text-white">${sp.min_score}</span> | تحديث: <span class="text-white">${upd}</span></div>
            </div>
            ${rows || '<div class="text-xs text-text-secondary">بانتظار اكتمال أول دورة مسح...</div>'}
            <div class="text-xs text-text-secondary mt-2">كل استراتيجية تُفحص فقط على العملات التي ينطبق نمطها السوقي الحالي (اتجاه صاعد / نطاق مترنم / انضغاط) على شخصيتها — بلا ترشيح عشوائي أو حسب السيولة فقط.</div>
        </div>`;
    });
}
function updateRejections() {
    updateStrategyPairs();
    fetchData('/api/rejection_logs').then(data => {
        if (!data) return;
        document.getElementById('rejections-list').innerHTML = data.length
            ? data.map(r => `<div class="p-2 border-b border-border-color"><span class="font-mono text-xs text-text-secondary">${new Date(r.timestamp).toLocaleString('ar-EG')}</span>: <strong class="text-accent-yellow">${r.symbol}</strong> - ${r.reason} <span class="text-xs text-gray-500">${JSON.stringify(r.details)}</span></div>`).join('')
            : '<div class="text-center text-text-secondary py-6">لا توجد رفضات مسجلة بعد (الكاش يحفظ آخر 100)</div>';
    });
    // [تحسين V9.12.0] بطاقة تحليل أسباب الرفض — الصورة الكاملة منذ الإقلاع
    fetchData('/api/rejection_summary').then(s => {
        if (!s || s.error) return;
        const box = document.getElementById('rejections-summary');
        if (!box) return;
        const filtersHtml = (s.filters && s.filters.length)
            ? s.filters.map(([name, count]) => {
                const pct = s.total_filter_rejects ? Math.round(count / s.total_filter_rejects * 100) : 0;
                return `<div class="flex items-center gap-2 mb-1"><span class="w-40 shrink-0 text-xs text-text-secondary truncate">${name}</span><div class="flex-1 h-2 bg-black/40 rounded overflow-hidden"><div class="h-full bg-accent-yellow" style="width:${pct}%"></div></div><span class="font-mono text-xs text-accent-yellow w-16 text-left">${count} (${pct}%)</span></div>`;
            }).join('')
            : '<div class="text-xs text-text-secondary mb-2">لا رفضات فلاتر بعد</div>';
        const stratHtml = (s.strategies && s.strategies.length)
            ? `<table class="w-full text-xs mt-1"><thead><tr class="text-text-secondary border-b border-border-color"><th class="text-right py-1">الاستراتيجية</th><th class="py-1">فحوصات</th><th class="py-1">نجاحات</th><th class="py-1">نسبة النجاح</th><th class="py-1" title="رفضات الفلتر الخاص بهذه الاستراتيجية (ملف فلترة منطقي لكل نمط)">رفض فلترها</th></tr></thead><tbody>${
                s.strategies.map(r => `<tr class="border-b border-border-color/50"><td class="text-right py-1 font-mono" title="${r.filter_breakdown ? Object.entries(r.filter_breakdown).map(([k,v]) => k + ': ' + v).join(' | ') : ''}">${r.strategy}</td><td class="text-center font-mono">${r.checks}</td><td class="text-center font-mono text-accent-green">${r.passes}</td><td class="text-center font-mono ${r.pass_rate_pct > 0 ? 'text-accent-green' : 'text-text-secondary'}">${r.pass_rate_pct}%</td><td class="text-center font-mono ${r.prefilter_rejects ? 'text-accent-yellow' : 'text-text-secondary'}">${r.prefilter_rejects || 0}</td></tr>`).join('')
            }</tbody></table>`
            : '<div class="text-xs text-text-secondary">لا فحوصات استراتيجيات بعد</div>';
        const lastAt = s.last_rejection_at ? new Date(s.last_rejection_at).toLocaleTimeString('ar-EG') : '—';
        box.innerHTML = `<div class="card p-4">
            <div class="flex flex-wrap items-center justify-between gap-2 mb-2">
                <h4 class="text-sm font-bold text-neon">📊 تحليل أسباب الرفض (منذ الإقلاع)</h4>
                <div class="font-mono text-xs text-text-secondary">رفضات فلاتر: <span class="text-accent-yellow">${s.total_filter_rejects}</span> | فحوصات استراتيجيات: <span class="text-white">${s.total_strategy_checks}</span> | نجاحات: <span class="text-accent-green">${s.total_strategy_passes}</span> | آخر رفض: <span class="text-white">${lastAt}</span></div>
            </div>
            <div class="mb-3">${filtersHtml}</div>
            <div class="text-xs text-text-secondary mb-1">أقرب الاستراتيجيات للاشتعال (مرتبة بالنجاحات) — كل استراتيجية لها فلترها المنطقي الخاص (V9.14.0):</div>
            ${stratHtml}
        </div>`;
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
        // [V9.21.0] حالة التنفيذ: مؤجل أثناء حظر باينانس بينما المسح مستمر عبر البدائل
        const execEl = document.getElementById('sys-exec');
        if (data.exec_blocked) { execEl.textContent = data.scan_during_ban ? 'مؤجل (حظر) — المسح مستمر' : 'متوقف (حظر)'; execEl.className = 'font-mono text-accent-yellow'; }
        else { execEl.textContent = 'حر'; execEl.className = 'font-mono text-accent-green'; }
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
        // [V9.16.0] حالة مركز بيانات WebSocket
        const wsEl = document.getElementById('sys-ws');
        const sh = data.stream_hub || {};
        if (sh.enabled && sh.connected) { wsEl.textContent = `🛰️ WS مباشر (${sh.subscribed ?? 0} تدفق / ${sh.buffers_warm ?? 0} مخزون دافئ)`; wsEl.className = 'font-mono text-accent-green'; }
        else if (sh.enabled) { wsEl.textContent = 'إعادة اتصال... (REST احتياطيًا)'; wsEl.className = 'font-mono text-accent-yellow'; }
        else { wsEl.textContent = 'REST'; wsEl.className = 'font-mono text-text-secondary'; }
    });
}

// [تحسين V9.13.0] خريطة القيادة: بطاقات القادة + جدول من يتبع من + قرارات البوابة
function updateLeaderMap() {
    fetchData('/api/leader_map').then(data => {
        if (!data || !data.enabled) return;
        const cMap = {green: 'text-accent-green', red: 'text-accent-red', yellow: 'text-accent-yellow'};
        const barMap = {green: '#00ff41', red: '#ff3b3b', yellow: '#ffd60a'};
        const names = {BTCUSDT: 'BTC 🟠', ETHUSDT: 'ETH 🔵', SOLUSDT: 'SOL 🟣'};
        const bearish = data.settings?.bearish_score ?? -18;
        // بطاقات القادة الثلاثة (سلوك كل قائد الآن — نفس محرك البوصلة)
        const cards = document.getElementById('leader-cards');
        const lkeys = Object.keys(data.leaders || {});
        cards.innerHTML = lkeys.length ? lkeys.map(sym => {
            const t = data.leaders[sym].trend || {};
            const pct = Math.min(100, Math.abs(t.score || 0)) / 2;
            const pos = (t.score || 0) >= 0 ? `left:50%;width:${pct}%` : `left:${50 - pct}%;width:${pct}%`;
            return `<div class="bg-black/40 rounded-lg border border-border-color p-3 flex flex-col items-center justify-center gap-1">
                <div class="text-sm font-bold text-text-secondary" dir="ltr">${names[sym] || sym}${data.leaders[sym].stale ? ' ⏳' : ''}</div>
                <div class="text-2xl leading-none ${cMap[t.color] || ''}">${t.arrow || '▬'}</div>
                <div class="font-bold ${cMap[t.color] || 'text-text-secondary'}">${t.label || '--'} <span class="font-mono text-xs">(${t.score ?? '--'})</span></div>
                <div class="w-full h-1.5 bg-gray-800 rounded-full relative overflow-hidden" dir="ltr">
                    <div class="absolute left-1/2 top-0 w-px h-full bg-gray-600"></div>
                    <div class="absolute top-0 h-full rounded-full transition-all duration-700" style="${pos};background:${barMap[t.color] || '#888'}"></div>
                </div>
            </div>`;
        }).join('') : '<div class="text-text-secondary text-sm py-3">لا بيانات قادة بعد (خيط البيانات يعمل...)</div>';
        // جدول العملات: من يتبع من
        const tbody = document.getElementById('leader-table');
        const rows = (data.map || []);
        tbody.innerHTML = rows.length ? rows.map(r => {
            let leaderCell, behaviorCell, decisionCell;
            if (r.is_leader) {
                leaderCell = '<span class="text-accent-yellow font-bold">👑 قائد سيادي</span>';
                behaviorCell = '--';
                decisionCell = '<span class="text-accent-yellow">يُتبع</span>';
            } else if (!r.leader) {
                leaderCell = '<span class="text-text-secondary">مستقلة</span>';
                behaviorCell = '--';
                decisionCell = '<span class="text-text-secondary">على سلوكها</span>';
            } else {
                const lt = data.leaders?.[r.leader]?.trend || {};
                const sc = lt.score ?? 0;
                leaderCell = `<span class="font-bold text-accent-blue" dir="ltr">${names[r.leader] || r.leader}</span>`;
                behaviorCell = `<span class="${cMap[lt.color] || 'text-text-secondary'}">${lt.arrow || '▬'} ${lt.label || '--'} <span class="font-mono">(${sc})</span></span>`;
                decisionCell = sc <= bearish
                    ? '<span class="text-accent-red">🚫 رفض شراء (قائد هابط)</span>'
                    : (sc >= 18 ? '<span class="text-accent-green">✅ مواتٍ للشراء</span>' : '<span class="text-accent-yellow">▬ محايد</span>');
            }
            const c = Math.max(0, Math.min(1, r.corr || 0));
            const cPct = Math.round(c * 100);
            return `<tr class="border-b border-border-color/50 hover:bg-white/5">
                <td class="p-2 font-bold font-mono" dir="ltr">${r.symbol}</td>
                <td class="p-2">${leaderCell}</td>
                <td class="p-2"><div class="flex items-center gap-2"><div class="flex-1 h-1.5 bg-black/40 rounded overflow-hidden min-w-[60px]"><div class="h-full bg-accent-blue" style="width:${cPct}%"></div></div><span class="font-mono text-xs w-10">${(r.corr || 0).toFixed(2)}</span></div></td>
                <td class="p-2 text-xs">${behaviorCell}</td>
                <td class="p-2 text-xs">${decisionCell}</td>
            </tr>`;
        }).join('') : '<tr><td colspan="5" class="text-center text-text-secondary py-4">لم يبدأ التصنيف بعد — يحدث تلقائيًا مع أول دورة مسح</td></tr>';
        const sm = data.summary || {};
        document.getElementById('leader-summary').textContent =
            `BTC:${sm['BTCUSDT'] || 0} | ETH:${sm['ETHUSDT'] || 0} | SOL:${sm['SOLUSDT'] || 0} | مستقلة:${sm['independent'] || 0}`;
        document.getElementById('leader-corr-min').textContent = data.settings?.corr_min ?? '--';
        document.getElementById('leader-bearish').textContent = data.settings?.bearish_score ?? '--';
        document.getElementById('leader-vetoes').textContent =
            Object.values(data.veto_stats || {}).reduce((a, b) => a + b, 0);
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

// [V9.20.0] شارة مصدر بيانات السوق (طبقة متعددة المصادر) — قراءة ذاكرة كل دقيقة
const updateDataFeed = () => {
    fetch('/api/datafeed').then(r => r.json()).then(d => {
        const el = document.getElementById('df-chip');
        if (!el || !d) return;
        const st = d.stats || {};
        const served = (st.klines_served || 0) + (st.tickers_served || 0) + (st.orderbook_served || 0);
        const fb = (st.klines_binance_fallback || 0) + (st.tickers_binance_fallback || 0) + (st.orderbook_binance_fallback || 0);
        const src = d.enabled ? String((d.last_source || {}).klines || 'binance').toUpperCase() : 'BINANCE';
        el.textContent = 'FEED:' + src + ' ✓' + served + ' ↩' + fb;
        el.style.color = (d.enabled && served > 0) ? '#34d399' : '#9ca3af';
        el.title = d.enabled ? ('مصادر السوق: ' + (d.providers || []).map(p => p.name + (p.cooling_down ? ' (تبريد)' : ' (نشط)')).join('، ')) : 'وضع باينانس فقط';
    }).catch(() => {});
};

function updateSmartPicks() {
    fetchData('/api/smart_picks').then(data => {
        if (!data) return;
        document.getElementById('smart-picks-updated').textContent = data.updated_at ? new Date(data.updated_at).toLocaleTimeString('ar-EG') : '--';
        const c = document.getElementById('smart-picks-container');
        if (!c) return;
        if (!data.enabled || !data.ready) { c.innerHTML = `<div class="text-text-secondary text-sm text-center py-4">${data.reason_ar || 'المحرك يجهز أدلته...'}</div>`; return; }
        if (!data.picks || !data.picks.length) { c.innerHTML = `<div class="text-accent-yellow text-sm text-center py-4">${data.reason_ar || 'لا خلية مثبتة الربحية الآن — الانتظار أفضل من التكهن'}</div>`; return; }
        c.innerHTML = data.picks.map((p, idx) => {
            const syms = (p.symbols || []).map(s => `<span class="text-xs px-2 py-0.5 rounded border ${idx === 0 ? 'border-accent-green/60 text-accent-green' : 'border-border-color text-text-secondary'}" style="border-style:solid">${s.symbol}</span>`).join(' ');
            const ev = p.evidence || {};
            return `<div class="rounded-lg border ${idx === 0 ? 'border-accent-green/60 bg-accent-green/5' : 'border-border-color bg-black/30'} p-3">
                <div class="flex items-center justify-between mb-1">
                    <span class="font-bold ${idx === 0 ? 'text-accent-green' : 'text-text-primary'}">${idx === 0 ? '★ ' : ''}${p.strategy.replace(/_/g, ' ')}</span>
                    <span class="text-[10px] px-1.5 py-0.5 rounded border border-border-color text-text-secondary">${p.regime_ar}</span>
                </div>
                <div class="text-xs text-text-secondary mb-2">${p.why_ar}</div>
                <div class="flex items-center justify-between flex-wrap gap-1">
                    <div class="flex gap-1 flex-wrap">${syms}</div>
                    <span class="text-[10px] font-mono text-text-secondary" dir="ltr">n=${ev.n} · PF=${ev.pf} · WR=${ev.wr}%</span>
                </div>
            </div>`;
        }).join('');
    });
}

// [V9.25.0] دائرة الفحص الموسعة — 10 عملات × كل استراتيجية (ترشيح حتمي لا تكهن)
function updateStrategyCandidates() {
    fetchData('/api/strategy_candidates').then(data => {
        if (!data) return;
        const metaEl = document.getElementById('candidates-meta');
        const c = document.getElementById('strategy-candidates-container');
        if (!c) return;
        if (metaEl) metaEl.textContent = `دائرة ${data.wide_universe || 0} عملة · فحص ${data.total || 0} (${data.per_strategy}/استراتيجية) · ${data.market_state || '--'}`;
        if (!data.candidates || !Object.keys(data.candidates).length) {
            c.innerHTML = '<div class="text-text-secondary text-sm text-center py-4">بناء الدائرة الواسعة جارٍ (بعد الإقلاع)...</div>';
            return;
        }
        const names = {MACD_EMA: 'تقاطعات MACD+EMA', BB_STOCH: 'الارتداد من القاع', EMA_RSI: 'تقاطع EMA+RSI', PULLBACK: 'شراء التراجع', BB_SQUEEZE: 'انفجار الانضغاط', BULLISH_MOMENTUM: 'زخم صاعد', SR_BREAKOUT: 'اختراق المقاومات'};
        c.innerHTML = Object.entries(data.candidates).map(([key, list]) => {
            const chips = (list || []).map(n => {
                const dim = n.examinable === false;
                const why = String(n.why_ar || '').replace(/"/g, '&quot;');
                return `<span class="text-xs px-2 py-0.5 rounded border ${dim ? 'border-border-color/40 text-text-secondary/50' : 'border-accent-green/50 text-accent-green'}" style="border-style:solid" title="${why}">${n.symbol} <span class="opacity-60 font-mono" dir="ltr">${Number(n.score).toFixed(0)}</span></span>`;
            }).join(' ');
            const examinable = (list || []).some(n => n.examinable !== false);
            return `<div class="rounded-lg border ${examinable ? 'border-border-color bg-black/30' : 'border-border-color/40 bg-black/20 opacity-70'} p-3">
                <div class="flex items-center justify-between mb-2">
                    <span class="font-bold text-sm text-text-primary">${names[key] || key}</span>
                    <span class="text-[10px] px-1.5 py-0.5 rounded border border-border-color text-text-secondary" dir="ltr">${(list || []).length}/10</span>
                </div>
                <div class="flex gap-1 flex-wrap">${chips}</div>
            </div>`;
        }).join('');
    });
}

document.addEventListener('DOMContentLoaded', () => {
    ['MarketStatus', 'Signals', 'Stats', 'Notifications', 'Rejections', 'SystemStatus', 'BtcTrend', 'LeaderMap', 'SmartPicks'].forEach(f => window[`update${f}`]());
    updateDataFeed();  // [V9.20.0] شارة مصدر البيانات
    // [تحسين V9.11.0] إيقاف الاستطلاع عند إخفاء التبويب — يمنع تراكم الطلبات
    // من التبويبات الخلفية ويخفف الضغط على خيوط الخادم (waitress queue)
    const whenVisible = (fn, ms) => setInterval(() => { if (!document.hidden) fn(); }, ms);
    whenVisible(updateMarketStatus, 5000); whenVisible(updateSignals, 4000); whenVisible(updateStats, 60000);
    whenVisible(updateNotifications, 15000); whenVisible(updateRejections, 15000); whenVisible(updateSystemStatus, 5000);
    whenVisible(updateBtcTrend, 30000);  // [تحسين V9.11] البوصلة تُحدّث كل 30 ثانية
    whenVisible(updateLeaderMap, 60000); // [تحسين V9.13.0] خريطة القيادة تُحدّث كل دقيقة (التصنيف بطيء التغير)
    whenVisible(updateDataFeed, 60000);  // [V9.20.0] شارة مصدر البيانات كل دقيقة
    whenVisible(updateSmartPicks, 120000); // [V9.23.0] الترشيح الذكي — الأدلة تتجدد كل 4 ساعات
    whenVisible(updateStrategyCandidates, 60000); // [V9.25.0] دائرة الفحص الموسعة — الترشيح يتجدد مع كل تحديث للقائمة (كل 30د)
});
</script>
</body></html>
"""
    # [V9.17.0] المصدر الوحيد للإصدار: حقن APP_VERSION وقت الطلب بدل النصوص المتفرقة
    return html.replace('{__VER__}', APP_VERSION)

@app.route('/')
def home(): return render_template_string(get_dashboard_html())

@app.route('/health')
def health_check():
    """[تحسين V9.8] نقطة فحص صحة خفيفة لمراقبة الخدمة على Render وأدوات Uptime."""
    return jsonify({"status": "ok", "version": APP_VERSION, "time": datetime.now(timezone.utc).isoformat()})

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
            'version': APP_VERSION,
            'client_ready': bool(client),
            'rate_guard': snap,
            # [V9.21.0] حالة التنفيذ: مؤجل أثناء حظر باينانس (المسح مستمر عبر البدائل)
            'exec_blocked': snap.get('banned_until') is not None,
            'scan_during_ban': bool(SCAN_DURING_BAN),
            # [V9.16.0] حالة مركز بيانات WebSocket للوحة التحكم
            'stream_hub': stream_hub.snapshot() if stream_hub is not None else {'enabled': False},
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

@app.route('/api/datafeed')
def api_datafeed():
    """[V9.20.0] حالة طبقة البيانات متعددة المصادر — قراءة ذاكرة فقط، بلا شبكة من خيوط الويب."""
    try:
        return jsonify(data_feed.status_snapshot())
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/btc_trend')
def api_btc_trend():
    """[تحسين V9.11] بوصلة اتجاه BTC — تقرأ الكاش فقط بلا أي نداء شبكي من خيوط الويب."""
    data = _btc_trend_cache['data']
    if not data:
        return jsonify({'status': 'init', 'message': 'جاري أول تحليل للفريمات...'})
    return jsonify(data)

@app.route('/api/leader_map')
def api_leader_map():
    """[تحسين V9.13.0] خريطة القيادة: من يتبع من + سلوك القادة الآن.
    تقرأ الكاش فقط بلا أي نداء شبكي من خيوط الويب (نفس مبدأ V9.10.1)."""
    now = time.time()
    with _leader_data_lock:
        leaders = {sym: {'trend': dict(d.get('trend') or {}),
                         'stale': (now - d.get('ts', 0)) > 3 * LEADER_REFRESH_SEC}
                   for sym, d in _leader_data.items()}
    with _leader_map_lock:
        rows = [{'symbol': sym, 'leader': info.get('leader'), 'corr': info.get('corr'),
                 'correlations': info.get('correlations') or {}, 'is_leader': bool(info.get('is_leader')),
                 'age_sec': int(now - info.get('updated', now)) if info.get('updated') else None}
                for sym, info in _leader_map.items()]
    rows.sort(key=lambda r: (-(r['corr'] if r['corr'] is not None else 0.0), r['symbol']))
    summary: Dict[str, int] = {}
    for r in rows:
        key = r['symbol'] if r['is_leader'] else (r['leader'] or 'independent')
        summary[key] = summary.get(key, 0) + 1
    return jsonify({'enabled': USE_LEADER_FILTER, 'leaders': leaders, 'map': rows[:60],
                    'summary': summary, 'veto_stats': dict(_leader_veto_stats),
                    'settings': {'corr_min': LEADER_CORR_MIN, 'bearish_score': LEADER_BEARISH_SCORE,
                                 'window_candles': LEADER_CORR_WINDOW, 'refresh_sec': LEADER_REFRESH_SEC}})

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

@app.route('/api/strategy_pairs')
def api_strategy_pairs():
    """[V9.17.0] الأزواج المرشحة لكل استراتيجية من آخر دورة مسح — مطابقة النمط السوقي
    بدل الترشيح العشوائي/السيولة: كل استراتيجية تُعرض قائمة أزواجها المطابقة بدرجاتها."""
    try:
        with pair_pools_lock:
            pools = {k: list(v) for k, v in strategy_pair_pools.items()}
            updated = pair_pools_updated_at
        return jsonify({'pools': pools, 'updated_at': updated,
                        'min_score': PAIR_MATCH_MIN_SCORE, 'enabled': PAIR_MATCHING_ENABLED})
    except Exception as api_err:
        logger.error(f"❌ [API أزواج الاستراتيجيات] خطأ: {api_err}", exc_info=True)
        return jsonify({'error': str(api_err)}), 500

@app.route('/api/smart_picks')
def api_smart_picks():
    """[V9.23.0] الترشيح الذكي بالأدلة: العملة المناسبة والاستراتيجية المناسبة الآن —
    برهان باك تيست حي (توقع صافي بعد الرسوم لكل خلية استراتيجية×ريم) لا تكهن.
    best = أقوى خلية مجتازة مع أفضل رموزها الحالية في نفس الريم."""
    try:
        if not EVIDENCE_ENABLED:
            return jsonify({'enabled': False, 'reason_ar': 'محرك الأدلة معطل بالإعدادات (EVIDENCE_ENABLED=false)'})
        with EVIDENCE_STATS_LOCK:
            cells = {f'{k[0]}||{k[1]}': dict(v) for k, v in EVIDENCE_REGIME_STATS.items()}
            pairs = {f'{k[0]}||{k[1]}': dict(v) for k, v in EVIDENCE_PAIR_STATS.items()}
            sym_regime = dict(EVIDENCE_SYMBOL_REGIME)
            updated = EVIDENCE_UPDATED_AT
        if not cells or not updated:
            return jsonify({'enabled': True, 'ready': False,
                            'reason_ar': 'أول تحديث للأدلة جارٍ (يستغرق دقائق بعد الإقلاع) — لا ترشيح بلا برهان',
                            'updated_at': None})
        with signal_cache_lock:
            open_syms = set(open_signals_cache.keys())
        picks = []
        for key, st in cells.items():
            sname, regime = key.split('||')
            if st.get('n', 0) < EVIDENCE_MIN_TRADES:
                continue
            exp_pct, pf = st.get('exp_pct'), st.get('pf')
            if exp_pct is None or exp_pct < EVIDENCE_MIN_EXP_PCT or pf is None or pf < EVIDENCE_MIN_PF:
                continue
            syms_now = [s for s, r in sym_regime.items() if r == regime and s not in open_syms]
            ranked = []
            for s in syms_now:
                pst = pairs.get(f'{sname}||{s}') or {}
                score = (pst.get('exp_pct') or -9.0) * min(pst.get('n', 0), 50)
                ranked.append({'symbol': s, 'pair_n': pst.get('n', 0),
                               'pair_exp_pct': pst.get('exp_pct'), 'pair_pf': pst.get('pf'),
                               '_score': score})
            ranked.sort(key=lambda x: x['_score'], reverse=True)
            for r in ranked[:3]:
                r.pop('_score')
            picks.append({
                'strategy': sname, 'regime': regime,
                'regime_ar': REGIME_AR.get(regime, regime),
                'evidence': st,
                'why_ar': f"الخلية حققت {exp_pct:+.2f}%/صفقة عبر {st['n']} صفقة (PF {pf:.2f}، فوز {st.get('wr', 0):.0f}%) آخر 10 أيام بعد الرسوم",
                'symbols': ranked,
            })
        picks.sort(key=lambda p: -(p['evidence']['exp_pct'] * max(1.0, p['evidence']['pf'])))
        result = {'enabled': True, 'ready': True, 'updated_at': updated,
                  'thresholds': {'min_trades': EVIDENCE_MIN_TRADES,
                                 'min_exp_pct': EVIDENCE_MIN_EXP_PCT, 'min_pf': EVIDENCE_MIN_PF},
                  'picks': picks[:SMART_PICKS_TOP],
                  'best': picks[0] if picks else None}
        if not picks:
            result['reason_ar'] = 'لا خلية (استراتيجية×ريم) مثبتة الربحية الآن — الانتظار أفضل من التكهن'
        return jsonify(result)
    except Exception as api_err:
        logger.error(f"❌ [API الترشيح الذكي] خطأ: {api_err}", exc_info=True)
        return jsonify({'error': str(api_err)}), 500

@app.route('/api/strategy_candidates')
def api_strategy_candidates():
    """[V9.25.0] دائرة الفحص الموسعة: العملات العشر المرشحة لكل استراتيجية (10×عدد
    الاستراتيجيات) — ترشيح حتمي من بصمة ظروف كل استراتيجية على الدائرة الواسعة،
    مع سبب عربي موثق لكل ترشيح وحالة استحقاق الفحص حسب خريطة حالة السوق."""
    try:
        with nominees_lock:
            cand = {k: [dict(n) for n in v] for k, v in STRATEGY_NOMINEES.items()}
            updated = strategy_nominees_updated_at
        with universe_lock:
            wide_n = len(wide_universe_rows)
            deep_n = len(validated_symbols_to_scan)
        with market_state_lock:
            mkt = str(current_market_state.get('overall_regime', 'UNCERTAIN'))
        allowed = MARKET_STATE_STRATEGY_ALLOW.get(mkt)
        display_names = {}
        try:
            with macd_ema_strategy_lock:
                if USE_MACD_EMA_STRATEGY: display_names['MACD_EMA'] = "MACD_EMA_Crossover"
            with bb_stoch_strategy_lock:
                if USE_BB_STOCH_STRATEGY: display_names['BB_STOCH'] = "BB_Stoch_Reversal_Enhanced"
            with ema_rsi_strategy_lock:
                if USE_EMA_RSI_STRATEGY: display_names['EMA_RSI'] = "EMA_RSI_Cross"
            with pullback_strategy_lock:
                if USE_PULLBACK_STRATEGY: display_names['PULLBACK'] = "Pullback_MACD"
            with bb_squeeze_strategy_lock:
                if USE_BB_SQUEEZE_STRATEGY: display_names['BB_SQUEEZE'] = "BB_Squeeze_Breakout"
            with bullish_momentum_strategy_lock:
                if USE_BULLISH_MOMENTUM_STRATEGY: display_names['BULLISH_MOMENTUM'] = "Bullish_Momentum"
            with sr_breakout_strategy_lock:
                if USE_SR_BREAKOUT_STRATEGY: display_names['SR_BREAKOUT'] = "SR_Breakout_Enhanced"
        except Exception:
            display_names = {}
        out = {}
        for k, lst in cand.items():
            out[k] = [{**n, 'strategy_name': display_names.get(k, k),
                       'examinable': (allowed is None or k in allowed)}
                      for n in lst]
        return jsonify({'per_strategy': NOMINEES_PER_STRATEGY,
                        'total': NOMINEES_PER_STRATEGY * len(display_names),
                        'wide_universe': wide_n, 'deep_universe': deep_n,
                        'market_state': mkt, 'updated_at': updated,
                        'candidates': out})
    except Exception as api_err:
        logger.error(f"❌ [API دائرة الترشيح] خطأ: {api_err}", exc_info=True)
        return jsonify({'error': str(api_err)}), 500

@app.route('/api/rejection_logs')
def get_rejection_logs():
    with rejection_logs_lock: return jsonify(list(rejection_logs_cache))

@app.route('/api/rejection_summary')
def api_rejection_summary():
    """[تحسين V9.12.0 + V9.14.0] تحليل أسباب الرفض — الصورة الكاملة منذ الإقلاع.
    يضم: رفضات بوابة العقلانية + فحوصات/نجاحات كل استراتيجية + رفضات الفلتر
    الخاص بكل استراتيجية (ملف فلترة منطقي لكل نمط) + آخر الرفضات المسجلة."""
    try:
        with _scan_stats_lock:
            filters = dict(_filter_reject_stats)
            strategies = {name: dict(c) for name, c in _strategy_scan_stats.items()}
            strategy_filters = {name: dict(c) for name, c in _strategy_filter_stats.items()}
            rec_stats = dict(_recommendation_stats)  # [V9.18.0] إحصاء التوصيات المفتوحة
        with rejection_logs_lock:
            recent = list(rejection_logs_cache)

        total_filter_rejects = sum(filters.values())
        total_checks = sum(s.get('checks', 0) for s in strategies.values())
        total_passes = sum(s.get('passes', 0) for s in strategies.values())

        strategy_rows = []
        for name, s in strategies.items():
            checks = s.get('checks', 0)
            passes = s.get('passes', 0)
            prefilter_rejects = sum(strategy_filters.get(name, {}).values())
            strategy_rows.append({
                'strategy': name, 'checks': checks, 'passes': passes,
                'pass_rate_pct': round(passes / checks * 100, 2) if checks else 0.0,
                'recommendations': s.get('recommendations', 0),  # [V9.18.0] توصيات فلاتر مفتوحة
                'prefilter_rejects': prefilter_rejects,
                'filter_breakdown': strategy_filters.get(name, {})
            })
        strategy_rows.sort(key=lambda r: r['passes'], reverse=True)

        return jsonify({
            'since_boot': True,
            'total_filter_rejects': total_filter_rejects,
            'filters': sorted(filters.items(), key=lambda kv: kv[1], reverse=True),
            'total_strategy_checks': total_checks,
            'total_strategy_passes': total_passes,
            'strategies': strategy_rows,
            'strategy_filters': strategy_filters,
            'recent_rejections_count': len(recent),
            'last_rejection_at': recent[0].get('timestamp') if recent else None,
            # [V9.18.0] وضع التوصيات: العملات المجتازة للفلاتر تُفتح كصفقات مُدارة
            'recommendations': {
                'enabled': bool(RECOMMENDATIONS_ENABLED and PAIR_MATCHING_ENABLED),
                'per_cycle': RECOMMENDATIONS_PER_CYCLE,
                'min_fit_score': RECOMMENDATION_MIN_FIT_SCORE,
                'cooldown_min': RECOMMENDATION_COOLDOWN_MIN,
                **rec_stats,
            },
        })
    except Exception as e:
        logger.error(f"❌ [API ملخص الرفض] خطأ: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/debug_threads')
def api_debug_threads():
    """[تشخيص V9.12.1] مكدس كل خيط لحظيًا — يكشف بالضبط أين علق أي خيط
    (مثلاً: نائم داخل قفل، منتظر شبكة، محسوبًا pandas). للفحص اليدوي فقط."""
    import traceback as _tb
    frames = sys._current_frames()
    out = []
    for t in threading_enumerate():
        entry = {'name': t.name, 'daemon': t.daemon, 'alive': t.is_alive()}
        f = frames.get(t.ident)
        if f:
            try:
                stack = _tb.extract_stack(f)
                entry['stack'] = [f"{fr.filename.split('/')[-1]}:{fr.lineno} in {fr.name}: {fr.line[:110] if fr.line else ''}" for fr in stack[-8:]]
            except Exception as ee:
                entry['stack'] = [f'unavailable: {ee}']
        out.append(entry)
    return jsonify({'thread_count': len(out), 'threads': out})

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
        # [إصلاح V9.12.1] كانت UPDATE على قاعدة البيانات + تليجرام تتم داخل القفل —
        # نفس عائلة خطأ close_signal (احتكار signal_cache_lock أثناء الشبكة/DB)
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

        # العمل الشبكي وقاعدة البيانات — خارج القفل تمامًا
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
    global _last_cache_reconcile
    logger.info("✅ [مدير الصفقات] بدء حلقة إدارة الصفقات...")
    while True:
        try:
            # [إصلاح V9.12.1 — حامل القفل الدائم] كان النوم (5 ثوانٍ) يتم داخل القفل
            # عندما يكون الكاش فارغًا: امسك ← نم 5ث ← حرر ميكروثانية ← امسك فورًا...
            # = احتكار شبه دائم لـ signal_cache_lock يجوّع نقاط اللوحة الثلاث التي تنتظره
            # (كان يبدأ فقط عندما يكون كاش الصفقات فارغًا — مثل فشل تحميله عند إقلاع DB باردة)
            with signal_cache_lock:
                has_signals = bool(open_signals_cache)
                signals_to_check = list(open_signals_cache.values()) if has_signals else []

            if not has_signals or not redis_client:
                time.sleep(5)  # النوم خارج القفل الآن
                # [V9.19.1] مصالحة دورية للكاش الفارغ: إقلاع بارد فشل فيه التحميل
                # الأولي (قاعدة باردة + حظر REST) يترك صفقات مفتوحة يتيمة في قاعدة
                # البيانات — غير مرئية في اللوحة وغير مُدارة. كل 60ث محاولة خفيفة
                # (استعلام واحد) حتى يظهر الكاش أو يثبت أن لا صفقات فعلًا.
                if not has_signals and redis_client and (time.time() - _last_cache_reconcile) >= 60:
                    _last_cache_reconcile = time.time()
                    try:
                        load_open_signals_to_cache(retries=1, delay=0)
                    except Exception as rec_err:
                        logger.warning(f"⚠️ [مصالحة الكاش] فشل محاولة المواءمة: {rec_err}")
                continue

            current_prices = redis_client.hgetall(REDIS_PRICES_HASH_NAME)
            # [V9.19.0] الأسعار الحية من مركز WebSocket أولًا (زمن حقيقي ~2ث، صفر وزن REST)
            # — قبل هذا كان الصفقات خارج القائمة الديناميكية تُهمل تمامًا هنا (لا سعر →
            # لا إدارة → لا وقف خسارة ولا أهداف!) — والآن WS يغطيها لأنها مثبتة في المركز
            if stream_hub is not None and signals_to_check:
                try:
                    hub_live = stream_hub.get_prices([str(s['symbol']).upper() for s in signals_to_check])
                    if hub_live:
                        current_prices = {**current_prices, **hub_live}
                except Exception as hub_price_err:
                    logger.debug(f"[مدير الصفقات] تعذر جلب أسعار WS: {hub_price_err}")
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
                        # [V9.22.0] بوت التفعيل: لا يُرفع الوقف المتحرك قبل أن تبلوغ القمة
                        # حد ربح أدنى (ATR_TRAIL_ACTIVATE_PROFIT_PCT). الرفع من أول قمة
                        # غبارية كان يخنق الرابحين ويضخم الدوران — الباك تيست 60 يومًا:
                        # هذه البوابة + المضاعف 2.8 قلبت الصافي من -85% إلى +181%
                        trail_ok = (ATR_TRAIL_ACTIVATE_PROFIT_PCT <= 0) or \
                                   (new_peak >= entry * (1.0 + ATR_TRAIL_ACTIVATE_PROFIT_PCT / 100.0))
                        if trail_ok:
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


# ===================== [V9.23.0] محرك الأدلة — الترشيح بالبرهان لا بالتكهن =====================
# الفلسفة: درجة المطابقة النمطية (V9.17.0) تقيس "هل البيئة تناسب شخصية الاستراتيجية"
# ولا تقيس "هل هذا المزيج يربح فعلًا بعد التكاليف" — الباك تيست (30 يومًا × 24 رمزًا ×
# 53 ألف حدث بمنطق المنتج حرفيًا) أثبت أن أولها يخسر -1.44U والثاني هو الفارق.
# الحل: إعادة تشغيل دورية (كل EVIDENCE_REFRESH_MIN) لمنطق المنتج كاملًا على شموع
# 15م الأخيرة لكل رمز: مطابقة الريم + الفلاتر الخاصة + المُطلِقات + محرك خروج
# V9.22.0 (جزئية + تمديد + تريلينغ) — ثم تجميع توقع كل خلية (استراتيجية × ريم)
# صافي الرسوم والانزلاق. البوابة: n ≥ 5، توقع ≥ +0.12%/صفقة، PF ≥ 1.15 (مثبت V12).

EVIDENCE_REGIME_STATS: Dict[Tuple[str, str], Dict[str, Any]] = {}
EVIDENCE_PAIR_STATS: Dict[Tuple[str, str], Dict[str, Any]] = {}
EVIDENCE_STATS_LOCK = Lock()
EVIDENCE_UPDATED_AT: Optional[str] = None
EVIDENCE_SYMBOL_REGIME: Dict[str, str] = {}     # آخر ريم لكل رمز من حلقة المسح (للعرض)
# [V9.24.0] كاش سلسلة الريم العام لمحرك الأدلة (من شموع BTC متعددة الفريمات)
_evidence_regime_series_cache: Optional[pd.Series] = None
_evidence_regime_series_at: float = 0.0
EVIDENCE_REFRESH_LOCK = Lock()

# سجل الاستراتيجيات المفعلة (نفس ترتيب الحلقة الرئيسية) — يُبنى مرة
def _evidence_strategy_table() -> List[Tuple[str, Any, str]]:
    return [
        ('MACD_EMA', check_macd_ema_strategy, 'MACD_EMA_Crossover'),
        ('BB_STOCH', check_bb_stoch_strategy_enhanced, 'BB_Stoch_Reversal_Enhanced'),
        ('EMA_RSI', check_ema_rsi_strategy, 'EMA_RSI_Cross'),
        ('PULLBACK', check_pullback_strategy, 'Pullback_MACD'),
        ('BB_SQUEEZE', check_bb_squeeze_strategy, 'BB_Squeeze_Breakout'),
        ('BULLISH_MOMENTUM', check_bullish_momentum_strategy, 'Bullish_Momentum'),
        ('SR_BREAKOUT', check_support_resistance_strategy_enhanced, 'SR_Breakout_Enhanced'),
    ]


def _evidence_summarize(nets: List[float]) -> Dict[str, Any]:
    if not nets:
        return {'n': 0, 'exp_pct': None, 'pf': None, 'wr': None}
    wins = [x for x in nets if x > 0]
    losses = [x for x in nets if x < 0]
    pf = (sum(wins) / abs(sum(losses))) if losses else (99.0 if wins else 0.0)
    return {'n': len(nets), 'exp_pct': round(sum(nets) / len(nets), 4),
            'pf': round(min(pf, 99.0), 3), 'wr': round(100.0 * len(wins) / len(nets), 1)}


def _evidence_simulate_exit(df: pd.DataFrame, sig_i: int, atr_sig: float, n: int) -> Optional[float]:
    """محاكاة خروج وفية لمحرك V9.22.0 (نفسها الباك تيست الخارجي):
    SL=2.5×ATR (×1.2 تقلب عالٍ / ×0.8 هادئ)، TP=4×ATR بنفس التعديل، جزئية 40%
    (60% إذا RR≥2) عند الهدف مع نقل الوقف إليه، تمديد +1.25×ATR، تريلينغ ببوابة
    +1.5% ومضاعف 2.8 من القمة. الدخول عند open الشمعة التالية (واقعي).
    تبسيطان موثّقان: الهدف الممتد = TP+1.25×ATR بلا مسح مقاومات، وأقصى احتفاظ 48 ساعة.
    يعيد الصافي % بعد رسوم جانبين + انزلاق، أو None إن تعذر."""
    try:
        e_i = sig_i + 1
        if e_i >= n:
            return None
        entry = float(df['open'].iloc[e_i])
        atr = float(atr_sig)
        if not (entry > 0 and atr > 0):
            return None
        atr_pct = (atr / entry) * 100.0
        slm, tpm = 2.5, 4.0
        if atr_pct > 3.0:
            slm *= 1.2
            tpm *= 1.2
        elif atr_pct < 1.0:
            slm *= 0.8
            tpm *= 0.8
        sl_d, tp_d = atr * slm, atr * tpm
        sl, tp = entry - sl_d, entry + tp_d
        rr = tp_d / sl_d if sl_d > 0 else 0.0
        frac = 0.6 if rr >= 2.0 else 0.4
        tp2 = entry + tp_d + 1.25 * atr
        peak, sl_act, partial_done = entry, sl, False
        fills: List[Tuple[float, float]] = []
        end_j = min(n, e_i + 192)
        if end_j <= e_i:
            return None
        for j in range(e_i, end_j):
            lo = float(df['low'].iloc[j])
            hi = float(df['high'].iloc[j])
            if lo <= sl_act:
                fills.append((sl_act, 1.0 - sum(f for _, f in fills)))
                break
            if not partial_done and hi >= tp:
                fills.append((tp, frac))
                partial_done = True
                sl_act = max(sl_act, tp)
                if hi >= tp2:
                    fills.append((tp2, 1.0 - sum(f for _, f in fills)))
                    break
                if lo <= sl_act:
                    fills.append((sl_act, 1.0 - sum(f for _, f in fills)))
                    break
            elif partial_done and hi >= tp2:
                fills.append((tp2, 1.0 - sum(f for _, f in fills)))
                break
            if hi > peak:
                peak = hi
            if (ATR_TRAIL_ACTIVATE_PROFIT_PCT <= 0) or (peak >= entry * (1.0 + ATR_TRAIL_ACTIVATE_PROFIT_PCT / 100.0)):
                a = float(df['atr'].iloc[j])
                if a and a > 0:
                    cand = peak - a * ATR_TS_MULTIPLIER
                    if cand > sl_act:
                        sl_act = cand
        else:
            px = float(df['close'].iloc[end_j - 1])
            fills.append((px, 1.0 - sum(f for _, f in fills)))
        if not fills:
            return None
        gross = sum(p * f for p, f in fills) / entry - 1.0
        return gross * 100.0 - 2.0 * (EVIDENCE_FEE_PCT + EVIDENCE_SLIP_PCT)
    except Exception:
        return None


def _evidence_global_regime_series() -> Optional[pd.Series]:
    """[V9.24.0] سلسلة زمنية لحالة السوق العام (نفس منطق determine_market_state_enhanced)
    لكل شمعة BTC 15م ضمن نافذة الدليل: EMA12/26 + ADX على فريمات 15م/1س/4س → تصويت الأغلبية.
    تُخزَّن مؤقتًا بين تحديثات محرك الأدلة (تتغير الحالة ببطء) لتفادي جلب متكرر.
    تُستخدم في _evidence_replay_symbol لفرض خريطة MARKET_STATE_STRATEGY_ALLOW تاريخيًا."""
    global _evidence_regime_series_cache, _evidence_regime_series_at
    try:
        if (_evidence_regime_series_cache is not None and _evidence_regime_series_at
                and (time.time() - _evidence_regime_series_at) < EVIDENCE_REFRESH_MIN * 60 * 0.9):
            return _evidence_regime_series_cache

        def _trend_labels(df: pd.DataFrame) -> Optional[pd.Series]:
            if df is None or len(df) < 60:
                return None
            ema_fast = df['close'].ewm(span=12, adjust=False).mean()
            ema_slow = df['close'].ewm(span=26, adjust=False).mean()
            feat = calculate_all_features(df.copy(), None)
            adx = feat['adx'] if not feat.empty else pd.Series(np.nan, index=df.index)
            up = (ema_fast > ema_slow).reindex(df.index)
            strong = (adx > 25).fillna(False).reindex(df.index)
            labels = pd.Series('Ranging', index=df.index)
            labels[up & strong] = 'Strong Uptrend'
            labels[up & ~strong] = 'Uptrend'
            labels[~up & strong] = 'Strong Downtrend'
            labels[~up & ~strong] = 'Downtrend'
            return labels

        btc15 = fetch_historical_data(BTC_SYMBOL, '15m', SIGNAL_GENERATION_LOOKBACK_DAYS)
        base = _trend_labels(btc15)
        if base is None:
            return None
        parts = [base.rename('15m')]
        for tf in ('1h', '4h'):
            b = _trend_labels(fetch_historical_data(BTC_SYMBOL, tf, SIGNAL_GENERATION_LOOKBACK_DAYS))
            if b is not None:
                parts.append(b.reindex(base.index, method='ffill').rename(tf))
        votes = pd.concat(parts, axis=1)
        # تصويت الأغلبية لكل شمعة (نفس روح max(set(trends), key=count) الحية)
        def _vote(row: pd.Series) -> str:
            vals = [v for v in row.tolist() if isinstance(v, str) and v != 'Ranging']
            if not vals:
                return 'Uncertain'
            return max(set(vals), key=vals.count)
        series = votes.apply(_vote, axis=1)
        with EVIDENCE_STATS_LOCK:
            _evidence_regime_series_cache = series
            _evidence_regime_series_at = time.time()
        return series
    except Exception as reg_err:
        logger.warning(f"⚠️ [محرك الأدلة] فشل سلسلة الريم العام: {reg_err}")
        return None


def _evidence_replay_symbol(symbol: str, btc_df: Optional[pd.DataFrame]) -> List[Tuple[str, str, float]]:
    """إعادة تشغيل منطق المنتج على نافذة الدليل لرمز واحد → قائمة (استراتيجية، ريم، صافي%).
    [V9.24.0] تشمل إعادة التشغيل: خريطة السوق العام → الاستراتيجيات المسموحة +
    كواشف التجهيز الشرطي — الأدلة تُحسب من نفس منطق الإنتاج الجديد لا من منطق قديم."""
    out: List[Tuple[str, str, float]] = []
    df = fetch_historical_data(symbol, SIGNAL_GENERATION_TIMEFRAME, SIGNAL_GENERATION_LOOKBACK_DAYS)
    if df is None or len(df) < 300:
        return out
    df_feat = calculate_all_features(df, btc_df)
    if df_feat.empty:
        return out
    n = len(df_feat)
    start = max(230, n - EVIDENCE_WINDOW_BARS)
    stop = n - 4
    table = _evidence_strategy_table()
    last_sig: Dict[str, int] = {name: -10 ** 9 for _, _, name in table}
    # [V9.24.0] سلسلة حالة السوق العام (من BTC) محاذاة زمنيًا إلى شموع الرمز + ثبات الصاعد
    regime_series = _evidence_global_regime_series()
    up_persist = None
    if regime_series is not None:
        regime_series = regime_series.reindex(df_feat.index, method='ffill')
        fam = regime_series.astype(str).str.upper().str.replace(' ', '_', regex=False).map(
            lambda r: 'UP' if 'UPTREND' in r else ('DOWN' if 'DOWNTREND' in r else 'FLAT'))
        # دقائق الثبات الصاعدي المتصل عند كل شمعة (15 دقيقة/شمعة)
        grp = (fam != 'UP').cumsum()
        up_persist = fam.eq('UP').groupby(grp).cumsum() * 15.0
    for i in range(start, stop, EVIDENCE_STRIDE):
        atr_i = float(df_feat['atr'].iloc[i]) if 'atr' in df_feat else 0.0
        if not (atr_i > 0):
            continue
        win = df_feat.iloc[max(0, i - 400 + 1): i + 1]
        win.name = symbol
        if len(win) < 60 or not passes_market_sanity_filter(win):
            continue
        ri = compute_symbol_regime(win)
        if not ri:
            continue
        regime = str(ri.get('regime'))
        # [V9.24.0] حالة السوق العامة عند هذه الشمعة (رسملة متوافقة مع الخريطة)
        mkt = 'UNCERTAIN'
        if regime_series is not None and i < len(regime_series):
            mkt = str(regime_series.iloc[i]).upper().replace(' ', '_')
        allowed_keys = MARKET_STATE_STRATEGY_ALLOW.get(mkt)
        up_min = float(up_persist.iloc[i]) if up_persist is not None and i < len(up_persist) else 0.0
        for key, fn, name in table:
            if i - last_sig[name] < EVIDENCE_ENTRY_SPACING:
                continue
            if allowed_keys is not None and key not in allowed_keys:
                continue
            # [V9.24.0] فلتر الثبات: الاستراتيجيات الاستمرارية بعد ثبات الصاعد فقط
            if key in TREND_CONTINUATION_KEYS and up_min < MARKET_REGIME_PERSIST_MIN:
                continue
            fit = score_strategy_pair_fit(ri, name)
            if fit is None or fit < PAIR_MATCH_MIN_SCORE:
                continue
            if not passes_strategy_prefilters(win, name):
                continue
            # [V9.24.0] بوابة التجهيز الشرطي — نفس منطق حلقة المسح
            scan_fn = STRATEGY_SETUP_SCANNERS.get(key)
            if scan_fn is not None and not scan_fn(win, mkt):
                continue
            if not fn(win):
                continue
            net = _evidence_simulate_exit(df_feat, i, atr_i, n)
            if net is not None:
                out.append((name, regime, float(net)))
                last_sig[name] = i + EVIDENCE_ENTRY_SPACING
    return out


def _refresh_evidence_engine() -> Dict[str, Any]:
    """تحديث كامل لأدلة الخلايا والأزواج — يعيد ملخصًا للتشخيص واللوحة."""
    global EVIDENCE_REGIME_STATS, EVIDENCE_PAIR_STATS, EVIDENCE_UPDATED_AT
    with EVIDENCE_REFRESH_LOCK:
        if _evidence_replaying:
            return {'skipped': 'already_running'}
        globals()['_evidence_replaying'] = True
        t0 = time.time()
        try:
            syms = list(validated_symbols_to_scan)[:EVIDENCE_MAX_SYMBOLS]
            if not syms:
                return {'skipped': 'empty_universe'}
            btc_df = get_btc_data_for_bot()
            cells: Dict[Tuple[str, str], List[float]] = {}
            pairs: Dict[Tuple[str, str], List[float]] = {}
            done = 0
            for sym in syms:
                try:
                    trades = _evidence_replay_symbol(sym, btc_df)
                    for (sname, regime, net) in trades:
                        cells.setdefault((sname, regime), []).append(net)
                        pairs.setdefault((sname, sym), []).append(net)
                    done += 1
                except Exception as sym_err:
                    logger.warning(f"🧪 [محرك الأدلة] تخطي {sym}: {sym_err}")
            with EVIDENCE_STATS_LOCK:
                EVIDENCE_REGIME_STATS = {k: _evidence_summarize(v) for k, v in cells.items()}
                EVIDENCE_PAIR_STATS = {k: _evidence_summarize(v) for k, v in pairs.items()}
                globals()['EVIDENCE_UPDATED_AT'] = datetime.now(timezone.utc).isoformat()
            passing = [k for k, st in EVIDENCE_REGIME_STATS.items()
                       if st.get('n', 0) >= EVIDENCE_MIN_TRADES
                       and (st.get('exp_pct') or -9) >= EVIDENCE_MIN_EXP_PCT
                       and (st.get('pf') or 0) >= EVIDENCE_MIN_PF]
            summary = {'symbols': done, 'cells': len(EVIDENCE_REGIME_STATS),
                       'passing_cells': len(passing), 'sec': round(time.time() - t0, 1)}
            logger.info(f"🧪 [محرك الأدلة] تحديث: {summary}")
            return summary
        finally:
            globals()['_evidence_replaying'] = False


def evidence_gate_pass(strategy_name: str, regime: Optional[str]) -> Tuple[bool, Dict[str, Any]]:
    """بوابة الدليل: (اجتياز؟، تفاصيل). تعطل المحرك = اجتياز دائم (سلوك قديم).
    بلا دليل كافٍ أو توقع دون العتبة = رفض (fail-closed) — لا صفقات بلا برهان.
    [V9.24.0] تدرج العينات الصغيرة: الخلايا الحديثة (3-4 صفقات) لا تُحجب كليًا وإلا
    بقيت الخلايا الجديدة (مثل قاع-صيد الارتداد) بلا فرصة إثبات حي — لكن تُشترط لها
    عتبة أعلى صرامة (exp ≥ +0.30% و PF ≥ 1.40) عوضًا عن عتبات العينة الناضجة (n≥5)."""
    if not EVIDENCE_ENABLED:
        return True, {'engine': 'disabled'}
    if not regime:
        return False, {'reason_ar': 'لا يوجد نمط سوقي محسوم — لا دليل', 'n': 0}
    with EVIDENCE_STATS_LOCK:
        st = dict(EVIDENCE_REGIME_STATS.get((strategy_name, str(regime))) or {})
    n, exp_pct, pf = st.get('n', 0), st.get('exp_pct'), st.get('pf')
    # [V9.24.0] عتبات متدرجة حسب حجم العينة — كلها أدنى من عتبة ن>=5 (أكثر صرامة)
    small_sample = 3 <= n < EVIDENCE_MIN_TRADES
    min_exp = EVIDENCE_MIN_EXP_PCT_SMALL if small_sample else EVIDENCE_MIN_EXP_PCT
    min_pf = EVIDENCE_MIN_PF_SMALL if small_sample else EVIDENCE_MIN_PF
    info = {'strategy': strategy_name, 'regime': regime,
            'regime_ar': REGIME_AR.get(str(regime), str(regime)),
            'n': n, 'exp_pct': exp_pct, 'pf': pf,
            'min_n': EVIDENCE_MIN_TRADES, 'min_exp_pct': min_exp,
            'min_pf': min_pf, 'small_sample': small_sample, 'updated_at': EVIDENCE_UPDATED_AT}
    if n < 3:
        info['reason_ar'] = f'أدلة غير كافية ({n} صفقة < 3)'
        return False, info
    if exp_pct is None or exp_pct < min_exp or pf is None or pf < min_pf:
        info['reason_ar'] = f'التوقع التاريخي سلبي أو دون العتبة ({(exp_pct or 0):+.2f}%/صفقة، PF {pf or 0:.2f}؛ العتبات: {min_exp:+.2f}% / {min_pf:.2f})'
        return False, info
    info['verdict_ar'] = f"مثبت ربحيًا: {exp_pct:+.2f}%/صفقة عبر {n} صفقة (PF {pf:.2f}) آخر 10 أيام"
    return True, info


def evidence_engine_loop():
    """خيط خلفي: تحديث أولي بعد تدفئة WS ثم دوري كل EVIDENCE_REFRESH_MIN."""
    time.sleep(150)  # مهلة تدفئة مركز WS وتحميل الكون الديناميكي
    while True:
        try:
            _refresh_evidence_engine()
        except Exception as ev_err:
            logger.error(f"❌ [محرك الأدلة] فشل التحديث: {ev_err}", exc_info=True)
        time.sleep(max(900, EVIDENCE_REFRESH_MIN * 60))


def main_loop_enhanced():
    global strategy_pair_pools, pair_pools_updated_at
    logger.info("[الحلقة الرئيسية] انتظار اكتمال التهيئة...")
    time.sleep(15)
    if not validated_symbols_to_scan:
        log_and_notify("critical", "قائمة العملات للمسح فارغة. يرجى التحقق من ملف 'crypto_list.txt'.", "SYSTEM_ERROR")
        return
    log_and_notify("info", f"✅ بدء حلقة المسح لـ {len(validated_symbols_to_scan)} عملة.", "SYSTEM")

    while True:
        try:
            logger.info("🔄 [الحلقة الرئيسية] بدء دورة مسح جديدة...")

            # [V9.21.0] حظر باينانس لم يعد يوقف المسح: كل بيانات الدورة (شموع/تكه/عمق/بوصلة)
            # من مركز WebSocket والمزودين البديلين وهي محصنة ضد الحظر. يُؤجَّل التنفيذ الحقيقي
            # فقط (أوامر باينانس) وتُؤخذ أسعار الدخول من مصادر بديلة — انظر get_entry_price_ban_aware.
            ban_remain = rate_guard.banned_until - time.time()
            exec_blocked = ban_remain > 0
            if exec_blocked and not SCAN_DURING_BAN:
                logger.warning(f"🚫 [الحلقة الرئيسية] حظر API ساري — الانتظار {int(ban_remain)} ثانية... (SCAN_DURING_BAN=False)")
                time.sleep(min(ban_remain, 60.0))
                continue
            if exec_blocked:
                logger.warning(f"🚫 [الحلقة الرئيسية] حظر باينانس ساري ({int(ban_remain)}ث) — المسح مستمر عبر التغذية البديلة والتنفيذ الحقيقي مؤجل")

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
            # [V9.24.0] حالة السوق العام لهذه الدورة — أساس خريطة الاستراتيجيات المسموحة
            with market_state_lock:
                overall_market_regime = str(current_market_state.get('overall_regime', 'UNCERTAIN'))
            # [V9.25.0] دائرة الترشيح الموسعة — طلب المستخدم: "ترشح 10 عملات للفحص مناسبة
            # لكل استراتيجية اي العدد الكلي 10×عدد الاستراتيجيات". الفحص أصبح استراتيجيًا:
            # كل استراتيجية تفحص عملاتها العشر المرشحة حصرًا — المُرشَّحون هم الأعلى
            # تطابقًا لبصمة ظروفها الرقمية (تكه 24س المجاني على الدائرة الواسعة 120)
            # بدل فحص كل عملة مع كل استراتيجية (2044 فحصًا بصفر اجتياز في السجل الحي).
            strategy_nominees_map = nominate_strategy_candidates()
            try:
                if stream_hub is not None:
                    _union_syms = list(validated_symbols_to_scan) + [
                        n['symbol'] for _lst in strategy_nominees_map.values() for n in _lst]
                    stream_hub.set_universe(sorted(set(_union_syms)))
            except Exception:
                pass
            # [V9.17.0] ترشيحات هذه الدورة: الأزواج المطابقة لكل استراتيجية (تُنشر للوحة آخر الدورة)
            cycle_pair_scores: Dict[str, List[Dict[str, Any]]] = {}
            # [V9.18.0] رصيد توصيات الفلاتر لهذه الدورة
            recommendations_opened_this_cycle = 0
            # [V9.25.0] كاش شموع الدورة: رمز قد يرشحه أكثر من باب استراتيجي — الشموع تُجلب مرة واحدة
            df_cycle_cache: Dict[str, Optional[pd.DataFrame]] = {}
            examinations_total = 0

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

            # [V9.25.0] الحلقة استراتيجيةً: كل استراتيجية تفحص مرشحيها العشرة حصرًا.
            # بوابات مستوى الاستراتيجية (حالة السوق + ثبات الصاعد) قبل جلب أي شموع.
            for key, check_func, name in strategies_to_check:
                # [V9.24.0] خريطة السوق العام (حتمية): في الهبوط الحاد لا زخم ولا اختراق
                # صاعد ضد الاتجاه — فقط قاع-صيد الارتداد واختراق الدعوم.
                allowed_keys = MARKET_STATE_STRATEGY_ALLOW.get(overall_market_regime)
                if allowed_keys is not None and key not in allowed_keys:
                    _count_strategy_filter_reject(name, f"ممنوعة في حالة السوق العامة ({overall_market_regime})")
                    continue
                # [V9.24.0] فلتر الثبات: الاستراتيجيات الاستمرارية لا تشتري إلا بعد
                # ثبات عائلة الصاعد MARKET_REGIME_PERSIST_MIN دقيقة.
                if key in TREND_CONTINUATION_KEYS:
                    up_min = market_up_persistence_minutes()
                    if up_min < MARKET_REGIME_PERSIST_MIN:
                        _count_strategy_filter_reject(
                            name, f"ثبات الصاعد غير كافٍ ({up_min:.0f}د < {MARKET_REGIME_PERSIST_MIN}د)")
                        continue
                # [V9.25.0] عملات هذه الاستراتيجية العشر المرشحة حصرًا — دائرة الفحص الموسعة
                with nominees_lock:
                    nominees = [dict(n) for n in (strategy_nominees_map.get(key) or [])][:NOMINEES_PER_STRATEGY]
                with _scan_stats_lock: _strategy_scan_stats[name]['nominated'] = len(nominees)
                if not nominees:
                    continue
                examinations_total += len(nominees)
                logger.info(f"🎯 [{name}] فحص {len(nominees)} عملة مرشحة حصرًا: {[n['symbol'] for n in nominees]}")

                for nom in nominees:
                    symbol = nom['symbol']
                    try:
                        with signal_cache_lock:
                            if symbol in open_signals_cache or len(open_signals_cache) >= MAX_OPEN_TRADES:
                                continue
                        # شموع الرمز — كاش الدورة (أبواب متعددة قد ترشح نفس الرمز: جلب واحد)
                        if symbol in df_cycle_cache:
                            df_15m = df_cycle_cache[symbol]
                        else:
                            df_15m = fetch_historical_data(symbol, SIGNAL_GENERATION_TIMEFRAME, SIGNAL_GENERATION_LOOKBACK_DAYS)
                            df_cycle_cache[symbol] = df_15m
                        if df_15m is None or len(df_15m) < 100:
                            continue
                        # [تحسين V9.13.0] تصنيف القائد (BTC/ETH/SOL) بلا أي نداء شبكي
                        update_leader_classification(symbol, df_15m['close'].tolist())
                        df_with_indicators = calculate_all_features(df_15m, btc_data)
                        df_with_indicators.name = symbol
                        if df_with_indicators.empty:
                            continue
                        # --- [V9.14.0] بوابة العقلانية العامة فقط (واسعة جدًا) ---
                        if not passes_market_sanity_filter(df_with_indicators):
                            with _scan_stats_lock: _filter_reject_stats['بوابة العقلانية العامة'] += 1
                            continue
                        # [V9.17.0] نمط الزوج السوقي — من نفس الشموع بلا وزن شبكي
                        regime_info = compute_symbol_regime(df_with_indicators)
                        # [V9.23.0] سجل ريم الرمز الحالي — يغذي /api/smart_picks
                        if regime_info:
                            EVIDENCE_SYMBOL_REGIME[symbol] = str(regime_info.get('regime'))

                        signal_found, strategy_used = False, None
                        # [V9.18.0] مصدر الإشارة ودرجة المطابقة (للعرض والتوثيق)
                        signal_source, signal_fit_score = 'strategy_trigger', None
                        # [V9.23.0] تفاصيل دليل الإشارة المقبولة (تُوثق في التفاصيل واللوحة)
                        signal_evidence_info: Optional[Dict[str, Any]] = None
                        with _scan_stats_lock: _strategy_scan_stats[name]['checks'] += 1

                        # [V9.17.0] بوابة مطابقة الزوج: نمط السوقي يطابق شخصية الاستراتيجية
                        fit_score: Optional[float] = None
                        if PAIR_MATCHING_ENABLED:
                            fit_score = score_strategy_pair_fit(regime_info, name)
                            if fit_score is None or fit_score < PAIR_MATCH_MIN_SCORE:
                                _count_strategy_filter_reject(
                                    name,
                                    f"الزوج خارج نمط الاستراتيجية ({REGIME_AR.get(regime_info['regime'], 'غير محسوم')})" if regime_info else 'الزوج خارج نمط الاستراتيجية (نمط غير محسوم)')
                                continue
                            cycle_pair_scores.setdefault(name, []).append(
                                {'symbol': symbol, 'score': fit_score,
                                 'regime': regime_info['regime'],
                                 'regime_ar': REGIME_AR.get(regime_info['regime'], regime_info['regime'])})
                        # [V9.14.0] فلتر الاستراتيجية الخاص (ملف منطقي لكل نمط)
                        if not passes_strategy_prefilters(df_with_indicators, name):
                            continue
                        # [V9.24.0] كاشف التجهيز الشرطي لكل استراتيجية: لا فحص ولا توصية
                        # إلا على رموز تجسدت ظروفها فيها بأرقام موثقة
                        scan_fn = STRATEGY_SETUP_SCANNERS.get(key)
                        if scan_fn is not None:
                            setup_ok, setup_ev = scan_fn(df_with_indicators, overall_market_regime)
                            if not setup_ok:
                                _count_strategy_filter_reject(name, f"التجهيز الشرطي غير متحقق ({setup_ev.get('reason', '')})")
                                continue
                        else:
                            setup_ok, setup_ev = True, {}
                        if check_func(df_with_indicators):
                            # [V9.23.0] بوابة الأدلة على المُطلقات أيضًا — لا إشارة بلا برهان
                            ok_ev, ev_info = evidence_gate_pass(name, (regime_info or {}).get('regime'))
                            if not ok_ev:
                                with _scan_stats_lock: _recommendation_stats['evidence_rejected'] += 1
                                log_rejection(symbol, "بوابة الأدلة رفضت إشارة استراتيجية", {'strategy': name, **ev_info})
                                continue
                            with _scan_stats_lock: _strategy_scan_stats[name]['passes'] += 1
                            signal_found, strategy_used = True, name
                            signal_fit_score = fit_score
                            # [V9.24.0] دليل الإشارة = أدلة التجهيز الشرطي + أدلة الخلية التاريخية
                            signal_evidence_info = {**setup_ev, **ev_info}
                        # [V9.24.0→V9.25.0] التجهيز الشرطي متحقق (مطابقة + فلاتر + كاشف) دون
                        # اكتمال مُطلق الشمعة → توصية موثقة بأدلة رقمية لهذه الاستراتيجية تحديدًا
                        if (not signal_found and setup_ok and RECOMMENDATIONS_ENABLED and PAIR_MATCHING_ENABLED):
                            if recommendations_opened_this_cycle >= RECOMMENDATIONS_PER_CYCLE:
                                pass  # نفد رصيد الدورة — تبقى الترشيحات معروضة في /api/strategy_pairs
                            else:
                                rec_score = float(fit_score) if fit_score is not None else 50.0
                                if rec_score < RECOMMENDATION_MIN_FIT_SCORE:
                                    with _scan_stats_lock: _recommendation_stats['below_min_score'] += 1
                                elif _symbol_recently_closed(symbol):
                                    with _scan_stats_lock: _recommendation_stats['cooldown_skipped'] += 1
                                    logger.info(f"⏳ [{symbol}] ضمن تهدئة ما بعد الإغلاق — لا توصية جديدة الآن")
                                else:
                                    # [V9.23.0] بوابة الأدلة: لا توصية بلا برهان ربحي تاريخي للخلية
                                    ok_ev, rec_evidence_info = evidence_gate_pass(name, (regime_info or {}).get('regime'))
                                    if not ok_ev:
                                        with _scan_stats_lock: _recommendation_stats['evidence_rejected'] += 1
                                        log_rejection(symbol, "بوابة الأدلة رفضت التوصية", {'strategy': name, **rec_evidence_info})
                                        continue
                                    signal_found, strategy_used = True, name
                                    signal_source, signal_fit_score = 'filter_recommendation', rec_score
                                    # [V9.24.0] توثيق كامل: أدلة التجهيز الشرطي + أدلة الخلية التاريخية
                                    signal_evidence_info = {**setup_ev, **rec_evidence_info}
                                    logger.info(f"  -> [{symbol}] 💡 تجهيز شرطي لـ {name} (مطابقة {rec_score:.0f} + دليل: {rec_evidence_info.get('verdict_ar', '')}) → توصية شراء موثقة")

                        if not signal_found:
                            continue

                        logger.info(f"  -> [{symbol}] {'💡 توصية تجهيز شرطي' if signal_source == 'filter_recommendation' else 'إشارة ناجحة'} من {strategy_used}. جاري التحقق النهائي...")

                        # --- [تحسين V9.13.0] بوابة سلوك القائد: لا شراء تابع مقابل قائد هابط ---
                        # [V9.18.0] تُطبق على التوصيات والإشارات معًا — نقيض القائد نقض للاثنين
                        leader_ok, leader_info = passes_leader_behavior_filter(symbol)
                        if not leader_ok:
                            li = leader_info or {}
                            if signal_source == 'filter_recommendation':
                                with _scan_stats_lock: _recommendation_stats['gate_rejected'] += 1
                            log_rejection(symbol, "Leader Behavior Veto", {
                                'leader': li.get('leader'), 'corr': li.get('corr'),
                                'leader_trend_score': li.get('leader_trend_score')})
                            continue

                        # [V9.21.0] سعر الدخول بلا تعلّق أثناء الحظر: باينانس ← مركز WS ← البدائل
                        entry_price = get_entry_price_ban_aware(symbol, exec_blocked=exec_blocked)
                        if not entry_price:
                            logger.error(f"❌ [{symbol}] فشل جلب سعر الدخول من كل المصادر.")
                            continue

                        # --- [تحسين V9.8] فلاتر تأكيد مستوى الإشارة ---
                        # [V9.18.0] فلاتر توقيت الدخول (HTF/القمة/الزخم القصير) خاصة بإشارات
                        # المُطلِقات — التوصية فعلها هو حكم الفلاتر نفسها، فتكفي بوابات:
                        # القائد + دفتر الطلبات + قابلية حساب الهدف/الوقف
                        if signal_source != 'filter_recommendation':
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
                            if signal_source == 'filter_recommendation':
                                with _scan_stats_lock: _recommendation_stats['gate_rejected'] += 1
                            continue

                        logger.info(f"  -> [{symbol}] ✅ نجح فلتر دفتر الطلبات. جاري تحضير الصفقة...")
                        tp_sl_data = calculate_dynamic_tp_sl(df_with_indicators, entry_price)
                        if not tp_sl_data: continue

                        new_signal = {
                            'symbol': symbol, 'strategy_name': strategy_used,
                            'signal_details': {**tp_sl_data},
                            'entry_price': entry_price, **tp_sl_data
                        }
                        # [V9.18.0] توثيق مصدر الإشارة (مُطلِق استراتيجية / اجتياز فلاتر) ودرجة
                        # المطابقة والنمط السوقي — للعرض في اللوحة والتليجرام والتحليل اللاحق
                        new_signal['signal_details']['source'] = signal_source
                        if signal_fit_score is not None:
                            new_signal['signal_details']['fit_score'] = round(float(signal_fit_score), 1)
                        # [V9.23.0] توثيق دليل الادعاء الربحي مع الإشارة (لوحة/تليجرام/تحليل لاحق)
                        if signal_evidence_info is not None:
                            new_signal['signal_details']['evidence'] = signal_evidence_info
                        if regime_info:
                            new_signal['signal_details']['regime'] = regime_info.get('regime')
                            new_signal['signal_details']['regime_ar'] = REGIME_AR.get(regime_info.get('regime'), regime_info.get('regime'))
                        # [تحسين V9.13.0] توثيق القائد التابع له داخل تفاصيل الإشارة (تليجرام/لوحة)
                        if leader_info and leader_info.get('leader'):
                            new_signal['signal_details']['leader_info'] = {
                                k: leader_info.get(k) for k in ('leader', 'corr', 'leader_trend_score', 'leader_trend_label')}

                        with trading_status_lock: is_enabled = is_trading_enabled
                        if is_enabled and exec_blocked:
                            # [V9.21.0] التنفيذ الحقيقي ينتظر انتهاء الحظر — ولا صفقة ورقية
                            # توهم موقعًا غير موجود على منصة التنفيذ
                            log_rejection(symbol, "Execution Deferred (Binance Ban)", {'ban_remain_sec': int(ban_remain)})
                            continue
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
                            # [V9.19.0] تثبيت فوري لتدفق WS للرمز المفتوح حديثًا — بدل انتظار
                            # دورة الصيانة (≤60ث) كان أول دقيقة من الصفقة بلا سعر حي
                            if stream_hub is not None:
                                try: stream_hub.set_universe(validated_symbols_to_scan)
                                except Exception: pass
                            if signal_source == 'filter_recommendation':
                                # [V9.18.0] محاسبة التوصية المفتوحة (كليًا ولكل استراتيجية)
                                with _scan_stats_lock:
                                    _recommendation_stats['opened'] += 1
                                    _strategy_scan_stats[strategy_used]['recommendations'] += 1
                                recommendations_opened_this_cycle += 1
                            log_and_notify('info', f"إشارة: إشارة شراء جديدة لـ {symbol} من استراتيجية {strategy_used}", "NEW_SIGNAL")

                    except Exception as e:
                        logger.error(f"❌ [خطأ معالجة] للرمز {symbol}: {e}", exc_info=True)
                    finally:
                        time.sleep(0.2)

            # [V9.25.0] تحرير كاش شموع الدورة بعد اكتمال فحص كل المرشحين
            gc.collect()
            df_cycle_cache.clear()

            # [V9.17.0] نشر ترشيحات هذه الدورة للوحة: أفضل الأزواج المطابقة لكل استراتيجية
            try:
                published: Dict[str, List[Dict[str, Any]]] = {}
                for sname, recs in cycle_pair_scores.items():
                    published[sname] = sorted(recs, key=lambda r: r['score'], reverse=True)[:PAIR_POOL_DISPLAY_SIZE]
                with pair_pools_lock:
                    strategy_pair_pools = published
                    pair_pools_updated_at = datetime.now(timezone.utc).isoformat()
            except Exception as pool_err:
                logger.warning(f"⚠️ [مطابقة الأزواج] فشل نشر الترشيحات: {pool_err}")

            _, session_liquidity, _ = get_session_state()
            sleep_duration = 45 if session_liquidity == 'HIGH_LIQUIDITY' else 60
            logger.info(f"✅ [نهاية الدورة] انتهت دورة المسح الكاملة. الانتظار {sleep_duration} ثانية...")
            time.sleep(sleep_duration)

        except (KeyboardInterrupt, SystemExit):
            log_and_notify("info", "إيقاف البوت.", "SYSTEM"); break
        except Exception as main_err:
            log_and_notify("error", f"خطأ حرج في الحلقة الرئيسية: {main_err}", "SYSTEM"); time.sleep(120)

def collect_price_symbols() -> List[str]:
    """[V9.19.0] رموز النشر السعري: القائمة الديناميكية + الصفقات المفتوحة + القادة.
    الجذر الحي للجمود: حلقة الأسعار كانت تنشر للقائمة الديناميكية فقط (20 رمزًا)
    — أي صفقة مفتوحة خرجت من القائمة بعد تحديث الترشيح (كل 30د) كان سعرها
    يتجمد في اللوحة وتهملها إدارة الصفقات تمامًا (لا TP/SL ولا وقف متحرك).
    الصفقات المثبتة في مركز WS أصلًا (pinned) — الآن تُنشر أسعارها كذلك.
    [V9.19.1] القادة (BTC/ETH/SOL) مثبتة منذ الإقلاع — بوصلة BTC والسعر العلوي
    يعملان حتى أثناء حظر وقبل تحميل القائمة الديناميكية."""
    syms = {str(s).upper() for s in (validated_symbols_to_scan or [])}
    syms |= {str(s).upper() for s in LEADER_SYMBOLS}
    try:
        with signal_cache_lock:
            syms |= {str(s).upper() for s in open_signals_cache.keys()}
    except Exception:
        pass
    return list(syms)

def price_update_loop():
    if not redis_client: return
    while True:
        try:
            # [V9.19.0] لا تفادي كامل للحلقة أثناء حظر REST بعد الآن: أسعار مركز
            # WebSocket حية ومجانية وتعمل أثناء الحظر (لاحظ الحي: حظر 21 دقيقة
            # واللمركز يرسل رسائل كل 0.1ث — كانت تُرمى وتتجمد اللوحة معها!)
            # الحظر يحكم فقط احتياط REST أدناه.
            if validated_symbols_to_scan or open_signals_cache:
                # [V9.19.0] القائمة الديناميكية + الصفقات المفتوحة (كانت الأخيرة مهملة)
                symbols_for_prices = collect_price_symbols()
                prices_to_set: Dict[str, float] = {}
                if stream_hub is not None:
                    prices_to_set = stream_hub.get_prices(symbols_for_prices)
                missing = [s for s in symbols_for_prices if s not in prices_to_set]
                if missing and (rate_guard.banned_until - time.time()) <= 0 and client:
                    # احتياط REST فقط (وزن 4 لطلب شامل) — لا يُلامس أثناء الحظر إطلاقًا
                    tickers = safe_get_symbol_ticker()
                    for t in tickers:
                        if t['symbol'] in missing:
                            prices_to_set[t['symbol']] = t['price']
                if prices_to_set: redis_client.hset(REDIS_PRICES_HASH_NAME, mapping=prices_to_set)
            time.sleep(max(1.0, PRICE_UPDATE_INTERVAL_SEC))
        except Exception as e: logger.error(f"خطأ في حلقة تحديث الأسعار: {e}"); time.sleep(10)

def initialize_bot_services():
    global client, validated_symbols_to_scan
    logger.info("🤖 [خدمات البوت] بدء التهيئة...")
    # [V9.16.0] مركز WebSocket يبدأ قبل كل شيء: يتصل ويجمع الشموع الحية حتى أثناء
    # حظر REST أو بيانات قاعدة باردة — التهيئة REST تكمل بالتوازي دون انتظار
    if stream_hub is not None:
        try:
            stream_hub.start()
        except Exception as hub_start_err:
            logger.warning(f"🛰️ [مركز البيانات] تعذر الإطلاق — REST فقط: {hub_start_err}")
    # [V9.20.0] بناء خريطة تغطية رموز المزودين البديلين في الخلفية (طلب واحد لكل مزود)
    try:
        data_feed.start_coverage_worker()
    except Exception as df_err:
        logger.warning(f"🌐 [تغذية البيانات] تعذر بدء خريطة التغطية: {df_err}")
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
            # [إصلاح V9.12.0] مهلة صريحة لكل طلبات العميل — بدونها يتدلى أي اتصال
            # نصف مفتوح للأبد ويحبس الحلقات والأقفال (السبب الجذري لتجمد اللوحة)
            client = Client(API_KEY, API_SECRET, requests_params={'timeout': BINANCE_CLIENT_TIMEOUT_SEC})
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
                # [V9.15.1] سكون كامل حتى انتهاء الحظر الحقيقي بدل إعادة الضرب كل دقيقتين:
                # الحظر الطويل كان يعني ~30 طلبًا مرفوضًا في الساعة وتسجيلات مضاعفة لنفس
                # الحظر. الآن ننام حتى موعد النهاية + تبريد الاستئناف على دفعات ≤300ث
                # تبقي اللوحة حية وتجعل الاحتكاك مع Binance أثناء الحظر = صفر.
                wait_sec = max(5.0, rate_guard.banned_until + rate_guard._active_cooldown - time.time())
            else:
                wait_sec = min(30.0 * attempt, 300.0)
            logger.critical(f"⚠️ [تهيئة] فشلت محاولة رقم {attempt} للاتصال بـ Binance: {msg[:200]}")
            if is_ban:
                logger.info(f"⏳ [تهيئة] سكون كامل حتى انتهاء الحظر (~{int(wait_sec)} ثانية ≈ {int(wait_sec // 60)} دقيقة) "
                            f"— صفر طلبات أثناء الحظر واللوحة تبقى تعمل...")
            else:
                logger.info(f"⏳ [تهيئة] إعادة المحاولة تلقائيًا بعد {int(wait_sec)} ثانية (اللوحة تبقى تعمل)...")
            if attempt == 1:
                send_telegram_message(f"⚠️ *تعذر الاتصال بـ Binance عند التهيئة*\n{msg[:200]}\nسيتم إعادة المحاولة تلقائيًا دون إيقاف الخدمة.")
            waited = 0.0
            while waited < wait_sec:  # [V9.15.1] نوم مجزّأ ≤300ث يسمح باستجابة الإيقاف
                chunk = min(300.0, wait_sec - waited)
                time.sleep(chunk)
                waited += chunk

    Thread(target=main_loop_enhanced, daemon=True).start()
    Thread(target=price_update_loop, daemon=True).start()
    Thread(target=trade_management_loop, daemon=True).start()
    Thread(target=balance_refresh_loop, daemon=True).start()  # [تحسين V9.11.0] كاش رصيد اللوحة
    Thread(target=btc_trend_loop, daemon=True).start()        # [تحسين V9.11] بوصلة اتجاه BTC
    Thread(target=leader_data_loop, daemon=True).start()      # [تحسين V9.13.0] بيانات قادة خريطة القيادة
    Thread(target=evidence_engine_loop, daemon=True).start()  # [V9.23.0] محرك الأدلة — ترشيح بالبرهان
    logger.info("✅ [خدمات البوت] تم بدء جميع الخدمات الخلفية بنجاح.")
    hub_line = ''
    if stream_hub is not None:
        try:
            hub_line = f"\n🛰️ مركز البيانات: WebSocket ({stream_hub.subscribed_count()} تدفق)" if stream_hub.is_connected() else "\n🛰️ مركز البيانات: جارٍ الاتصال..."
        except Exception:
            hub_line = "\n🛰️ مركز البيانات: مفعّل"
    send_telegram_message("✅ *البوت قيد التشغيل الآن (نسخة " + APP_VERSION + " - Neon Security)*" + hub_line)

# ---------------------- نقطة الدخول ----------------------
if __name__ == "__main__":
    # [تحسين V9.11.0] تخفيف تجوّع CPU على خطة Render المجانية (0.1 CPU):
    # حسابات pandas لـ 20 عملة تحتجز الـ GIL — تقصير مفتاح التبديل يمنح خيوط
    # الويب فرصة تنفيذ أسرع ويمنع تراكم طابور waitress أثناء دورات المسح
    try:
        sys.setswitchinterval(0.002)
    except Exception:
        pass
    logger.info(f"🚀 إطلاق بوت التداول ولوحة التحكم ({APP_VERSION} - Neon Security) 🚀")
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

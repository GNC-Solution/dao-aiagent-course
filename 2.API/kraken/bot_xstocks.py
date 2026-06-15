# Periodically read Kraken stocks and save any unavailable ones
# Read and save prices every minute; if unavailable, save at the previous price and volume = 0
# 주기적으로 크라켄 종목을 읽어서 없는 종목은 저장
# 가격을 1분마다 읽어서 저장, 없으면 이전 가격으로 저장하고 volume = 0
# 
# Django model.py
#
# class ATMSymbolKraken(models.Model):
#     service = models.ForeignKey(ATMService, on_delete=models.DO_NOTHING)
#     symbol = models.CharField(db_column="symbol", max_length=255)
#     symbol_call = models.CharField(db_column="symbol_call", max_length=255, default='')
#     auto_flag = models.CharField(db_column="auto_flag", max_length=10, default='0')
#     tr_price = models.DecimalField(max_digits=31, decimal_places=18, default=0)
#     tr_qty = models.DecimalField(max_digits=31, decimal_places=18, default=0)
#     tr_time = models.BigIntegerField(db_column="tr_time", default=0)
#     deleted_flag = models.BooleanField(default=False, verbose_name='사용여부')

#     class Meta:
#         db_table = "atm_symbol_kraken"

# class ADHHistoryKraken(models.Model):
#     symbol = models.CharField(db_column="symbol", max_length=255)
#     interval = models.CharField(db_column="interval", max_length=20, default='5m')
#     tmestamp = models.BigIntegerField(db_column="tmestamp")
#     openPrice = models.DecimalField(max_digits=31, decimal_places=18, default=0)
#     closePrice = models.DecimalField(max_digits=31, decimal_places=18, default=0)
#     highPrice = models.DecimalField(max_digits=31, decimal_places=18, default=0)
#     lowPrice = models.DecimalField(max_digits=31, decimal_places=18, default=0)
#     volume = models.DecimalField(max_digits=31, decimal_places=18, default=0)
#     status = models.CharField(db_column="status", max_length=20, default='')

#     class Meta:
#         db_table = "adh_history_kraken"
#         indexes = [
#             models.Index(fields=['symbol', 'tmestamp'], name='symbol_kraken_tmestamp_asc_idx')
#         ]
# 

import os
import sys

def setup_django_if_needed():
    """
    gunicorn / manage.py 환경: 이미 settings 로딩됨 → 아무것도 안 함
    terminal 직접 실행: settings 로딩 + django.setup()
    """
    if not os.environ.get("DJANGO_SETTINGS_MODULE"):
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        if BASE_DIR not in sys.path:
            sys.path.append(BASE_DIR)

        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
        import django
        django.setup()

setup_django_if_needed()

from django.conf import settings
from django.db import transaction
from system_manage.models import ATMService, ATMSymbolKraken, ADHHistoryKraken
from asgiref.sync import sync_to_async

import signal
import asyncio
from datetime import datetime, timedelta
import json
import traceback
import requests
import time
import logging
from logging.handlers import TimedRotatingFileHandler
import inspect
import math

# Global Logger 초기화
logger = logging.getLogger('kraken_xstocks')

def setup_logger(name: str, filename: str, level=logging.INFO):
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(level)
    base_name, ext = os.path.splitext(filename)
    final_log_file = os.path.join(settings.LOG_DIR, f"{base_name}{ext}")

    formatter = logging.Formatter("%(levelname)s %(asctime)s %(filename)s %(process)d %(thread)d [%(name)s] %(message)s", "%Y-%m-%d %H:%M:%S")

    file_handler = TimedRotatingFileHandler(final_log_file, when="midnight", interval=1, backupCount=7, encoding="utf-8")
    file_handler.suffix = "%Y-%m-%d"
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.propagate = False
    return logger

def setup_main_logger():
    return setup_logger("main", "kraken_xstocks.log")


# ============================================================
# 💾 [싱글톤] 전역 메모리 캐시 매니저 (DB 부하 최소화)
# ============================================================
class XStocksCache:
    _cached_symbols = set()
    _is_initialized = False

    @classmethod
    def initialize(cls, main_logger):
        """최초 1회 DB에서 전체 등록 심볼을 메모리에 상주시켜 캐싱합니다."""
        if not cls._is_initialized:
            main_logger.info("[XStocksCache] DB 자산 목록을 최초 1회 메모리에 캐싱 진행 중...")
            cls._cached_symbols = set(ATMSymbolKraken.objects.values_list('symbol', flat=True))
            cls._is_initialized = True
            main_logger.info("[XStocksCache] 캐싱 완료. 총 %d 개의 심볼이 보관됨.", len(cls._cached_symbols))

    @classmethod
    def has_symbol(cls, symbol: str) -> bool:
        return symbol in cls._cached_symbols

    @classmethod
    def add_symbol(cls, symbol: str):
        cls._cached_symbols.add(symbol)


# ============================================================
# 🔹 Django ORM 비동기 안전 래퍼 함수 레이어
# ============================================================
@sync_to_async
def get_active_kraken_symbols():
    """DB에서 auto_flag가 1인 수집 대상 심볼 목록을 가져옵니다."""
    return list(ATMSymbolKraken.objects.filter(auto_flag="1").values_list('symbol', 'symbol_call'))

@sync_to_async
def insert_candle_to_db(symbol: str, interval: str, timestamp_raw: str, c_open, c_high, c_low, c_close, c_volume):
    """
    🎯 [신규] gnc_api_defi.adh_history_kraken 테이블에 데이터를 적재합니다.
    크라켄의 타임스탬프 문자열('2026-06-11T07:01:00.000000Z') 또는 숫자를 정수형 에포크 타임으로 변환하여 저장합니다.
    """
    logger = setup_main_logger()
    try:
        # 타임스탬프 파싱 (스트링 형태로 올 경우 정수 변환)
        if isinstance(timestamp_raw, str):
            # ISO 포맷 파싱 후 타임스탬프 정수(초 단위) 추출
            dt = datetime.strptime(timestamp_raw.replace("Z", ""), "%Y-%m-%dT%H:%M:%S.%f")
            timestamp_int = int(time.mktime(dt.timetuple()))
        else:
            timestamp_int = int(timestamp_raw)

        # 중복 적재 방지를 위한 유니크 체크 (필요시 활성화)
        # if ADHHistoryKraken.objects.filter(symbol=symbol, interval=interval, tmestamp=timestamp_int).exists():
        #     return

        # ⚠️ 스키마 컬럼명에 맞춰 필드 매핑 ('tmestamp', 'openPrice' 등)
        ADHHistoryKraken.objects.create(
            symbol=symbol,
            interval=interval,
            tmestamp=timestamp_int,
            openPrice=c_open,
            highPrice=c_high,
            lowPrice=c_low,
            closePrice=c_close,
            volume=c_volume,
            status="1"
        )
    except Exception as e:
        logger.error("[DB_INSERT_ERROR] 심볼 %s 적재 실패: %s", symbol, e)

# ============================================================
# 🌐 데이터 레이어 구문 (API 및 동기 동기화)
# ============================================================
symbol_checks = []

def get_kraken_xstocks_symbols():
    current_function_name = inspect.currentframe().f_code.co_name
    logger = setup_main_logger()
    logger.info("[%s] is start.", current_function_name)

    url = "https://api.kraken.com/0/public/AssetPairs"
    params = {"aclass_base": "tokenized_asset"}

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        result_data = response.json()

        if result_data.get("error"):
            logger.error("[%s] 거래소 에러 발생 : %s", current_function_name, result_data['error'])
            return []

        pairs = result_data.get("result", {})
        rest_symbols = []      
        websocket_symbols = [] 

        for pair_id, info in pairs.items():
            ws_name = info.get("wsname")
            if ws_name:
                websocket_symbols.append(ws_name)
                rest_symbols.append(pair_id)

        websocket_symbols.sort()
        rest_symbols.sort()

        return websocket_symbols, rest_symbols

    except Exception as e:
        logger.error("[%s] ❌ 네트워크 또는 파싱 에러: %s", current_function_name, e)
        logger.error("[%s] ❌ 상세 에러 로그: %s", current_function_name, traceback.format_exc())
        return [], []


def kraken_xstocks_symbol_sync():
    """
    [데일리 동기화 함수]
    순수 메모리 캐시와 API 데이터를 초고속 비교 후 누락 자산만 데이터베이스에 INSERT 합니다.
    """
    current_function_name = inspect.currentframe().f_code.co_name
    logger = setup_main_logger()
    logger.info("[%s] 데일리 자산 동기화 비교를 개시합니다.", current_function_name)

    ws_list, rest_list = get_kraken_xstocks_symbols()
    if not ws_list:
        logger.warning("[%s] API 수신 데이터가 없어 배치를 건너뜁니다.", current_function_name)
        return

    XStocksCache.initialize(logger)

    try:
        atm_service = ATMService.objects.get(service_cd='SPOTAUTO')
    except (ATMService.DoesNotExist, ATMService.MultipleObjectsReturned) as e:
        logger.error("[%s] 'SPOTAUTO' 서비스 엔티티 예외 발생으로 처리를 중단합니다: %s", current_function_name, e)
        return

    new_inserted_count = 0

    for ws, rest in zip(ws_list, rest_list):
        if XStocksCache.has_symbol(ws):
            continue

        try:
            with transaction.atomic():
                atmsymbolkraken = ATMSymbolKraken.objects.create(
                    service_id=atm_service.id, 
                    symbol=ws, 
                    symbol_call=rest, 
                    auto_flag=0
                )
                XStocksCache.add_symbol(ws)
                new_inserted_count += 1
                logger.info("[%s] 🆕 신규 주식 자산 적재 완료 -> %s (%s)", current_function_name, ws, rest)
        except Exception as ex:
            logger.error("[%s] %s 생성 중 DB 처리 실패: %s", current_function_name, ws, ex)

    logger.info("[%s] 데일리 자산 배치 동기화 종료 (신규 반영 건수: %d 건)", current_function_name, new_inserted_count)


# ============================================================
# 🕒 비동기 데몬 제어 루프 파트
# ============================================================
async def daily_batch_daemon_loop():
    """
    매일 새벽 정해진 시간대에 자산 테이블을 완벽히 동기화하는 상주형 데몬 루프.
    """
    logger = setup_main_logger()
    logger.info("🚀 데일리 자산 배치 데몬 루프가 메모리에 적재되었습니다.")
    
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, kraken_xstocks_symbol_sync)

    while True:
        now = datetime.now()
        next_run = now.replace(hour=5, minute=0, second=0, microsecond=0)

        if now >= next_run:
            next_run += timedelta(days=1)

        sleep_seconds = (next_run - now).total_seconds()
        logger.info("💤 다음 신규 상장 조사 배치까지 대기 모드 진입: [%d초 남음] (목표 시간: %s)", int(sleep_seconds), next_run)

        await asyncio.sleep(sleep_seconds)

        logger.info("⏰ 정각 스케줄 타임 도달. Kraken API 검사를 재개합니다.")
        await loop.run_in_executor(None, kraken_xstocks_symbol_sync)


async def receive_websocket_ohlc_engine():
    """
    [실시간 1분봉 수집 엔진 - 완전판]
    장외 시간 무거래 상태를 감지하여 1분마다 메모리 상의 직전 최종 데이터를 강제 출력합니다.
    """
    current_function_name = inspect.currentframe().f_code.co_name
    logger = setup_main_logger()
    logger.info("📡 [%s] 실시간 1분봉 수집 파이프라인 가동 개시.", current_function_name)

    import websockets

    # 1. DB에서 수집 대상(auto_flag=1) 주식 목록 최초 조회 및 상태 세트 빌드
    active_pairs = await get_active_kraken_symbols()
    currently_subscribed = {pair[0] for pair in active_pairs}

    if not currently_subscribed:
        logger.warning("[%s] ⚠️ DB에 수집 활성화(auto_flag=1)된 자산이 없습니다. 30초 후 재시도합니다.", current_function_name)
        await asyncio.sleep(30)
        return

    logger.info("[%s] 🔥 총 %d 개의 자산 최초 감시 시작.", current_function_name, len(currently_subscribed))

    ws_url = "wss://ws.kraken.com/v2"
    last_db_check_time = time.time()

    # 🎯 [장외 시간 대응] 각 심볼별 직전 최종 캔들 데이터를 보관할 로컬 메모리 저장소
    last_candles = {}

    try:
        async with websockets.connect(ws_url) as websocket:
            logger.info("[%s] 🤝 Kraken WebSocket 서버와 핸드셰이크 성공.", current_function_name)

            # --------------------------------============================
            # 🎯 [알고리즘 1] 최초 구독 시 청크 리스트 균등 분할 매커니즘 작동
            # --------------------------------============================
            sub_list = list(currently_subscribed)
            MAX_CHUNK_SIZE = 20
            num_chunks = math.ceil(len(sub_list) / MAX_CHUNK_SIZE)
            
            for i in range(num_chunks):
                start_idx = (i * len(sub_list)) // num_chunks
                end_idx = ((i + 1) * len(sub_list)) // num_chunks
                chunk_symbols = sub_list[start_idx:end_idx]

                subscribe_message = {
                    "method": "subscribe",
                    "params": {"channel": "ohlc", "interval": 1, "symbol": chunk_symbols},
                    "req_id": int(time.time()) + i
                }

                await websocket.send(json.dumps(subscribe_message))
                logger.info("[%s] ✉️ [최초 균등 분할 구독] %d/%d 그룹 (%d개 발송): %s", current_function_name, i + 1, num_chunks, len(chunk_symbols), chunk_symbols)
                await asyncio.sleep(0.1)

            # 🎯 시간 제어를 위한 변수 추가
            last_candles = {}
            last_print_time = time.time()  # 마지막으로 화면에 출력한 시간 트래킹

            # --------------------------------============================
            # 🔄 실시간 수신 및 런타임 동적 제어 내부 무한 루프
            # --------------------------------============================
            while True:
                try:
                    # 🎯 타임아웃을 60초(1분)로 조율하여 거래 데이터가 끊기면 1분마다 TimeoutError 발생
                    #raw_response = await asyncio.wait_for(websocket.recv(), timeout=60.0)
                    raw_response = await asyncio.wait_for(websocket.recv(), timeout=2.0)
                    data = json.loads(raw_response)

                    if "event" in data or "status" in data:
#                        if data.get("method") == "subscribe" and data.get("success") is True:
#                            logger.info("[%s] ✅ 분할 채널 구독 정상 승인 완료.", current_function_name)
                        continue

                    # 실제 1분봉 데이터 패킷 정밀 실시간 파싱부
                    if "channel" in data and data["channel"] == "ohlc":
                        candle_data_list = data.get("data", [])
                        
                        for candle in candle_data_list:
                            symbol = candle.get("symbol")
                            c_open = candle.get("open")
                            c_high = candle.get("high")
                            c_low = candle.get("low")
                            c_close = candle.get("close")
                            c_volume = candle.get("volume")
                            c_timestamp = candle.get("timestamp")
                            
                            # 🎯 [메모리 업데이트] 최신 데이터 패킷이 수신되면 메모리 캐시 갱신
                            last_candles[symbol] = {
                                "timestamp": c_timestamp,
                                "open": c_open, "high": c_high, "low": c_low, "close": c_close, "volume": c_volume
                            }

                            logger.info(
                                "📈 [실시간 수신] %s | UTC: %s | O:%s H:%s L:%s C:%s | V:%s",
                                symbol, c_timestamp, c_open, c_high, c_low, c_close, c_volume
                            )

                            # 2. 🎯 [실시간 DB 적재] 수신 즉시 비동기 안전하게 INSERT
                            await insert_candle_to_db(
                                symbol=symbol, interval="1m", timestamp_raw=c_timestamp,
                                c_open=c_open, c_high=c_high, c_low=c_low, c_close=c_close, c_volume=c_volume
                            )
                except asyncio.TimeoutError:
                    # 🎯 [장외 시간대 강제 출력 처리] 1분 동안 거래소 데이터가 없으면 작동
#                    current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
#                    if not last_candles:
#                        logger.info("💤 [장외 대기 모드] %s - 아직 수신된 최초 데이터가 없어 직전 내역을 출력할 수 없습니다.", current_time_str)
#                    else:
#                        logger.info("💤 [장외 대기 모드] %s - 1분간 실시간 무거래. [직전 최종 데이터 캐시 출력]", current_time_str)
                        
                        # 보관 중이던 최종 종가 데이터를 순회하며 강제 생존 신고 및 정보 로깅 출력
#                        for symbol, c in last_candles.items():
#                            logger.info(
#                                "💤 [장외유지] %s | 최종수신UTC: %s | O:%s H:%s L:%s C:%s | V:%s (무거래 유지)",
#                                symbol, c["timestamp"], c["open"], c["high"], c["low"], c["close"], c["volume"]
#                            )
                    pass

                # 🎯 [핵심 변경점] 거래소 데이터 유무와 상관없이, 내 시계 기준으로 60초가 지나면 무조건 실행
                if time.time() - last_print_time >= 60.0:
                    current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    if not last_candles:
                        logger.info("💤 [장외 모니터링] %s - 아직 수신된 최초 데이터가 없습니다.", current_time_str)
                    else:
                        logger.info("💤 [장외 모니터링] %s - [1분 주기 현재 캐시 현황 출력]", current_time_str)
                        for symbol, c in last_candles.items():
                            # 장외 가상 분봉은 거래량을 0으로 처리하는 것이 정합성에 맞습니다.
                            virtual_volume = 0
                            # 시간은 현재 1분 주기가 흐른 시점이므로 현재 시각의 에포크 타임으로 가공
                            virtual_timestamp = int(time.time())
                            logger.info(
                                "💤 [장외유지] %s | 최종수신UTC: %s | O:%s H:%s L:%s C:%s | V:%s",
                                symbol, virtual_timestamp, c["open"], c["high"], c["low"], c["close"], virtual_volume
                            )

                            # 3. 🎯 [장외 가상 캔들 DB 적재] 직전 종가 데이터 기반 연장선 적재
                            await insert_candle_to_db(
                                symbol=symbol, interval="1m", timestamp_raw=virtual_timestamp,
                                c_open=c["open"], c_high=c["high"], c_low=c["low"], c_close=c["close"], c_volume=virtual_volume
                            )

                    # 출력 시간 갱신
                    last_print_time = time.time()

                # --------------------------------============================
                # 🎯 [알고리즘 2] 10분마다 DB 유동 스캔 후 실시간 구독 조정 처리
                # --------------------------------============================
                if time.time() - last_db_check_time > 600:
                    logger.info("[%s] 🔍 auto_flag 실시간 변동 내역 모니터링 중...", current_function_name)
                    latest_pairs = await get_active_kraken_symbols()
                    latest_symbols = {pair[0] for pair in latest_pairs}

                    # A. 새로 1로 투입된 자산 검출 후 추가 구독
                    symbols_to_add = latest_symbols - currently_subscribed
                    if symbols_to_add:
                        logger.info("🆕 [동적 추가 신호] 종목 구독 추가: %s", symbols_to_add)
                        add_message = {
                            "method": "subscribe",
                            "params": {"channel": "ohlc", "interval": 1, "symbol": list(symbols_to_add)},
                            "req_id": int(time.time())
                        }
                        await websocket.send(json.dumps(add_message))
                        currently_subscribed.update(symbols_to_add)

                    # B. 중간에 0으로 제외된 자산 검출 후 구독 철회
                    symbols_to_remove = currently_subscribed - latest_symbols
                    if symbols_to_remove:
                        logger.info("🚫 [동적 제거 신호] 자산 감시 제외 (구독 해제): %s", symbols_to_remove)
                        remove_message = {
                            "method": "unsubscribe",
                            "params": {"channel": "ohlc", "interval": 1, "symbol": list(symbols_to_remove)},
                            "req_id": int(time.time())
                        }
                        await websocket.send(json.dumps(remove_message))
                        currently_subscribed -= symbols_to_remove
                        
                        # 구독에서 제외된 심볼은 장외 출력 보관소에서도 메모리 정리
                        for sym in symbols_to_remove:
                            last_candles.pop(sym, None)

                    last_db_check_time = time.time()

    except websockets.exceptions.ConnectionClosed as cc:
        logger.error("[%s] ❌ 웹소켓 커넥션이 종료되었습니다: %s", current_function_name, cc)
    except Exception as e:
        logger.error("[%s] ❌ 엔진 메인 핸들러 예외 발생: %s", current_function_name, e)
        logger.error("[%s] ❌ 상세 예외 트레이스: %s", current_function_name, traceback.format_exc())
    
    logger.warning("[%s] 파이프라인 마감 세션 도달. 비동기 소켓 리커버리 모드로 진입합니다.", current_function_name)
    await asyncio.sleep(5)


# ============================================================
# 🛑 프로세스 시그널 및 라이프사이클 관리
# ============================================================
def graceful_shutdown_async(loop):
    logger = setup_main_logger()
    logger.info("🚨 [Signal] 종료 시그널 수신 완료. 비동기 태스크 자원을 안전하게 회수합니다.")
    
    for task in asyncio.all_tasks(loop=loop):
        task.cancel()
        
    logger.info("✅ All async tasks requested to cancel. Exiting process safely.")
    os._exit(0)


# ============================================================
# 🚀 비동기 메인 엔트리 포인트
# ============================================================
async def main():
    async def websocket_supervisor():
        while True:
            await receive_websocket_ohlc_engine()
            await asyncio.sleep(5)

    await asyncio.gather(
        daily_batch_daemon_loop(),
        websocket_supervisor()
    )

if __name__ == "__main__":
    logger = setup_main_logger()
    logger.info("🏁 Kraken xStocks 통합 관리 데몬 부팅 완료.")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: graceful_shutdown_async(loop))
        except NotImplementedError:
            pass

    try:
        loop.run_until_complete(main())
    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt detected.")
    finally:
        loop.close()
        logger.info("🔒 Loop securely closed. Daemon process safely exited.")

# nohup /home/gnc/anaconda3/envs/dao_artcoin_defi/bin/python bot/kraken/bot_xstocks.py >/mnt/data2/yyc9997/dao-artcoin-defi/logs/kraken_xstocks.log 2>&1 &

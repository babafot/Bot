import asyncio
import aiohttp
from datetime import datetime
import time
import os

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
FUNDING_RATE_THRESHOLD = 1.5

API_SYMBOLS_URL = "https://api.bitget.com/api/v2/mix/market/contracts?productType=usdt-futures"
API_RATE_URL = "https://api.bitget.com/api/v2/mix/market/current-fund-rate?symbol={}&productType=usdt-futures"
MAX_CONCURRENT = 20

async def get_symbol_list(session):
    try:
        async with session.get(API_SYMBOLS_URL, timeout=10) as resp:
            data = await resp.json()
            return [item["symbol"] for item in data.get("data", [])]
    except Exception as e:
        print("Sembol listesi alınamadı:", e)
        return []

async def get_funding_rate(session, symbol):
    try:
        async with session.get(API_RATE_URL.format(symbol), timeout=5) as resp:
            data = await resp.json()
            if data.get("data"):
                return symbol, float(data["data"][0].get("fundingRate", 0)) * 100
    except Exception as e:
        print(f"{symbol} için hata:", e)
    return symbol, None

async def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message,
        'parse_mode': 'HTML'
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=payload, timeout=5) as resp:
                if resp.status != 200:
                    print("Telegram gönderim hatası:", await resp.text())
    except Exception as e:
        print("Telegram bağlantı hatası:", e)

async def process_in_batches(session, symbols):
    all_results = []
    for i in range(0, len(symbols), MAX_CONCURRENT):
        batch = symbols[i:i + MAX_CONCURRENT]
        tasks = [get_funding_rate(session, symbol) for symbol in batch]
        results = await asyncio.gather(*tasks)
        all_results.extend(results)
        await asyncio.sleep(1.1)
    return all_results

async def run_monitor():
    while True:
        print("\n🟡 Taramaya başlandı...")
        start_time = time.time()

        async with aiohttp.ClientSession() as session:
            symbols = await get_symbol_list(session)
            if not symbols:
                print("Sembol listesi alınamadı, 5 dakika sonra tekrar denenecek.")
                await asyncio.sleep(300)
                continue

            results = await process_in_batches(session, symbols)

        message_lines = []
        for symbol, rate in results:
            if rate is not None and abs(rate) >= FUNDING_RATE_THRESHOLD:
                message_lines.append(f"<b>{symbol}</b> → {rate:.2f}%")

        timestamp = datetime.now().strftime('%H:%M')
        if message_lines:
            message = f"✅ <b>Funding Rate Raporu</b>\n🕒 {timestamp}\n\n" + "\n".join(message_lines)
        else:
            message = f"✅ <b>Funding Rate Raporu</b>\n🕒 {timestamp}\n\nUygun coin bulunamadı."

        await send_telegram_message(message)

        elapsed = time.time() - start_time
        print(f"📤 Rapor gönderildi. Süre: {elapsed:.2f} saniye. 5 dakika bekleniyor...\n")
        await asyncio.sleep(300)

asyncio.run(run_monitor())
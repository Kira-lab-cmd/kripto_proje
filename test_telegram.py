import asyncio, aiohttp

async def test():
    token = "8239164881:AAG5j2gREgeVkywvYgNMM118OJTHxguWwhM"
    chat_id = "6644935753"
    worker_url = "https://telegram-proxy.tetik03yusuf.workers.dev"
    
    async with aiohttp.ClientSession() as s:
        r = await s.post(
            f"{worker_url}/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": "✅ Bot bağlantı testi"},
        )
        print(await r.json())

asyncio.run(test())
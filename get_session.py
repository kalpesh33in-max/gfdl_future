import os
import asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession
from dotenv import load_dotenv

load_dotenv()

API_ID = os.getenv('API_ID')
API_HASH = os.getenv('API_HASH')

async def main():
    if not API_ID or not API_HASH:
        print("Error: API_ID and API_HASH must be set in your .env file or environment.")
        return

    async with TelegramClient(StringSession(), API_ID, API_HASH) as client:
        session_str = client.session.save()
        print("\n" + "="*50)
        print("YOUR STRING SESSION (Save this for Railway):")
        print("="*50)
        print(f"\n{session_str}\n")
        print("="*50)
        print("Copy the long string above and use it as STRING_SESSION in Railway/Env variables.")

if __name__ == '__main__':
    asyncio.run(main())

import asyncio

from prefect.events.clients import PrefectEventsClient

async def main():
    async with PrefectEventsClient() as client:
        print(f"Connected to: {client._events_socket_url}")
        pong = await client._websocket.ping()
        pong_time = await pong
        print(f"Response received in: {pong_time}")

if __name__ == '__main__':
    asyncio.run(main())
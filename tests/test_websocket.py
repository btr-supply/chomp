import asyncio
import websockets
import json

async def test_ws():
    try:
        async with websockets.connect('ws://localhost:40004/ws') as websocket:
            # Send subscribe message
            subscribe_msg = {
                'action': 'subscribe',
                'topics': ['chomp:XtComFeeds']
            }
            await websocket.send(json.dumps(subscribe_msg))
            print('Sent subscribe message:', json.dumps(subscribe_msg))
            
            # Wait for a few messages
            for i in range(3):
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                    print(f'Received message {i+1}:', message[:200] + '...' if len(message) > 200 else message)
                except asyncio.TimeoutError:
                    print(f'Timeout waiting for message {i+1}')
                    break
            
            # Unsubscribe
            unsubscribe_msg = {
                'action': 'unsubscribe',
                'topics': ['chomp:XtComFeeds']
            }
            await websocket.send(json.dumps(unsubscribe_msg))
            print('Sent unsubscribe message')
            
    except Exception as e:
        print(f'WebSocket test failed: {e}')

if __name__ == "__main__":
    asyncio.run(test_ws()) 
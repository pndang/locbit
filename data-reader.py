import asyncio
import websockets
import json
import psycopg2
from dotenv import load_dotenv
import os 

# Load .env vars
if not os.getenv("DATABASE_URL"):
    load_dotenv()

# Database connection
DATABASE_URL = os.getenv("DATABASE_URL")

async def websocket_to_postgresql():
    url = "ws://67.58.51.89:1881/e"
    async with websockets.connect(url) as ws:
        while True:
            msg = await ws.recv()
            data = json.loads(msg)

            print(data)
            
            # filter for server room only
            if data.get('location') == 'Server Room':
                store_to_database(data, 'Server Room')

            # filter for san diego only
            if data.get('location') == 'San Diego':
                store_to_database(data, 'San Diego')

            # filter for the computers
            if data.get('system_id') != 'lab_lights':
                store_to_database(data, 'pc')


def store_to_database(data, tag):
    try:
        # Connect to Heroku PostgreSQL with Secure Sockets Layer enabled
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        cursor = conn.cursor()

        # Insert data to 'server_room_data' table
        if tag == 'Server Room':

            cursor.execute("""
                INSERT INTO server_room_data (location, energy_consumption_kW, cpu_usage_percent, timestamp) VALUES (%s, %s, %s, to_timestamp(%s / 1000.0))
            """, (data['location'], data['energy_consumption_kW'], data['cpu_usage_percent'], data['timestamp']))
            
        elif tag == 'San Diego':
            
            cursor.execute("""
                INSERT INTO sd_data (location, temperature, timestamp) VALUES (%s, %s, to_timestamp(%s / 1000.0))
            """, (data['location'], data['temperature'], data['timestamp']))

        elif tag == 'pc':

            cursor.execute("""
                INSERT INTO pc_data (id, cpuUsagePercent, memoryUsagePercent, gpuUsagePercent, timestamp) VALUES (%s, %s, %s, %s, to_timestamp(%s / 1000.0))
            """, (data['system_id'], data['cpuUsagePercent'], data['memoryUsagePercent'], data['gpuUsagePercent'], data['time']))

        conn.commit()
        cursor.close()
        conn.close()
    
    except Exception as e:
        print(f"Database error: {e}")

# Run
asyncio.run(websocket_to_postgresql())
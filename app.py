import os
os.environ['EVENTLET_NO_GREENDNS'] = 'yes'
import eventlet
eventlet.monkey_patch()

from flask import Flask, render_template, request
from flask_socketio import SocketIO
import aprslib
import json
import os
import math
import time
import sqlite3

app = Flask(__name__)
app.secret_key = "aprsrx_secret"
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

CONFIG_FILE = "config.json"

def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

config = load_config()
AIS = None
welcomed_users = {}

DB_FILE = "database.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS location_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            callsign TEXT,
            lat REAL,
            lon REAL,
            timestamp REAL
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS message_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender TEXT,
            receiver TEXT,
            message_text TEXT,
            timestamp REAL
        )
    ''')
    
    # 30 Günden eski verileri temizle
    thirty_days_ago = time.time() - (30 * 24 * 60 * 60)
    c.execute("DELETE FROM location_history WHERE timestamp < ?", (thirty_days_ago,))
    c.execute("DELETE FROM message_history WHERE timestamp < ?", (thirty_days_ago,))
    
    conn.commit()
    conn.close()

init_db()

def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371.0 # Dünya yarıçapı (km)
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad
    
    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    distance = R * c
    return distance

def aprs_listener():
    global AIS
    try:
        callsign = config.get("aprs", {}).get("callsign", "N0CALL")
        passcode = config.get("aprs", {}).get("passcode", "-1")
        server = config.get("aprs", {}).get("server", "euro.aprs2.net")
        port = int(config.get("aprs", {}).get("port", 14580))
        filter_str = config.get("aprs", {}).get("filter", "r/40.8/37.3/200")
        
        AIS = aprslib.IS(callsign, passwd=passcode, port=port, host=server)
        if filter_str:
            AIS.set_filter(filter_str)
            
        AIS.connect()
        print(f"APRS-IS Bağlantısı Başarılı ({server}:{port}). Filtre: {filter_str}")
        
        def process_packet(packet):
            # Parse edilmiş paketi doğrudan web istemcilerine yolla
            socketio.emit('aprs_packet', packet)
            
            callsign = packet.get('from', '')
            
            # --- Veritabanı Loglama ---
            now = time.time()
            try:
                conn = sqlite3.connect(DB_FILE)
                c = conn.cursor()
                
                # Sadece TA7KES ve TA7BSS için KONUM logla
                if (callsign.startswith('TA7KES') or callsign.startswith('TA7BSS')) and packet.get('latitude') and packet.get('longitude'):
                    c.execute("INSERT INTO location_history (callsign, lat, lon, timestamp) VALUES (?, ?, ?, ?)",
                              (callsign, packet.get('latitude'), packet.get('longitude'), now))
                
                # BÖLGEDEKİ HERKES İÇİN (Ordu civarı 200km) MESAJLARI logla
                if packet.get('format') == 'message':
                    receiver = (packet.get('addresse') or "").trim() if hasattr((packet.get('addresse') or ""), "trim") else str(packet.get('addresse') or "").strip()
                    msg_text = (packet.get('message_text') or "").trim() if hasattr((packet.get('message_text') or ""), "trim") else str(packet.get('message_text') or "").strip()
                    c.execute("INSERT INTO message_history (sender, receiver, message_text, timestamp) VALUES (?, ?, ?, ?)",
                              (callsign, receiver, msg_text, now))
                              
                conn.commit()
                conn.close()
            except Exception as e:
                print(f"DB Log Error: {e}")
            
            # --- Geofencing: Korgan Sınırına Girenleri Karşılama ---
            if packet.get('latitude') and packet.get('longitude'):
                callsign = packet.get('from')
                lat = packet.get('latitude')
                lon = packet.get('longitude')
                
                KORGAN_LAT = 40.8000
                KORGAN_LON = 37.3000
                RADIUS_KM = 5.0
                
                dist = haversine_distance(lat, lon, KORGAN_LAT, KORGAN_LON)
                
                if dist <= RADIUS_KM:
                    now = time.time()
                    last_welcomed = welcomed_users.get(callsign, 0)
                    my_callsign = config.get("aprs", {}).get("callsign", "N0CALL")
                    
                    # 12 saat (43200 saniye) bekleme süresi ve kendi çağrı işaretimize atmama kontrolü
                    if now - last_welcomed > 43200 and callsign != my_callsign:
                        welcomed_users[callsign] = now
                        # Hedef çağrı işaretini 9 karaktere tamamla
                        target_padded = callsign.ljust(9)[:9]
                        msg = "Korgan'a Hosgeldiniz, 73 de TA7KES (Op.Ertugrul)"
                        
                        welcome_packet = f"{my_callsign}>APRS::{target_padded}:{msg}"
                        try:
                            AIS.sendall(welcome_packet)
                            print(f"KARŞILAMA MESAJI GÖNDERİLDİ: {callsign} (Mesafe: {dist:.1f}km)")
                            
                            # Gönderdiğimiz mesajı arayüze de düşürelim
                            socketio.emit('aprs_packet', {
                                "from": my_callsign,
                                "format": "message",
                                "addresse": target_padded.strip(),
                                "messageText": msg
                            })
                        except Exception as e:
                            print(f"Karşılama gönderilemedi: {e}")
            
        AIS.consumer(process_packet, raw=False)
    except Exception as e:
        print(f"APRS Listener Hatası: {e}")

@app.route('/')
def index():
    return render_template('index.html', config=config)

@app.route('/api/history')
def api_history():
    try:
        conn = sqlite3.connect(DB_FILE)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        
        # Son 1 aylık lokasyonları çek (Zamansal sıralı)
        c.execute("SELECT callsign, lat, lon, timestamp FROM location_history ORDER BY timestamp ASC")
        locations = [dict(row) for row in c.fetchall()]
        
        # Son 1 aylık mesajları çek
        c.execute("SELECT sender, receiver, message_text, timestamp FROM message_history ORDER BY timestamp ASC")
        messages = [dict(row) for row in c.fetchall()]
        
        conn.close()
        return json.dumps({"status": "success", "locations": locations, "messages": messages})
    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)})

@socketio.on('send_message')
def handle_send_message(data):
    target = data.get('target', '')
    msg = data.get('message', '')
    
    if not target or not msg:
        return {"status": "error", "message": "Eksik bilgi"}
        
    # APRS protokolünde hedefin çağrı işareti tam 9 karakter olmalı (boşluklarla tamamlanır)
    target_padded = target.ljust(9)[:9]
    
    if AIS:
        callsign = config.get("aprs", {}).get("callsign", "N0CALL")
        packet_raw = f"{callsign}>APRS::{target_padded}:{msg}"
        try:
            AIS.sendall(packet_raw)
            print(f"Mesaj Gönderildi: {packet_raw}")
            return {"status": "success", "packet": packet_raw}
        except Exception as e:
            print(f"Mesaj Gönderim Hatası: {e}")
            return {"status": "error", "message": str(e)}
    else:
        return {"status": "error", "message": "APRS sunucusuna bağlı değil"}

if __name__ == "__main__":
    port = int(config.get("web", {}).get("port", 6061))
    print(f"Web sunucusu başlatılıyor: http://0.0.0.0:{port}")
    
    # Arka plan görevini başlat (eventlet thread)
    eventlet.spawn(aprs_listener)
    
    socketio.run(app, host="0.0.0.0", port=port, debug=False)

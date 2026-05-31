import os
os.environ['EVENTLET_NO_GREENDNS'] = 'yes'
import eventlet
eventlet.monkey_patch()

from flask import Flask, render_template, request, Response
from flask_socketio import SocketIO
import aprslib
import json
import os
import math
import time
import sqlite3
import threading
import datetime

def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Dünya yarıçapı (km)
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    a = math.sin(dLat/2) * math.sin(dLat/2) + \
        math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * \
        math.sin(dLon/2) * math.sin(dLon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

app = Flask(__name__)
app.secret_key = "aprsrx_secret"
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

CONFIG_FILE = "config.json"

def load_config():
    if not os.path.exists(CONFIG_FILE):
        return {"aprs": {"callsign": "NOCALL", "passcode": "-1", "server": "euro.aprs2.net", "port": 14580}}
    with open(CONFIG_FILE, "r") as f:
        return json.load(f)

def save_config(cfg):
    with open(CONFIG_FILE, "w") as f:
        json.dump(cfg, f, indent=4)
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
            comment TEXT,
            timestamp REAL,
            symbol TEXT,
            symbol_table TEXT
        )
    ''')
    
    # Eskiden kalan tablolar için eksik sütunları ekleme (Hata verirse sütun zaten vardır, yoksay)
    try:
        c.execute('ALTER TABLE location_history ADD COLUMN symbol TEXT')
        c.execute('ALTER TABLE location_history ADD COLUMN symbol_table TEXT')
    except sqlite3.OperationalError:
        pass
    c.execute('''
        CREATE TABLE IF NOT EXISTS message_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender TEXT,
            receiver TEXT,
            message_text TEXT,
            timestamp REAL
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS welcome_history (
            callsign TEXT PRIMARY KEY,
            timestamp REAL
        )
    ''')
    
    # Startup'ta sadece genel eski temizliği yap
    thirty_days_ago = time.time() - (30 * 24 * 60 * 60)
    c.execute("DELETE FROM location_history WHERE timestamp < ?", (thirty_days_ago,))
    c.execute("DELETE FROM message_history WHERE timestamp < ?", (thirty_days_ago,))
    
    conn.commit()
    conn.close()

init_db()

def db_cleanup_task():
    while True:
        try:
            conn = sqlite3.connect(DB_FILE)
            c = conn.cursor()
            now = time.time()
            thirty_days_ago = now - (30 * 24 * 60 * 60)
            two_hours_ago = now - (2 * 60 * 60)
            
            # Takip edilenler ve kendi çağrı işaretimiz
            callsign = config.get("aprs", {}).get("callsign", "NOCALL")
            tracked = config.get('tracked_callsigns', [])
            important_calls = [callsign] + tracked
            
            # Parametre stringleri (örn: callsign LIKE ? OR callsign LIKE ?)
            loc_like_clauses = " OR ".join(["callsign LIKE ?"] * len(important_calls))
            msg_like_clauses = " OR ".join(["sender LIKE ?"] * len(important_calls))
            
            params = [f"{c}%" for c in important_calls]
            
            # Önemli olanlar için 30 günlük temizlik
            c.execute(f"DELETE FROM location_history WHERE timestamp < ? AND ({loc_like_clauses})", [thirty_days_ago] + params)
            c.execute(f"DELETE FROM message_history WHERE timestamp < ? AND ({msg_like_clauses})", [thirty_days_ago] + params)
            
            # Diğer herkes için 2 saatlik temizlik
            c.execute(f"DELETE FROM location_history WHERE timestamp < ? AND NOT ({loc_like_clauses})", [two_hours_ago] + params)
            c.execute(f"DELETE FROM message_history WHERE timestamp < ? AND NOT ({msg_like_clauses})", [two_hours_ago] + params)
            
            # Welcome geçmişini temizle (24 saat)
            twenty_four_hours_ago = now - (24 * 60 * 60)
            c.execute("DELETE FROM welcome_history WHERE timestamp < ?", (twenty_four_hours_ago,))
            
            conn.commit()
            conn.close()
        except Exception as e:
            print("DB Cleanup Error:", e)
        eventlet.sleep(600)  # Her 10 dakikada bir çalıştır

# Arka planda temizlik thread'ini başlat
eventlet.spawn(db_cleanup_task)

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
        
        AIS = aprslib.IS(callsign, passwd=passcode, port=port, host=server)
        
        # Radar filtresi (varsayılan: 100km)
        radius_filter = config.get("aprs", {}).get("filter", "r/40.8/37.3/100")
        
        # Takip edilecek çağrı işaretleri filtresi
        tracked = config.get('tracked_callsigns', [])
        buddylist = [callsign] + tracked
        buddylist_filter = "b/" + "/".join(buddylist)
        
        # Filtreleri birleştir (Boşlukla ayır)
        filter_str = f"{radius_filter} {buddylist_filter}"
        
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
                
                # HERKES İÇİN KONUM LOGLA VE HOŞGELDİN MESAJI KONTROLÜ
                lat = packet.get('latitude')
                lng = packet.get('longitude')
                if lat and lng:
                    comment = packet.get('comment', '')
                    symbol = packet.get('symbol', '')
                    symbol_table = packet.get('symbol_table', '')
                    c.execute("INSERT INTO location_history (callsign, lat, lon, comment, timestamp, symbol, symbol_table) VALUES (?, ?, ?, ?, ?, ?, ?)",
                              (callsign, lat, lng, comment, now, symbol, symbol_table))
                              
                    # --- HOŞGELDİN MESAJI ---
                    my_callsign = config.get("aprs", {}).get("callsign", "NOCALL")
                    # Kendimize mesaj atmayalım
                    if not callsign.startswith(my_callsign):
                        # Korgan merkez (40.8, 37.3) mesafesi
                        dist = haversine(40.8, 37.3, float(lat), float(lng))
                        if dist <= 5:
                            c.execute("SELECT timestamp FROM welcome_history WHERE callsign = ?", (callsign,))
                            row = c.fetchone()
                            
                            # 24 saatte (86400 sn) bir defa
                            if not row or (now - row[0]) > 86400:
                                target_padded = callsign.ljust(9)
                                msg_text = "Korgan'a Hosgeldiniz! Iletisim: +905314913916"
                                packet_raw = f"{my_callsign}>APRS::{target_padded}:{msg_text}"
                                
                                try:
                                    if AIS:
                                        AIS.sendall(packet_raw)
                                        print(f"Hoşgeldin Mesajı Gönderildi: {packet_raw}")
                                        
                                        # Mesajı geçmişe kaydet (Telsiz mesajları ekranında görünsün)
                                        c.execute("INSERT INTO message_history (sender, receiver, message_text, timestamp) VALUES (?, ?, ?, ?)",
                                                  (my_callsign, callsign, f"[Sistem-Oto] {msg_text}", now))
                                        
                                        # Welcome tablosunu güncelle
                                        c.execute("INSERT OR REPLACE INTO welcome_history (callsign, timestamp) VALUES (?, ?)", (callsign, now))
                                except Exception as e:
                                    print(f"Oto-mesaj gönderme hatası: {e}")
                
                # HERKES İÇİN MESAJ LOGLA
                if packet.get('format') == 'message':
                    receiver = (packet.get('addresse') or "").trim() if hasattr((packet.get('addresse') or ""), "trim") else str(packet.get('addresse') or "").strip()
                    msg_text = (packet.get('message_text') or "").trim() if hasattr((packet.get('message_text') or ""), "trim") else str(packet.get('message_text') or "").strip()
                    
                    if not msg_text and packet.get('response'):
                        resp = packet.get('response')
                        msg_no = packet.get('msgNo', '')
                        if resp == 'ack':
                            msg_text = f"✅ [İletildi - ACK] Mesaj No: {msg_no}"
                        elif resp == 'rej':
                            msg_text = f"❌ [Reddedildi - REJ] Mesaj No: {msg_no}"
                        else:
                            msg_text = f"[Sistem: {resp}]"
                            
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

@app.route('/tracker')
def tracker():
    # Mobil cihazlar için özel takip sayfası
    return render_template('tracker.html')

@app.route('/api/history')
def api_history():
    try:
        conn = sqlite3.connect(DB_FILE)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        
        # URL'den date parametresini al (YYYY-MM-DD)
        date_str = request.args.get('date')
        if date_str:
            try:
                dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
                start_ts = dt.timestamp()
                end_ts = start_ts + 86400
            except ValueError:
                # Hatalı tarih formatıysa bugünü kullan
                now = datetime.datetime.now()
                dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
                start_ts = dt.timestamp()
                end_ts = start_ts + 86400
        else:
            # Parametre yoksa bugünü kullan
            now = datetime.datetime.now()
            dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
            start_ts = dt.timestamp()
            end_ts = start_ts + 86400
            
        # Sadece takip edilenleri getir
        cfg = load_config()
        my_call = cfg.get("aprs", {}).get("callsign", "NOCALL")
        tracked = cfg.get('tracked_callsigns', [])
        important_calls = [my_call] + tracked
        
        loc_like_clauses = " OR ".join(["callsign LIKE ?"] * len(important_calls))
        msg_like_clauses = " OR ".join(["sender LIKE ?"] * len(important_calls))
        params = [f"{c}%" for c in important_calls]
        
        # Yabancı istasyonların son 1 saatini de dahil et
        one_hour_ago = time.time() - 3600
        
        c.execute(f"SELECT callsign, lat, lon, comment, timestamp, symbol, symbol_table FROM location_history WHERE (timestamp >= ? AND timestamp < ? AND ({loc_like_clauses})) OR (timestamp > ?) ORDER BY timestamp ASC", [start_ts, end_ts] + params + [one_hour_ago])
        locations = [dict(row) for row in c.fetchall()]
        
        c.execute(f"SELECT sender, receiver, message_text, timestamp FROM message_history WHERE (timestamp >= ? AND timestamp < ? AND ({msg_like_clauses})) OR (timestamp > ?) ORDER BY timestamp ASC", [start_ts, end_ts] + params + [one_hour_ago])
        messages = [dict(row) for row in c.fetchall()]
        
        conn.close()
        response = Response(json.dumps({"status": "success", "locations": locations, "messages": messages}), mimetype='application/json')
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        return response
    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)})

@app.route('/api/settings', methods=['GET', 'POST'])
def api_settings():
    cfg = load_config()
    if request.method == 'POST':
        data = request.json
        if data and 'tracked_callsigns' in data:
            # Temizle ve kaydet (kullanıcı * koymuşsa temizle)
            callsigns = [c.strip().upper().replace('*', '') for c in data['tracked_callsigns'] if c.strip()]
            cfg['tracked_callsigns'] = callsigns
            save_config(cfg)
            
            # Ayarların etkili olması için 1 saniye sonra sistemi yeniden başlat
            def restart_server():
                time.sleep(1)
                os._exit(1)
            threading.Thread(target=restart_server).start()
            
            return json.dumps({"status": "success", "message": "Ayarlar kaydedildi. Sistem yeniden başlatılıyor..."})
        return json.dumps({"status": "error", "message": "Geçersiz veri"}), 400
    
    # GET isteği: Mevcut ayarları döndür
    tracked = cfg.get('tracked_callsigns', [])
    return json.dumps({"status": "success", "tracked_callsigns": tracked})

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
            
            # Gönderilen mesajı veritabanına kaydet
            now = time.time()
            conn = sqlite3.connect(DB_FILE)
            c = conn.cursor()
            c.execute("INSERT INTO message_history (sender, receiver, message_text, timestamp) VALUES (?, ?, ?, ?)",
                      (callsign, target, msg, now))
            conn.commit()
            conn.close()
            
            return {"status": "success", "packet": packet_raw}
        except Exception as e:
            print(f"Mesaj Gönderim Hatası: {e}")
            return {"status": "error", "message": str(e)}
    else:
        return {"status": "error", "message": "APRS sunucusuna bağlı değil"}

@socketio.on('private_location')
def handle_private_location(data):
    callsign = data.get('callsign', 'PRIVATE-1')
    lat = data.get('lat')
    lon = data.get('lon')
    speed = data.get('speed', 0)
    alt = data.get('alt', 0)
    
    if not lat or not lon:
        return
        
    now = time.time()
    # Mobil uygulama için Telefon ikonunu temsil eden APRS kodunu sabitleyelim
    symbol = '$'
    symbol_table = '/'
    
    # Hız ve Yüksekliği yoruma ekle
    comment_text = "Mobil Tracker"
    if speed or alt:
        comment_text += f" (Hız: {speed:.1f}m/s, Rkm: {alt:.0f}m)"
    
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("INSERT INTO location_history (callsign, lat, lon, comment, timestamp, symbol, symbol_table) VALUES (?, ?, ?, ?, ?, ?, ?)",
                  (callsign, lat, lon, comment_text, now, symbol, symbol_table))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Private Tracker DB Error: {e}")
        
    # Anlık olarak haritaya düşmesi için sahte bir APRS paketi hazırlayıp yayınla
    packet = {
        "from": callsign,
        "latitude": lat,
        "longitude": lon,
        "format": "private",
        "comment": comment_text,
        "symbol": symbol,
        "symbol_table": symbol_table,
        "speed": speed,
        "altitude": alt
    }
    socketio.emit('aprs_packet', packet)

if __name__ == "__main__":
    port = int(config.get("web", {}).get("port", 6061))
    print(f"Web sunucusu başlatılıyor: http://0.0.0.0:{port}")
    
    # Arka plan görevini başlat (eventlet thread)
    eventlet.spawn(aprs_listener)
    
    socketio.run(app, host="0.0.0.0", port=port, debug=False)

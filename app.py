import os
os.environ['EVENTLET_NO_GREENDNS'] = 'yes'
# pyrefly: ignore [missing-import]
import eventlet
eventlet.monkey_patch()

from flask import Flask, render_template, request, Response, session, redirect, url_for, send_from_directory, make_response
from flask_socketio import SocketIO
import aprslib
import json
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

import functools
app = Flask(__name__)
app.secret_key = "aprsrx_secret_key_2024_sdnnet"
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

# ─── Kimlik Doğrulama ────────────────────────────────────────────────────────
APP_PASSWORD = "SdnNET1997"

def login_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated

CONFIG_FILE = "config.json"

def load_config():
    if not os.path.exists(CONFIG_FILE):
        return {
            "aprs": {"callsign": "NOCALL", "passcode": "-1", "server": "euro.aprs2.net", "port": 14580},
            "web": {"port": 6061},
            "tracked_callsigns": []
        }
    with open(CONFIG_FILE, "r") as f:
        return json.load(f)

def save_config(cfg):
    with open(CONFIG_FILE, "w") as f:
        json.dump(cfg, f, indent=4)
    return {}

config = load_config()
AIS = None
tcp_clients = []
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
    
    # Eskiden kalan tablolar için eksik sütunları ekleme
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
    c.execute('''
        CREATE TABLE IF NOT EXISTS outgoing_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender TEXT,
            receiver TEXT,
            message_text TEXT,
            queued_at REAL,
            status TEXT DEFAULT 'pending',
            attempts INTEGER DEFAULT 0,
            last_attempt REAL,
            sent_at REAL
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
            
            # Parametre stringleri
            loc_like_clauses = " OR ".join(["callsign LIKE ?"] * len(important_calls))
            msg_like_clauses = " OR ".join(["sender LIKE ?"] * len(important_calls))
            
            params = [f"{cs}%" for cs in important_calls]
            
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

def message_sender_task():
    """
    Giden mesaj kuyruğunu (outgoing_queue) her 5 saniyede kontrol eder.
    Pending mesajları APRS-IS üzerinden gönderir.
    Sayfa kapalı olsa bile çalışır.
    """
    global AIS
    MAX_ATTEMPTS = 5
    RETRY_DELAY  = 30  # saniye — başarısız sonrası bekleme

    while True:
        try:
            if AIS:  # APRS-IS bağlantısı aktifse
                now  = time.time()
                conn = sqlite3.connect(DB_FILE)
                c    = conn.cursor()

                # 'pending' + yeterince beklemiş (ilk deneme veya RETRY_DELAY geçti)
                c.execute(
                    """
                    SELECT id, sender, receiver, message_text, attempts
                    FROM outgoing_queue
                    WHERE status = 'pending'
                      AND (last_attempt IS NULL OR (? - last_attempt) >= ?)
                    ORDER BY queued_at ASC
                    LIMIT 10
                    """,
                    (now, RETRY_DELAY)
                )
                rows = c.fetchall()

                for row in rows:
                    qid, sender, receiver, msg_text, attempts = row
                    target_padded = receiver.ljust(9)[:9]
                    pkt_raw = f"{sender}>APRS,TCPIP*::{target_padded}:{msg_text}"

                    success = False
                    try:
                        AIS.sendall(pkt_raw)
                        success = True
                        print(f"✅ Kuyruk Gönderildi [{qid}]: {pkt_raw}")
                    except Exception as e:
                        print(f"❌ Kuyruk Gönderim Hatası [{qid}]: {e}")

                    if success:
                        c.execute(
                            "UPDATE outgoing_queue SET status='sent', sent_at=?, last_attempt=? WHERE id=?",
                            (now, now, qid)
                        )
                    else:
                        new_attempts = attempts + 1
                        new_status   = 'failed' if new_attempts >= MAX_ATTEMPTS else 'pending'
                        c.execute(
                            "UPDATE outgoing_queue SET attempts=?, last_attempt=?, status=? WHERE id=?",
                            (new_attempts, now, new_status, qid)
                        )

                # Eski 'sent'/'failed' kayıtları temizle (24 saat sonra)
                c.execute(
                    "DELETE FROM outgoing_queue WHERE status IN ('sent','failed') AND queued_at < ?",
                    (now - 86400,)
                )

                conn.commit()
                conn.close()
        except Exception as e:
            print(f"message_sender_task hatası: {e}")

        eventlet.sleep(5)  # Her 5 saniyede bir kontrol et

def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371.0  # Dünya yarıçapı (km)
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

# Korgan koordinatları ve hoşgeldin yarıçapı
KORGAN_LAT  = 40.822
KORGAN_LON  = 37.346
KORGAN_R_KM = 5.0

# Fatsa koordinatları ve hoşgeldin yarıçapı
FATSA_LAT  = 40.9215
FATSA_LON  = 37.5043
FATSA_R_KM = 7.0

# Ordu bölgesi geniş harita filtre merkezi ve yarıçapı
ORDU_MAP_LAT  = 40.98
ORDU_MAP_LON  = 37.89
ORDU_MAP_R_KM = 150

def build_filter(callsign, tracked):
    """
    APRS-IS çoklu filtre (OR mantığı — aralarında boşluk):
      1) b/TA7KES*/TA7BSS*...       → takip listesinin tüm SSID varyantları
      2) r/KORGAN_LAT/LON/5         → Korgan 5km — hoşgeldin mesajı için
      3) r/FATSA_LAT/LON/7          → Fatsa 7km  — hoşgeldin mesajı için
      4) r/ORDU_LAT/LON/150         → Ordu bölgesi 150km — harita görüntüsü için
    """
    buddylist = [callsign] + [cs for cs in tracked if cs]
    seen = set()
    filter_parts = []
    for cs in buddylist:
        base = cs.split('-')[0].upper()
        wildcard = base + '*'
        if wildcard not in seen:
            seen.add(wildcard)
            filter_parts.append(wildcard)
    buddy_filter  = "b/" + "/".join(filter_parts)
    korgan_filter = f"r/{KORGAN_LAT}/{KORGAN_LON}/{int(KORGAN_R_KM)}"
    fatsa_filter  = f"r/{FATSA_LAT}/{FATSA_LON}/{int(FATSA_R_KM)}"
    ordu_filter   = f"r/{ORDU_MAP_LAT}/{ORDU_MAP_LON}/{int(ORDU_MAP_R_KM)}"
    return f"{buddy_filter} {korgan_filter} {fatsa_filter} {ordu_filter}"


def process_parsed_packet(packet):
    global AIS
    pkt_callsign = packet.get('from', '')
    if not pkt_callsign:
        return

    cfg_now      = load_config()
    my_callsign  = cfg_now.get("aprs", {}).get("callsign", "NOCALL")
    tracked_now  = cfg_now.get('tracked_callsigns', [])
    allowed      = [my_callsign] + tracked_now

    # SSID'siz eşleşme: TA7KES-9 listede TA7KES varsa da kabul et
    def matches_any(cs, allowed_list):
        if not cs:
            return False
        cs_base = cs.split('-')[0].upper()
        for a in allowed_list:
            a_base = a.split('-')[0].upper()
            if cs.upper() == a.upper() or cs_base == a_base:
                return True
        return False

    lat = packet.get('latitude')
    lng = packet.get('longitude')
    now = time.time()

    # ─────────────────────────────────────────────────────────────
    # BLOK 1a: HOŞGELDİN — KORGAN (5km)
    # ─────────────────────────────────────────────────────────────
    if lat and lng:
        is_myself = pkt_callsign.upper().startswith(my_callsign.split('-')[0].upper())
        if not is_myself:
            dist_korgan = haversine(KORGAN_LAT, KORGAN_LON, float(lat), float(lng))
            if dist_korgan <= KORGAN_R_KM:
                try:
                    conn_w = sqlite3.connect(DB_FILE)
                    cw     = conn_w.cursor()
                    # welcome_history anahtarı: callsign (Korgan için düz callsign)
                    cw.execute("SELECT timestamp FROM welcome_history WHERE callsign = ?", (pkt_callsign,))
                    row = cw.fetchone()
                    if not row or (now - row[0]) > 86400:
                        target_padded = pkt_callsign.ljust(9)
                        msg_text  = "Korgan'a Hosgeldiniz! TA7KES Op. Ertugrul Iletisim: +905314913916"
                        pkt_raw   = f"{my_callsign}>APRS,TCPIP*::{target_padded}:{msg_text}"
                        for client_fd in tcp_clients:
                            try:
                                client_fd.write(pkt_raw + "\r\n")
                                client_fd.flush()
                            except: pass
                        if AIS:
                            try:
                                AIS.sendall(pkt_raw)
                            except: pass
                        print(f"✅ Korgan Hoşgeldin → {pkt_callsign} ({dist_korgan:.1f}km): {pkt_raw}")
                        cw.execute(
                            "INSERT INTO message_history (sender, receiver, message_text, timestamp) VALUES (?, ?, ?, ?)",
                            (my_callsign, pkt_callsign, f"[Sistem-Oto] {msg_text}", now)
                        )
                        cw.execute(
                            "INSERT OR REPLACE INTO welcome_history (callsign, timestamp) VALUES (?, ?)",
                            (pkt_callsign, now)
                        )
                        conn_w.commit()
                    conn_w.close()
                except Exception as e:
                    print(f"Korgan hoşgeldin hatası: {e}")

    # ─────────────────────────────────────────────────────────────
    # BLOK 1b: HOŞGELDİN — FATSA (7km)
    # ─────────────────────────────────────────────────────────────
    if lat and lng:
        is_myself = pkt_callsign.upper().startswith(my_callsign.split('-')[0].upper())
        if not is_myself:
            dist_fatsa = haversine(FATSA_LAT, FATSA_LON, float(lat), float(lng))
            if dist_fatsa <= FATSA_R_KM:
                # Fatsa için welcome_history anahtarı: "FATSA_<callsign>" ile çakışma önlenir
                fatsa_key = f"FATSA_{pkt_callsign}"
                try:
                    conn_w = sqlite3.connect(DB_FILE)
                    cw     = conn_w.cursor()
                    cw.execute("SELECT timestamp FROM welcome_history WHERE callsign = ?", (fatsa_key,))
                    row = cw.fetchone()
                    if not row or (now - row[0]) > 86400:
                        target_padded = pkt_callsign.ljust(9)
                        msg_text  = "Fatsa'ya Hosgeldiniz! TA7KES Op. Ertugrul Iletisim: +905314913916"
                        pkt_raw   = f"{my_callsign}>APRS,TCPIP*::{target_padded}:{msg_text}"
                        for client_fd in tcp_clients:
                            try:
                                client_fd.write(pkt_raw + "\r\n")
                                client_fd.flush()
                            except: pass
                        if AIS:
                            try:
                                AIS.sendall(pkt_raw)
                            except: pass
                        print(f"✅ Fatsa Hoşgeldin → {pkt_callsign} ({dist_fatsa:.1f}km): {pkt_raw}")
                        cw.execute(
                            "INSERT INTO message_history (sender, receiver, message_text, timestamp) VALUES (?, ?, ?, ?)",
                            (my_callsign, pkt_callsign, f"[Sistem-Oto-Fatsa] {msg_text}", now)
                        )
                        cw.execute(
                            "INSERT OR REPLACE INTO welcome_history (callsign, timestamp) VALUES (?, ?)",
                            (fatsa_key, now)
                        )
                        conn_w.commit()
                    conn_w.close()
                except Exception as e:
                    print(f"Fatsa hoşgeldin hatası: {e}")

    # ─────────────────────────────────────────────────────────────
    # BLOK 2: HARİTA — Konum paketi olan HERKES haritada görünsün
    # (Ordu bölgesi geniş filtresinden gelen tüm araçlar dahil)
    # DB'ye YAZILMAZ — sadece socket.io ile iletilir.
    # ─────────────────────────────────────────────────────────────
    pkt_receiver    = str(packet.get('addresse') or '').strip()
    is_from_tracked = matches_any(pkt_callsign, allowed)
    is_to_tracked   = matches_any(pkt_receiver, allowed)

    if lat and lng and not is_from_tracked:
        # Takip listesinde olmayan araç: sadece haritaya yayınla, DB'ye yazma
        socketio.emit('aprs_packet', packet)
        return

    if not is_from_tracked and not is_to_tracked:
        return  # Konum yok, takip listesinde de yok → atla

    # Takip listesindekiler: hem haritaya hem DB'ye
    socketio.emit('aprs_packet', packet)

    try:
        conn = sqlite3.connect(DB_FILE)
        c    = conn.cursor()

        # KONUM LOGLA (sadece takip listesindekiler)
        if lat and lng:
            comment = packet.get('comment', '')
            speed   = packet.get('speed')
            alt     = packet.get('altitude')
            if speed is not None or alt is not None:
                speed_val  = float(speed) * 3.6 if speed is not None else 0
                alt_val    = float(alt) if alt is not None else 0
                extra_parts = []
                if speed is not None:
                    extra_parts.append(f"Hız: {speed_val:.1f}km/h")
                if alt is not None:
                    extra_parts.append(f"Rkm: {alt_val:.0f}m")
                if extra_parts:
                    extra_str = ", ".join(extra_parts)
                    comment = f"{comment} [{extra_str}]" if comment else f"[{extra_str}]"

            symbol       = packet.get('symbol', '')
            symbol_table = packet.get('symbol_table', '')
            c.execute(
                "INSERT INTO location_history (callsign, lat, lon, comment, timestamp, symbol, symbol_table) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (pkt_callsign, lat, lng, comment, now, symbol, symbol_table)
            )

        # MESAJ LOGLA  (BUG FIX: .trim() → .strip())
        if packet.get('format') == 'message':
            receiver = str(packet.get('addresse') or '').strip()
            msg_text = str(packet.get('message_text') or '').strip()

            if not msg_text and packet.get('response'):
                resp = packet.get('response')
                msg_no = packet.get('msgNo', '')
                if resp == 'ack':
                    msg_text = f"✅ [İletildi - ACK] Mesaj No: {msg_no}"
                elif resp == 'rej':
                    msg_text = f"❌ [Reddedildi - REJ] Mesaj No: {msg_no}"
                else:
                    msg_text = f"[Sistem: {resp}]"

            c.execute(
                "INSERT INTO message_history (sender, receiver, message_text, timestamp) VALUES (?, ?, ?, ?)",
                (pkt_callsign, receiver, msg_text, now)
            )

        conn.commit()
        conn.close()
    except Exception as e:
        print(f"DB Log Error: {e}")


def aprs_listener():
    """
    APRS-IS'e bağlanır, paketleri işler.
    Bağlantı kopunca 30 saniye bekleyip yeniden bağlanır.
    """
    global AIS
    RECONNECT_DELAY = 30  # saniye

    while True:
        try:
            cfg = load_config()  # Her bağlantıda güncel config oku
            callsign = cfg.get("aprs", {}).get("callsign", "N0CALL")
            passcode = cfg.get("aprs", {}).get("passcode", "-1")
            server = cfg.get("aprs", {}).get("server", "euro.aprs2.net")
            port = int(cfg.get("aprs", {}).get("port", 14580))
            tracked = cfg.get('tracked_callsigns', [])

            filter_str = build_filter(callsign, tracked)

            AIS = aprslib.IS(callsign, passwd=passcode, port=port, host=server)
            AIS.set_filter(filter_str)
            AIS.connect()
            print(f"APRS-IS Bağlantısı Başarılı ({server}:{port}). Filtre: {filter_str}")

            def process_global_packet(raw_line):
                # Gelen raw byteları stringe çevir
                if isinstance(raw_line, bytes):
                    try:
                        line_str = raw_line.decode('utf-8', 'ignore')
                    except:
                        line_str = str(raw_line)
                else:
                    line_str = raw_line

                # Globalden gelen (takip listesindeki) araç konumlarını yerel ağdaki cihazlara yankıla
                for client_fd in tcp_clients:
                    try:
                        client_fd.write(line_str.strip() + "\r\n")
                        client_fd.flush()
                    except: pass
                
                try:
                    packet = aprslib.parse(raw_line)
                    process_parsed_packet(packet)
                except Exception:
                    pass

            AIS.consumer(process_global_packet, raw=True)

        except Exception as e:
            print(f"APRS Listener Hatası: {e}. {RECONNECT_DELAY} saniye sonra yeniden bağlanılıyor...")
            AIS = None
            eventlet.sleep(RECONNECT_DELAY)

def handle_tcp_client(sock, address):
    print(f"[{address[0]}:{address[1]}] Özel APRS istemcisi bağlandı.")
    fd = sock.makefile('rw')
    try:
        login_line = fd.readline()
        if not login_line: return
        print(f"[{address[0]}:{address[1]}] Login: {login_line.strip()}")
        
        if login_line.lower().startswith("user"):
            parts = login_line.split()
            callsign = parts[1] if len(parts) > 1 else "UNKNOWN"
            fd.write(f"# logresp {callsign} unverified, server aprsrx\r\n")
            fd.flush()
            print(f"[{address[0]}:{address[1]}] Login kabul edildi: {callsign}")
            
        tcp_clients.append(fd)
        
        while True:
            line = fd.readline()
            if not line: break
            line = line.strip()
            if not line or line.startswith('#'): continue
            
            try:
                # SADECE kendi sunucumuza bağlı TCP istemcilerine gönder (Kendisi hariç)
                for client_fd in tcp_clients:
                    if client_fd != fd:
                        try:
                            client_fd.write(line + "\r\n")
                            client_fd.flush()
                        except: pass

                packet = aprslib.parse(line)
                process_parsed_packet(packet)
            except aprslib.ParseError:
                pass
            except Exception as e:
                print(f"[{address[0]}:{address[1]}] Paket işleme hatası: {e}")
    except Exception as e:
        print(f"[{address[0]}:{address[1]}] TCP İstemci hatası: {e}")
    finally:
        if fd in tcp_clients:
            tcp_clients.remove(fd)
        sock.close()
        print(f"[{address[0]}:{address[1]}] İstemci ayrıldı.")

def aprs_tcp_server():
    cfg = load_config()
    port = int(cfg.get("aprs", {}).get("tcp_port", 14581))
    try:
        server = eventlet.listen(('0.0.0.0', port))
        print(f"🚀 Özel APRS-IS TCP sunucusu {port} portunda başlatıldı...")
        while True:
            new_sock, address = server.accept()
            eventlet.spawn(handle_tcp_client, new_sock, address)
    except Exception as e:
        print(f"❌ TCP Sunucu başlatılamadı: {e}")

@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        password = request.form.get('password', '')
        if password == APP_PASSWORD:
            session['logged_in'] = True
            return redirect(url_for('index', v=4))
        else:
            error = 'Hatalı şifre! Lütfen tekrar deneyin.'
    return render_template('login.html', error=error)

@app.route('/auto-login')
def auto_login():
    """Mobil uygulama için otomatik giriş endpoint'i"""
    session['logged_in'] = True
    return redirect(url_for('index'))

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/manifest.json')
def manifest():
    return send_from_directory('static', 'manifest.json')

@app.route('/sw.js')
def service_worker():
    response = make_response(send_from_directory('static', 'sw.js'))
    response.headers['Cache-Control'] = 'no-cache'
    return response

@app.route('/')
@login_required
def index():
    response = make_response(render_template('index.html', config=config))
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response

@app.route('/tracker')
@login_required
def tracker():
    # Mobil cihazlar için özel takip sayfası
    return render_template('tracker.html')

@app.route('/api/history')
@login_required
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
                now = datetime.datetime.now()
                dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
                start_ts = dt.timestamp()
                end_ts = start_ts + 86400
        else:
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
        params = [f"{cs}%" for cs in important_calls]
        
        c.execute(
            f"SELECT callsign, lat, lon, comment, timestamp, symbol, symbol_table FROM location_history "
            f"WHERE timestamp >= ? AND timestamp < ? AND ({loc_like_clauses}) ORDER BY timestamp ASC",
            [start_ts, end_ts] + params
        )
        locations = [dict(row) for row in c.fetchall()]
        
        c.execute(
            f"SELECT sender, receiver, message_text, timestamp FROM message_history "
            f"WHERE timestamp >= ? AND timestamp < ? AND ({msg_like_clauses}) ORDER BY timestamp ASC",
            [start_ts, end_ts] + params
        )
        messages = [dict(row) for row in c.fetchall()]
        
        conn.close()
        response = Response(
            json.dumps({"status": "success", "locations": locations, "messages": messages}),
            mimetype='application/json'
        )
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        return response
    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)})

@app.route('/api/settings', methods=['GET', 'POST'])
@login_required
def api_settings():
    cfg = load_config()
    if request.method == 'POST':
        data = request.json
        if data and 'tracked_callsigns' in data:
            # Temizle ve kaydet (kullanıcı * koymuşsa temizle)
            callsigns = [cs.strip().upper().replace('*', '') for cs in data['tracked_callsigns'] if cs.strip()]
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

@app.route('/api/queue')
@login_required
def api_queue():
    """Giden mesaj kuyruğunu döndür"""
    try:
        conn = sqlite3.connect(DB_FILE)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            """
            SELECT id, sender, receiver, message_text, queued_at,
                   status, attempts, last_attempt, sent_at
            FROM outgoing_queue
            ORDER BY queued_at DESC
            LIMIT 50
            """
        )
        rows = [dict(r) for r in c.fetchall()]
        conn.close()
        return json.dumps({"status": "success", "queue": rows})
    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)})

@socketio.on('send_message')
def handle_send_message(data):
    target   = data.get('target', '').strip()
    msg      = data.get('message', '').strip()
    callsign = data.get('callsign', '').strip()

    if not target or not msg:
        return {"status": "error", "message": "Eksik bilgi"}

    cfg     = load_config()
    my_call = callsign.upper() if callsign else cfg.get("aprs", {}).get("callsign", "N0CALL")
    now     = time.time()

    try:
        conn = sqlite3.connect(DB_FILE)
        c    = conn.cursor()

        # 1) Mesaj geçmişine kaydet (chat paneli için)
        c.execute(
            "INSERT INTO message_history (sender, receiver, message_text, timestamp) VALUES (?, ?, ?, ?)",
            (my_call, target, msg, now)
        )

        # 2) Giden kuyruğa ekle (sayfa kapalı olsa bile APRS-IS'e gider)
        c.execute(
            "INSERT INTO outgoing_queue (sender, receiver, message_text, queued_at) VALUES (?, ?, ?, ?)",
            (my_call, target, msg, now)
        )

        conn.commit()
        conn.close()

        # 3) Bağlı web istemcilerine anlık göster (Socket.IO)
        packet_parsed = {
            "from":         my_call,
            "addresse":     target,
            "format":       "message",
            "message_text": msg,
            "timestamp":    now
        }
        socketio.emit('aprs_packet', packet_parsed)

        target_padded = target.ljust(9)[:9]
        packet_raw    = f"{my_call}>APRS,TCPIP*::{target_padded}:{msg}"
        print(f"📥 Kuyruğa eklendi → {packet_raw}")
        return {"status": "success", "packet": packet_raw, "queued": True}

    except Exception as e:
        print(f"Mesaj Kuyruk Hatası: {e}")
        return {"status": "error", "message": str(e)}

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
    symbol = '>'
    symbol_table = '/'
    
    comment_text = "Mobil Tracker"
    if speed or alt:
        comment_text += f" (Hız: {speed:.1f}m/s, Rkm: {alt:.0f}m)"
    
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute(
            "INSERT INTO location_history (callsign, lat, lon, comment, timestamp, symbol, symbol_table) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (callsign, lat, lon, comment_text, now, symbol, symbol_table)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Private Tracker DB Error: {e}")
        
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
    
    # Arka plan görevlerini başlat (eventlet thread)
    eventlet.spawn(aprs_listener)
    eventlet.spawn(aprs_tcp_server)
    eventlet.spawn(message_sender_task)  # ✔ Giden mesaj kuyruğu göndericisi
    
    socketio.run(app, host="0.0.0.0", port=port, debug=False)

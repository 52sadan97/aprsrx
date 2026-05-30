import os
os.environ['EVENTLET_NO_GREENDNS'] = 'yes'
import eventlet
eventlet.monkey_patch()

from flask import Flask, render_template, request
from flask_socketio import SocketIO
import aprslib
import json
import os

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
            
        AIS.consumer(process_packet, raw=False)
    except Exception as e:
        print(f"APRS Listener Hatası: {e}")

@app.route("/")
def index():
    return render_template("index.html")

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

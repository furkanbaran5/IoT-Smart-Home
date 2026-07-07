import threading
import json
import time
import os
import pickle
import logging
from datetime import datetime
import numpy as np
import torch
import torch.nn as nn
import dash
from dash import dcc, html, Input, Output, State, callback_context
import paho.mqtt.client as mqtt
import ssl
# dash-cytoscape required for topology mapping
try:
    import dash_cytoscape as cyto
except ImportError:
    print("dash-cytoscape kurulu değil. Lütfen 'pip install dash-cytoscape' çalıştırın.")
    cyto = None

# Tüm kütüphane loglarını sustur
logging.basicConfig(level=logging.CRITICAL)
logging.getLogger('werkzeug').setLevel(logging.CRITICAL)
logging.getLogger('werkzeug').disabled = True
logging.getLogger('torch').setLevel(logging.CRITICAL)
logging.getLogger('paho').setLevel(logging.CRITICAL)
logging.getLogger('mqtt').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)
logging.getLogger().setLevel(logging.CRITICAL)  # root logger

# ==================== MODEL MIMARISI ====================
class LSTMAutoencoder(nn.Module):
    def __init__(self, input_dim, hidden_dim):
        super(LSTMAutoencoder, self).__init__()
        self.encoder = nn.LSTM(input_dim, hidden_dim, batch_first=True)
        self.decoder = nn.LSTM(hidden_dim, hidden_dim, batch_first=True)
        self.output_layer = nn.Linear(hidden_dim, input_dim)
        
    def forward(self, x):
        _, (hn, _) = self.encoder(x)
        hidden_repeated = hn[-1].unsqueeze(1).repeat(1, x.size(1), 1)
        decoded_out, _ = self.decoder(hidden_repeated)
        out = self.output_layer(decoded_out)
        return out

def load_model_and_params():
    input_dim = 5
    hidden_dim = 32
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    
    model = LSTMAutoencoder(input_dim, hidden_dim).to(device)
    base_dir = os.path.dirname(__file__)
    model_path = os.path.join(base_dir, "model_files", "lstm_autoencoder.pth")
    scaler_pkl_path = os.path.join(base_dir, "model_files", "lstm_scaler.pkl")
    scaler_txt_path = os.path.join(base_dir, "model_files", "scaler_params.txt")
    
    model.load_state_dict(torch.load(model_path, map_location=device))
    model.eval()
    
    with open(scaler_pkl_path, "rb") as f:
        scaler_params = pickle.load(f)
        
    temp_params = {}
    with open(scaler_txt_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or "=" not in line:
                continue
            key, val = line.split("=", 1)
            temp_params[key] = float(val)
            
    scaler_params.update(temp_params)
    return model, scaler_params, device

print("Yapay Zeka Modeli Yükleniyor...")
model, scaler_params, device = load_model_and_params()
print("Model Başarıyla Yüklendi!")

# ==================== GLOBAL STATE ====================
app_data = {
    'connection_status': 'connecting',
    'topology': [], # Ağ haritası için nodelar
    'away_mode': False,
    'show_alerts': True,
    'nodes': {}, # Dinamik sensör verileri
    'logs': [] # Gelen log kayıtları
}

def add_log(node_id, timestamp, data_summary, is_anomaly):
    global app_data
    if 'logs' not in app_data:
        app_data['logs'] = []
    
    app_data['logs'].append({
        'timestamp': timestamp,
        'node_id': node_id,
        'data': data_summary,
        'is_anomaly': is_anomaly
    })
    
    # En fazla 200 log kaydı sakla
    if len(app_data['logs']) > 200:
        app_data['logs'].pop(0)

# ==================== MQTT & ANOMALY LOGIC ====================
MQTT_BROKER = "3bbc547ace4f48ac8a185f2c1e92a5ae.s1.eu.hivemq.cloud"
MQTT_PORT = 8883
MQTT_TOPIC_SUB = "ev/guvenlik/anomali/#"
MQTT_TOPOLOGY_SUB = "ev/guvenlik/topology"

def process_anomali(payload_str, topic):
    global app_data
    try:
        data = json.loads(payload_str)
        if isinstance(data, str): data = json.loads(data)
            
        node_id = data.get("node", "Bilinmeyen")
        raw_data = data.get("data", "")
        
        if "|" not in raw_data: return
            
        parts = raw_data.split("|")
        if len(parts) < 5: return
        prefix = parts[0]
        base_vals = parts[1].split(",")
        if len(base_vals) != 2: return
        
        base_temp = float(base_vals[0]) / 10.0
        t_chars = parts[2]
        p_chars = parts[4]
        
        def decode_delta(c):
            if 'A' <= c <= 'Z': return ord(c) - ord('A')
            if 'a' <= c <= 'z': return ord(c) - ord('a') + 26
            if '0' <= c <= '9': return ord(c) - ord('0') + 52
            return 0
            
        sicakliklar = [decode_delta(c) / 10.0 + base_temp for c in t_chars]
        hareketler = [int(c) for c in p_chars]
        
        now = datetime.now()
        saat = int(data.get("saat", now.hour))
        dakika = int(data.get("dakika", now.minute))
        ay = int(data.get("ay", now.month))
        
        features_list = []
        # Ay bazli Z-Score parametrelerini sec
        mean_key = f"mean_temp_ay{ay}"
        std_key  = f"std_temp_ay{ay}"
        if mean_key in scaler_params and std_key in scaler_params:
            ay_mean = scaler_params[mean_key]
            ay_std  = scaler_params[std_key]
        else:
            ay_mean = scaler_params["mean_temp"]
            ay_std  = scaler_params["std_temp"]
        if ay_std < 1e-6:
            ay_std = 1.0

        for i in range(len(sicakliklar)):
            sicaklik_z = (sicakliklar[i] - ay_mean) / ay_std  # aylik Z-Score
            kesirli_saat = saat + (dakika / 60.0)
            saat_sin = np.sin(2 * np.pi * kesirli_saat / 24.0)
            saat_cos = np.cos(2 * np.pi * kesirli_saat / 24.0)
            ay_sin = np.sin(2 * np.pi * ay / 12.0)
            ay_cos = np.cos(2 * np.pi * ay / 12.0)
            features_list.append([sicaklik_z, saat_sin, saat_cos, ay_sin, ay_cos])
            
        input_tensor = torch.tensor([features_list], dtype=torch.float32).to(device)
        with torch.no_grad():
            reconstruction = model(input_tensor)
            mse = torch.mean((input_tensor - reconstruction) ** 2).item()
            
        threshold = scaler_params["threshold"]

        # --- ANOMALi KARARI (uc kontrol) ---
        avg_temp = float(np.mean(sicakliklar))
        spread   = float(max(sicakliklar) - min(sicakliklar))
        high_avg_limit = ay_mean + 3.5 * ay_std  # ornek: Ocak~18.7C, Haziran~36.6C, Temmuz~40.1C

        lstm_anomaly     = mse > threshold
        spike_anomaly    = spread > 6.0            # 30sn icinde >6C ani sicrama
        high_avg_anomaly = avg_temp > high_avg_limit  # aylik beklentinin cok ustunde

        is_temp_anomaly = lstm_anomaly or spike_anomaly or high_avg_anomaly
        hareket_count = sum(hareketler)
        is_motion_anomaly = app_data.get('away_mode', False) and (hareket_count >= 1)
        
        is_anomaly = is_temp_anomaly or is_motion_anomaly
        
        # Hangi kosul tetikledigini belirle
        reasons = []
        if lstm_anomaly:
            reasons.append(f"LSTM({mse:.4f}>{threshold:.4f})")
        if spike_anomaly:
            reasons.append(f"Ani Sicrama({spread:.1f}C>6C)")
        if high_avg_anomaly:
            reasons.append(f"Yuksek Ort({avg_temp:.1f}C>{high_avg_limit:.1f}C)")
        if is_motion_anomaly:
            reasons.append(f"Hareket({hareket_count})")
        reason_str = " | ".join(reasons) if reasons else "—"

        if is_anomaly:
            if is_motion_anomaly and not is_temp_anomaly:
                status = "ALARM_MOTION"
                status_text = f"Beklenmeyen Hareket! ({hareket_count} Tetik)"
            elif is_temp_anomaly and not is_motion_anomaly:
                status = "ALARM_TEMP"
                status_text = "Sıcaklık Anomalisi!"
            else:
                status = "ALARM_BOTH"
                status_text = "Çifte Alarm!"
        else:
            status = "FALSE_POSITIVE" if (prefix == "Z_ALARM" or prefix == "PIR_ALARM") else "NORMAL"
            status_text = "Sistem Normal"
            
        app_data["nodes"][node_id] = {
            'timestamp': f"{saat:02d}:{dakika:02d}:{now.second:02d}",
            'last_update_time': time.time(),
            'sicakliklar': [float(t) for t in sicakliklar],
            'hareketler': [int(h) for h in hareketler],
            'mse': float(mse),
            'threshold': float(threshold),
            'status': status,
            'status_text': status_text
        }
        
        # Log kaydını ekle
        avg_temp = float(np.mean(sicakliklar))
        hareket_count = sum(hareketler)
        reasons_desc = f" ({reason_str})" if reason_str != "—" else ""
        data_summary = f"Ort. Sıcaklık: {avg_temp:.1f}°C, PIR: {hareket_count} Tetik | {status_text}{reasons_desc}"
        log_time = f"{saat:02d}:{dakika:02d}:{now.second:02d}"
        add_log(node_id, log_time, data_summary, is_anomaly)
    except Exception as e:
        print("Model inference error:", e)

def process_topology(payload_str):
    global app_data
    try:
        data = json.loads(payload_str)
        if data.get("type") == "topology":
            app_data["topology"] = data.get("nodes", [])
            # Topology içindeki node'ları da app_data['nodes'] içine ekle (eğer yoksa)
            for node_entry in data.get("nodes", []):
                nid = node_entry.get("id")
                if nid and nid not in app_data["nodes"]:
                    app_data["nodes"][nid] = {
                        'timestamp': datetime.now().strftime("%H:%M:%S"),
                        'last_update_time': time.time(),
                        'sicakliklar': [0.0]*30, 'hareketler': [0]*30,
                        'mse': 0.0, 'threshold': 0.0,
                        'status': 'ONLINE', 'status_text': 'Sistem Bağlı'
                    }
    except Exception as e:
        print("Topology parsing error:", e)

def process_heartbeat(node_id, data_str):
    """Gateway'in MQTT'ye ilettiği heartbeat paketini işle.
    Format: T:25.8|H:69.0|ALIVE|N:GW;N_42DC
    Bu paketlerin sensör verisi yoktur ama node online'dır, arayüzde gösterilmeli.
    """
    global app_data
    try:
        temp, hum = 0.0, 0.0
        parts = data_str.split("|")
        for p in parts:
            if p.startswith("T:"):
                try: temp = float(p[2:])
                except: pass
            elif p.startswith("H:"):
                try: hum = float(p[2:])
                except: pass

        if "nodes" not in app_data:
            app_data["nodes"] = {}

        existing = app_data["nodes"].get(node_id, {})
        # Eğer bu node için gerçek anomali verisi zaten varsa ezip geçme
        # Sadece last_update_time ve temel T/H bilgisini güncelle
        sicakliklar = existing.get('sicakliklar', [temp] * 30)
        if temp > 0:
            sicakliklar = sicakliklar[1:] + [temp]   # Kayan pencere

        app_data["nodes"][node_id] = {
            'timestamp': datetime.now().strftime("%H:%M:%S"),
            'last_update_time': time.time(),
            'sicakliklar': sicakliklar,
            'hareketler': existing.get('hareketler', [0] * 30),
            'mse': existing.get('mse', 0.0),
            'threshold': existing.get('threshold', 0.0),
            'status': existing.get('status', 'NORMAL'),
            'status_text': existing.get('status_text', 'Sistem Normal'),
        }
        
        # Log kaydını ekle
        data_summary = f"Heartbeat - Sıcaklık: {temp:.1f}°C, Nem: {hum:.1f}% | Cihaz Aktif"
        log_time = datetime.now().strftime("%H:%M:%S")
        add_log(node_id, log_time, data_summary, False)
    except Exception as e:
        print("Heartbeat parse error:", e)

def on_connect(client, userdata, flags, rc, properties=None):
    print("Connected to MQTT Broker!")
    app_data['connection_status'] = 'connected'
    client.subscribe([(MQTT_TOPIC_SUB, 0), (MQTT_TOPOLOGY_SUB, 0)])

def on_disconnect(client, userdata, rc):
    print("Disconnected from MQTT Broker")
    app_data['connection_status'] = 'disconnected'

def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode('utf-8')
    if topic == MQTT_TOPOLOGY_SUB:
        process_topology(payload)
    else:
        try:
            data = json.loads(payload)
            if isinstance(data, str): data = json.loads(data)
            node_id  = data.get("node", "")
            raw_data = data.get("data", "")
            # Heartbeat: "T:25.8|H:69.0|ALIVE|N:..." formatı
            # Anomali:   "Z_ALARM|245,60|ABCDE...|..." formatı (| ile ayrılmış, ilk part prefix)
            if raw_data.startswith(("Z_ALARM", "PIR_ALARM", "P_ALARM")):
                process_anomali(payload, topic)
            elif "ALIVE" in raw_data or raw_data.startswith("T:"):
                if node_id:
                    process_heartbeat(node_id, raw_data)
            else:
                process_anomali(payload, topic)
        except Exception:
            process_anomali(payload, topic)

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqtt_client.on_connect = on_connect
mqtt_client.on_disconnect = on_disconnect
mqtt_client.on_message = on_message

mqtt_client.username_pw_set("cabir", "Cabir123")
mqtt_client.tls_set(tls_version=ssl.PROTOCOL_TLS)

def start_mqtt():
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.loop_forever()
    except Exception as e:
        print("MQTT Connection Error:", e)
        app_data['connection_status'] = 'disconnected'

threading.Thread(target=start_mqtt, daemon=True).start()

# ==================== KİMLİK BİLGİLERİ ====================
VALID_USERNAME = "123"
VALID_PASSWORD = "123"

# ==================== DASH APP ====================
app = dash.Dash(__name__, title="IoT Mesh Dashboard", suppress_callback_exceptions=True, update_title=None)
app.logger.setLevel(logging.ERROR)

# ==================== LOGIN SAYFASI LAYOUT ====================
login_layout = html.Div(
    id='login-page',
    style={
        'minHeight': '100vh',
        'display': 'flex',
        'alignItems': 'center',
        'justifyContent': 'center',
        'backgroundColor': '#f4f7f6',
        'position': 'fixed',
        'top': '0', 'left': '0', 'right': '0', 'bottom': '0',
        'zIndex': '9999'
    },
    children=[
        html.Div(
            style={
                'background': 'rgba(255, 255, 255, 0.9)',
                'border': '1px solid rgba(226, 232, 240, 0.8)',
                'borderRadius': '24px',
                'padding': '3rem 3.5rem',
                'width': '100%',
                'maxWidth': '420px',
                'boxShadow': '0 20px 40px rgba(0,0,0,0.08), 0 4px 6px -1px rgba(0,0,0,0.04)',
                'display': 'flex',
                'flexDirection': 'column',
                'alignItems': 'center',
                'gap': '0',
            },
            children=[
                # Logo / Ikon
                html.Div(
                    '🛡️',
                    style={
                        'fontSize': '3.5rem',
                        'marginBottom': '0.75rem',
                    }
                ),
                html.H1(
                    'IoT Mesh',
                    style={
                        'fontSize': '1.8rem',
                        'fontWeight': '800',
                        'margin': '0',
                        'letterSpacing': '-0.5px',
                        'background': 'linear-gradient(135deg, #0f172a 0%, #334155 100%)',
                        'WebkitBackgroundClip': 'text',
                        'WebkitTextFillColor': 'transparent',
                    }
                ),
                html.P(
                    'Güvenlik Paneli',
                    style={
                        'color': '#64748b',
                        'fontSize': '0.9rem',
                        'marginTop': '0.3rem',
                        'marginBottom': '2rem'
                    }
                ),
                # Kullanıcı Adı
                html.Div(
                    style={'width': '100%', 'marginBottom': '1rem'},
                    children=[
                        html.Label(
                            'Kullanıcı Adı',
                            style={'color': '#64748b', 'fontSize': '0.8rem', 'fontWeight': '600',
                                   'letterSpacing': '0.05em', 'marginBottom': '0.4rem', 'display': 'block'}
                        ),
                        dcc.Input(
                            id='login-username',
                            type='text',
                            placeholder='Kullanıcı adınızı girin',
                            debounce=False,
                            style={
                                'width': '100%',
                                'display': 'block',
                                'padding': '10px 14px',
                                'height': '44px',
                                'lineHeight': '22px',
                                'borderRadius': '12px',
                                'border': '1px solid rgba(226, 232, 240, 0.8)',
                                'background': '#f8fafc',
                                'color': '#1e293b',
                                'fontSize': '0.95rem',
                                'outline': 'none',
                                'boxSizing': 'border-box',
                                'fontFamily': 'Outfit, sans-serif',
                                'overflow': 'visible',
                            }
                        )
                    ]
                ),
                # Şifre
                html.Div(
                    style={'width': '100%', 'marginBottom': '0.5rem'},
                    children=[
                        html.Label(
                            'Şifre',
                            style={'color': '#64748b', 'fontSize': '0.8rem', 'fontWeight': '600',
                                   'letterSpacing': '0.05em', 'marginBottom': '0.4rem', 'display': 'block'}
                        ),
                        dcc.Input(
                            id='login-password',
                            type='password',
                            placeholder='Şifrenizi girin',
                            debounce=False,
                            style={
                                'width': '100%',
                                'display': 'block',
                                'padding': '10px 14px',
                                'height': '44px',
                                'lineHeight': '22px',
                                'borderRadius': '12px',
                                'border': '1px solid rgba(226, 232, 240, 0.8)',
                                'background': '#f8fafc',
                                'color': '#1e293b',
                                'fontSize': '0.95rem',
                                'outline': 'none',
                                'boxSizing': 'border-box',
                                'fontFamily': 'Outfit, sans-serif',
                                'overflow': 'visible',
                            }
                        )
                    ]
                ),
                # Hata mesajı
                html.Div(
                    id='login-error-msg',
                    style={
                        'color': '#ef4444',
                        'fontSize': '0.82rem',
                        'minHeight': '1.4rem',
                        'width': '100%',
                        'textAlign': 'center',
                        'marginTop': '0.4rem',
                        'marginBottom': '0.6rem'
                    }
                ),
                # Giriş Butonu — mevcut .btn .btn-motion renk paleti
                html.Button(
                    'Giriş Yap →',
                    id='login-btn',
                    n_clicks=0,
                    className='btn btn-motion',
                    style={
                        'width': '100%',
                        'marginTop': '0.5rem',
                        'padding': '0.85rem',
                        'fontSize': '1rem',
                        'fontWeight': '700',
                        'borderRadius': '12px',
                        'cursor': 'pointer',
                    }
                ),
                html.P(
                    'IoT Mesh Security Dashboard v2.0',
                    style={'color': '#94a3b8', 'fontSize': '0.72rem', 'marginTop': '2rem', 'marginBottom': '0'}
                )
            ]
        )
    ]
)

app.layout = html.Div(className="app-container", children=[
    dcc.Store(id='login-store', data={'logged_in': False}),
    dcc.Interval(id='interval-update', interval=1000, n_intervals=0),
    dcc.Store(id='active-tab-store', data='broker'),
    dcc.Store(id='expanded-nodes-store', data=[]),
    dcc.Store(id='dismissed-alarms-store', data={}),
    
    # Login sayfası (başlangıçta görünür)
    login_layout,

    # Ana dashboard (başlangıçta gizli)
    html.Div(id='main-dashboard', style={'display': 'none'}, children=[

    html.Header(className="app-header", children=[
        html.Div(className="brand-section", children=[
            html.H1("IoT Mesh Güvenlik Paneli"),
            html.P("Yapay Zeka, Kural Tabanlı Hibrit Anomali Tespiti ve Ağ Haritası")
        ]),
        html.Div(className="header-controls", children=[
            html.Button("🔔 Uyarıları Kapat", id="btn-toggle-alerts", n_clicks=0, className="nav-tab-btn", style={'backgroundColor': '#f59e0b', 'color': 'white', 'marginRight': '10px'}),
            html.Button("🏠 Evdeyim", id="btn-away-mode", n_clicks=0, className="nav-tab-btn", style={'backgroundColor': '#10b981', 'color': 'white', 'marginRight': '15px'}),
            html.Nav(className="nav-tabs", children=[
                html.Button("🖥️ Broker Paneli", id="btn-tab-broker", className="nav-tab-btn active"),
                html.Button("🗺️ Ağ Haritası", id="btn-tab-topology", className="nav-tab-btn"),
                html.Button("📜 Node Logları", id="btn-tab-logs", className="nav-tab-btn"),
                html.Button("🧪 Test Paneli", id="btn-tab-test", className="nav-tab-btn")
            ]),
            html.Div(className="connection-status", children=[
                html.Span(id="conn-dot", className="status-dot connecting"),
                html.Span(id="conn-text", children="Bağlanıyor...")
            ])
        ])
    ]),
    
    html.Main(id="main-content", className="dashboard-grid", children=[
        # Özet Bar — her zaman görünür
        html.Div(style={'gridColumn': '1/-1'}, children=[
            html.Div(id="summary-bar-content", className="summary-bar",
                     children=[html.Span("Cihaz verisi bekleniyor...", style={'color':'#94a3b8','fontSize':'0.85rem'})])
        ]),
        html.Div(id="broker-panel", style={'display': 'block', 'gridColumn': '1/-1'}, children=[
            html.Div(id="broker-content")
        ]),
        html.Div(id="topology-panel", style={'display': 'none', 'gridColumn': '1/-1'}, children=[
            html.Div(className="card", style={'height': '600px', 'padding': '0'}, children=[
                html.Div(style={'padding': '1rem', 'borderBottom': '1px solid #e2e8f0'}, children=[
                    html.H3("🗺️ Canlı Ağ Topolojisi (Node Routing Map)", style={'margin':'0'}),
                    html.P("Gateway tarafından gönderilen Sync paketleriyle oluşturulan ağ haritası.", style={'fontSize':'0.85rem','color':'#64748b','margin':'0.5rem 0 0 0'})
                ]),
                cyto.Cytoscape(
                    id='cytoscape-network',
                    elements=[],
                    layout={'name': 'breadthfirst', 'roots': '[id = "GW"]'},
                    style={'width': '100%', 'height': '100%'},
                    stylesheet=[
                        {'selector': 'node', 'style': {'label': 'data(label)', 'text-wrap': 'wrap', 'text-valign': 'center', 'text-halign': 'center', 'font-size': '12px', 'font-weight': 'bold', 'color': '#ffffff', 'text-outline-color': '#000', 'text-outline-width': '1px'}},
                        {'selector': '.gateway-node', 'style': {'background-color': '#3b82f6', 'shape': 'hexagon', 'width': '60px', 'height': '60px'}},
                        {'selector': '.sensor-node', 'style': {'background-color': 'data(status)', 'shape': 'ellipse', 'width': '50px', 'height': '50px'}},
                        {'selector': 'edge', 'style': {'width': 3, 'line-color': '#94a3b8', 'target-arrow-color': '#94a3b8', 'target-arrow-shape': 'triangle', 'curve-style': 'bezier'}}
                    ]
                ) if cyto else html.Div("dash-cytoscape kurulu değil.", style={'padding': '2rem'})
            ])
        ]),
        html.Div(id="test-panel", style={'display': 'none', 'gridColumn': '1/-1'}, children=[
            html.Div(className="card", style={'background':'linear-gradient(135deg, #f8fafc 0%, #eff6ff 100%)', 'marginBottom': '1.5rem'}, children=[
                html.H3("🧪 Simülatör ve Özel Parametre Test Alanı", style={'fontSize':'1.25rem','fontWeight':700,'color':'var(--text-primary)'}),
                html.P("Buradan gönderilen simülasyon paketleri MQTT üzerinden doğrudan LSTM modeline aktarılır.", style={'fontSize':'0.9rem','color':'var(--text-secondary)'})
            ]),
            html.Div(style={'display':'grid', 'gridTemplateColumns':'1fr 1fr', 'gap':'1.5rem'}, children=[
                html.Section(className="card", children=[
                    html.H3("⌨️ İnteraktif Parametre Test Paneli", className="card-title"),
                    html.Div(style={'display':'flex','flexDirection':'column','gap':'1rem'}, children=[
                        html.Div(style={'display':'grid','gridTemplateColumns':'repeat(auto-fit, minmax(110px, 1fr))','gap':'0.75rem'}, children=[
                            html.Div(style={'display':'flex','flexDirection':'column','gap':'0.25rem'}, children=[
                                html.Label("Sıcaklık (°C)", style={'fontSize':'0.75rem','fontWeight':600}),
                                dcc.Input(id="input-temp", type="number", value=22.5, step=0.1, style={'padding':'0.5rem','borderRadius':'8px','border':'1px solid var(--border-card)'})
                            ]),
                            html.Div(style={'display':'flex','flexDirection':'column','gap':'0.25rem'}, children=[
                                html.Label("Hareket (0-30)", style={'fontSize':'0.75rem','fontWeight':600}),
                                dcc.Input(id="input-motion", type="number", value=0, min=0, max=30, style={'padding':'0.5rem','borderRadius':'8px','border':'1px solid var(--border-card)'})
                            ]),
                            html.Div(style={'display':'flex','flexDirection':'column','gap':'0.25rem'}, children=[
                                html.Label("Saat (0-23)", style={'fontSize':'0.75rem','fontWeight':600}),
                                dcc.Input(id="input-hr", type="number", value=14, min=0, max=23, style={'padding':'0.5rem','borderRadius':'8px','border':'1px solid var(--border-card)'})
                            ]),
                            html.Div(style={'display':'flex','flexDirection':'column','gap':'0.25rem'}, children=[
                                html.Label("Dakika (0-59)", style={'fontSize':'0.75rem','fontWeight':600}),
                                dcc.Input(id="input-min", type="number", value=30, min=0, max=59, style={'padding':'0.5rem','borderRadius':'8px','border':'1px solid var(--border-card)'})
                            ]),
                            html.Div(style={'display':'flex','flexDirection':'column','gap':'0.25rem'}, children=[
                                html.Label("Ay (1-12)", style={'fontSize':'0.75rem','fontWeight':600}),
                                dcc.Input(id="input-mo", type="number", value=6, min=1, max=12, style={'padding':'0.5rem','borderRadius':'8px','border':'1px solid var(--border-card)'})
                            ]),
                        ]),
                        html.Button("🚀 Değerleri Gönder", id="btn-custom-sim", n_clicks=0, className="btn btn-motion")
                    ])
                ]),
                html.Section(className="card", children=[
                    html.H3("⚡ Hızlı Hazır Senaryolar", className="card-title"),
                    html.Div(className="simulation-buttons-grid", children=[
                        html.Button("🟢 Normal Sıcaklık (22.5°C)", id="btn-sim-normal", n_clicks=0, className="btn btn-normal"),
                        html.Button("🔥 Yangın Anomalisi (45.0°C)", id="btn-sim-alarm", n_clicks=0, className="btn btn-alarm"),
                        html.Button("👣 Gece Hareketi (5 Tetikleme)", id="btn-sim-night", n_clicks=0, className="btn btn-motion")
                    ]),
                    html.Div(id="sim-dummy-output", style={'display':'none'})
                ])
            ])
        ]),
        html.Div(id="logs-panel", style={'display': 'none', 'gridColumn': '1/-1'}, children=[
            html.Div(className="card", children=[
                html.Div(style={'display': 'flex', 'justifyContent': 'space-between', 'alignItems': 'center', 'borderBottom': '1px solid #e2e8f0', 'paddingBottom': '1rem'}, children=[
                    html.Div(children=[
                        html.H3("📜 Canlı Node Log Kayıtları", style={'margin':'0', 'fontSize': '1.25rem', 'fontWeight': 700}),
                        html.P("Cihazlardan gelen tüm anlık verilerin ve anomali durumlarının geçmişi.", style={'fontSize':'0.85rem','color':'#64748b','margin':'0.5rem 0 0 0'})
                    ]),
                    html.Button("🧹 Logları Temizle", id="btn-clear-logs", n_clicks=0, className="btn-dismiss-alarm", style={'padding': '0.5rem 1rem', 'borderRadius': '12px'})
                ]),
                html.Div(
                    id="logs-container",
                    className="logs-container",
                    children=[
                        html.Div("Henüz log kaydı bulunmuyor...", style={'textAlign': 'center', 'color': '#64748b', 'padding': '2rem'})
                    ]
                )
            ])
        ])
    ])

    ]) # /main-dashboard
])
# ==================== LOGIN CALLBACK ====================
@app.callback(
    Output('login-page', 'style'),
    Output('main-dashboard', 'style'),
    Output('login-error-msg', 'children'),
    Input('login-btn', 'n_clicks'),
    Input('login-password', 'n_submit'),
    State('login-username', 'value'),
    State('login-password', 'value'),
    prevent_initial_call=True
)
def handle_login(n_clicks, n_submit, username, password):
    hidden_login = {
        'display': 'none'
    }
    shown_login = {
        'minHeight': '100vh', 'display': 'flex', 'alignItems': 'center',
        'justifyContent': 'center',
        'backgroundColor': '#f4f7f6',
        'position': 'fixed', 'top': '0', 'left': '0', 'right': '0', 'bottom': '0',
        'zIndex': '9999'
    }
    shown_dashboard = {'display': 'block'}
    hidden_dashboard = {'display': 'none'}

    if not username or not password:
        return shown_login, hidden_dashboard, '⚠️ Kullanıcı adı ve şifre boş bırakılamaz.'

    if username.strip() == VALID_USERNAME and password.strip() == VALID_PASSWORD:
        return hidden_login, shown_dashboard, ''
    else:
        return shown_login, hidden_dashboard, '❌ Kullanıcı adı veya şifre hatalı.'

def get_status_class(status):
    if status == 'NORMAL': return 'status-normal'
    if status == 'FALSE_POSITIVE': return 'status-false_positive'
    if status in ['ALARM_MOTION', 'ALARM_TEMP', 'ALARM_BOTH']: return 'status-alarm'
    return 'status-offline'

def get_status_icon(status):
    if status == 'NORMAL':          return '🛡️'
    if status == 'ONLINE':          return '📡'
    if status == 'FALSE_POSITIVE':  return '🔹'
    if status == 'ALARM_MOTION':    return '👣'
    if status == 'ALARM_TEMP':      return '🔥'
    if status == 'ALARM_BOTH':      return '⚠️'
    if status == 'OFFLINE':         return '💤'
    return '📡'


# Sekme Değiştirme (Sadece butonlara tıklandığında çalışır, DOM'u silmez)
@app.callback(
    Output("btn-tab-broker", "className"),
    Output("btn-tab-topology", "className"),
    Output("btn-tab-logs", "className"),
    Output("btn-tab-test", "className"),
    Output("broker-panel", "style"),
    Output("topology-panel", "style"),
    Output("logs-panel", "style"),
    Output("test-panel", "style"),
    Input("btn-tab-broker", "n_clicks"),
    Input("btn-tab-topology", "n_clicks"),
    Input("btn-tab-logs", "n_clicks"),
    Input("btn-tab-test", "n_clicks"),
    prevent_initial_call=False
)
def switch_tabs(n_b, n_top, n_l, n_t):
    ctx = callback_context
    triggered_id = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered else "btn-tab-broker"

    b_cls = topo_cls = logs_cls = test_cls = "nav-tab-btn"
    b_sty = topo_sty = logs_sty = test_sty = {'display': 'none'}

    if triggered_id == "btn-tab-topology":
        topo_cls = "nav-tab-btn active"
        topo_sty = {'display': 'block', 'gridColumn': '1/-1'}
    elif triggered_id == "btn-tab-logs":
        logs_cls = "nav-tab-btn active"
        logs_sty = {'display': 'block', 'gridColumn': '1/-1'}
    elif triggered_id == "btn-tab-test":
        test_cls = "nav-tab-btn active"
        test_sty = {'display': 'block', 'gridColumn': '1/-1'}
    else:
        b_cls = "nav-tab-btn active"
        b_sty = {'display': 'contents'}

    return b_cls, topo_cls, logs_cls, test_cls, b_sty, topo_sty, logs_sty, test_sty

# Evdeyim / Dışarıdayım Modu
@app.callback(
    Output("btn-away-mode", "children"),
    Output("btn-away-mode", "style"),
    Input("btn-away-mode", "n_clicks"),
    prevent_initial_call=True
)
def toggle_away_mode(n_clicks):
    is_away = (n_clicks % 2) == 1
    app_data['away_mode'] = is_away
    
    payload = json.dumps({"away": is_away})
    mqtt_client.publish("ev/guvenlik/away_mode", payload)
    
    if is_away:
        return "🔒 Dışarıdayım", {'backgroundColor': '#ef4444', 'color': 'white', 'marginRight': '15px'}
    else:
        return "🏠 Evdeyim", {'backgroundColor': '#10b981', 'color': 'white', 'marginRight': '15px'}

# Uyarıları Gizle / Göster
@app.callback(
    Output("btn-toggle-alerts", "children"),
    Output("btn-toggle-alerts", "style"),
    Input("btn-toggle-alerts", "n_clicks"),
    prevent_initial_call=True
)
def toggle_alerts(n_clicks):
    show = (n_clicks % 2) == 0
    app_data['show_alerts'] = show
    if show:
        return "🔔 Uyarıları Kapat", {'backgroundColor': '#f59e0b', 'color': 'white', 'marginRight': '10px'}
    else:
        return "🔕 Uyarıları Aç", {'backgroundColor': '#64748b', 'color': 'white', 'marginRight': '10px'}

# Sadece Broker verilerini periyodik günceller
@app.callback(
    Output("broker-content", "children"),
    Output("summary-bar-content", "children"),
    Input("interval-update", "n_intervals"),
    State('expanded-nodes-store', 'data'),
    State('dismissed-alarms-store', 'data'),
)
def update_broker(n, expanded_nodes, dismissed_alarms):
    nodes = app_data.get('nodes', {})
    expanded_nodes = expanded_nodes or []
    dismissed_alarms = dismissed_alarms or {}

    if not nodes:
        empty = html.Div("Henüz cihazlardan veri alınmadı...",
                         style={'textAlign':'center','padding':'3rem','color':'#64748b','fontSize':'1.2rem'})
        bar = [html.Span("Cihaz verisi bekleniyor...", style={'color':'#94a3b8','fontSize':'0.85rem'})]
        return empty, bar

    topology_nodes = app_data.get('topology', [])
    topo_dict = {nd.get('id', ''): nd.get('lastSeenSec', 999) for nd in topology_nodes}

    all_cards = []
    summary_chips = []

    for node_id, data in nodes.items():
        seconds_since_last = topo_dict.get(node_id, 999)
        if seconds_since_last == 999:
            seconds_since_last = time.time() - data.get('last_update_time', 0)

        is_offline = seconds_since_last > 20

        if is_offline:
            status = 'OFFLINE'
            status_text = f"BAĞLANTI KOPTU! ({int(seconds_since_last)} sn önce)"
        else:
            status = data.get('status', 'OFFLINE')
            status_text = data.get('status_text', '')

        if not app_data.get('show_alerts', True) and 'ALARM' in status:
            status = 'NORMAL'
            status_text = "Uyarılar Gizlendi"

        # Dismiss kontrolü: aynı status_text dismiss edildiyse maskele
        dismissed_text = dismissed_alarms.get(node_id)
        if dismissed_text and dismissed_text == status_text and 'ALARM' in status:
            status = 'NORMAL'
            status_text = "Alarm Onaylandı ✓"

        icon = get_status_icon(status)
        sicakliklar = data.get('sicakliklar', [20]*30)
        hareketler  = data.get('hareketler', [0]*30)
        last_temp   = sicakliklar[-1] if sicakliklar else 0.0
        motion_cnt  = sum(hareketler)
        mse         = data.get('mse', 0)
        threshold   = data.get('threshold', 0)

        is_alarm   = 'ALARM' in status
        is_offline2 = status == 'OFFLINE'

        row_cls   = 'row-alarm' if is_alarm else ('row-offline' if is_offline2 else 'row-normal')
        badge_cls = 'badge-alarm' if is_alarm else ('badge-offline' if is_offline2 else 'badge-normal')
        chip_cls  = 'chip-alarm' if is_alarm else ('chip-offline' if is_offline2 else 'chip-normal')

        # ── Özet bar chip ──
        summary_chips.append(
            html.Div(className=f"summary-chip {chip_cls}", children=[
                html.Span(icon),
                html.Strong(node_id),
                html.Span(f"{last_temp:.1f}°C  •  {int(seconds_since_last)}s"),
            ])
        )

        # ── Accordion detay ──
        bar_items = []
        for i, temp in enumerate(sicakliklar):
            hp = max(10, min(100, (temp / 50.0) * 100))
            ic = 'anomalous-temp' if is_alarm else 'normal-temp'
            bar_items.append(html.Div(className=f"bar-item {ic}", style={"height": f"{hp}%"},
                                      children=[html.Span(f"Sn {i+1}: {temp:.1f}°C", className="tooltip-custom")]))

        motion_items = []
        for i, val in enumerate(hareketler):
            mc = "motion-block active" if val == 1 else "motion-block"
            motion_items.append(html.Div(className=mc,
                                         children=[html.Span(f"Sn {i+1}: {'Hareket' if val==1 else 'Sakin'}", className="tooltip-custom")]))

        is_expanded = node_id in expanded_nodes
        expand_label = f"▲ Gizle" if is_expanded else "▼ Detaylar"

        detail_panel = html.Div(
            className="node-detail-panel",
            style={'display': 'grid' if is_expanded else 'none'},
            children=[
                html.Div(children=[
                    html.Div(className="visualizer-label",
                             children=[html.Span("🌡️ Sıcaklık Trendi"), html.Span(f"{last_temp:.1f}°C")]),
                    html.Div(bar_items, className="bar-chart")
                ]),
                html.Div(children=[
                    html.Div(className="visualizer-label",
                             children=[html.Span("👣 Hareket (PIR)"), html.Span(f"{motion_cnt} Tetik")]),
                    html.Div(motion_items, className="motion-grid"),
                    html.Div(className="metrics-row", style={'marginTop':'1rem'}, children=[
                        html.Div(className="metric-box", children=[
                            html.P("MSE Skoru", className="metric-label"),
                            html.P(f"{mse:.6f}", className=f"metric-value {'highlight-red' if mse > threshold else ''}")
                        ]),
                        html.Div(className="metric-box", children=[
                            html.P("Eşik", className="metric-label"),
                            html.P(f"{threshold:.6f}", className="metric-value")
                        ]),
                        html.Div(className="metric-box", children=[
                            html.P("Son Paket", className="metric-label"),
                            html.P(data.get('timestamp','—'), className="metric-value", style={'fontSize':'0.9rem'})
                        ]),
                    ])
                ])
            ]
        )

        # ── Yatay header row ──
        header_children = [
            html.Span(f"{icon}", style={'fontSize':'1.2rem'}),
            html.Div(node_id, className="node-row-id"),
            html.Div(className="node-row-stat", children=[html.Span("🌡️"), html.Strong(f"{last_temp:.1f}°C")]),
            html.Div(className="node-row-stat", children=[html.Span("👣"), html.Span(f"{motion_cnt} Tetik")]),
            html.Div(className="node-row-stat", children=[html.Span("⏱️"), html.Span(f"{int(seconds_since_last)}s")]),
            html.Div(className="node-row-spacer"),
            html.Span(status_text, className=f"node-row-badge {badge_cls}"),
        ]

        if is_alarm and dismissed_alarms.get(node_id) != status_text:
            header_children.append(
                html.Button("✓ Alarmı Gördüm",
                            id={'type': 'dismiss-alarm', 'index': node_id},
                            className="btn-dismiss-alarm",
                            n_clicks=0)
            )

        header_children.append(
            html.Button(expand_label,
                        id={'type': 'expand-node', 'index': node_id},
                        className="btn-expand-toggle",
                        n_clicks=0)
        )

        node_card = html.Div(
            className=f"node-row-wrapper {row_cls}",
            children=[
                html.Div(className="node-row-header", children=header_children),
                detail_panel
            ]
        )
        all_cards.append(node_card)

    broker_content = html.Div(all_cards)
    return broker_content, summary_chips


# Accordion toggle
from dash import ALL, MATCH
@app.callback(
    Output('expanded-nodes-store', 'data'),
    Input({'type': 'expand-node', 'index': ALL}, 'n_clicks'),
    State('expanded-nodes-store', 'data'),
    prevent_initial_call=True
)
def toggle_expand(clicks_list, expanded):
    ctx = callback_context
    if not ctx.triggered: return dash.no_update
    triggered = ctx.triggered[0]
    # Her saniye butonlar n_clicks=0 ile yeniden üretilince bu callback tetiklenir.
    # Sadece gerçek bir tıklama (n_clicks > 0) ise işle.
    if not triggered['value'] or triggered['value'] == 0:
        return dash.no_update
    triggered_prop = triggered['prop_id']
    try:
        node_id = json.loads(triggered_prop.split('.')[0])['index']
    except Exception:
        return dash.no_update
    expanded = list(expanded or [])
    if node_id in expanded:
        expanded = [x for x in expanded if x != node_id]
    else:
        expanded.append(node_id)
    return expanded

# Alarm dismiss
@app.callback(
    Output('dismissed-alarms-store', 'data'),
    Input({'type': 'dismiss-alarm', 'index': ALL}, 'n_clicks'),
    State('dismissed-alarms-store', 'data'),
    prevent_initial_call=True
)
def dismiss_alarm_cb(clicks_list, dismissed):
    ctx = callback_context
    if not ctx.triggered: return dash.no_update
    triggered = ctx.triggered[0]
    # Sadece gerçek bir tıklama ise işle
    if not triggered['value'] or triggered['value'] == 0:
        return dash.no_update
    triggered_prop = triggered['prop_id']
    try:
        node_id = json.loads(triggered_prop.split('.')[0])['index']
    except Exception:
        return dash.no_update
    dismissed = dict(dismissed or {})
    node_data = app_data.get('nodes', {}).get(node_id, {})
    dismissed[node_id] = node_data.get('status_text', '__dismissed__')
    return dismissed

# Sadece Topoloji (Harita) verilerini periyodik günceller
@app.callback(
    Output("cytoscape-network", "elements"),
    Input("interval-update", "n_intervals"),
    prevent_initial_call=True
)
def update_topology(n):
    nodes_data = app_data.get('topology', [])
    elements = [{'data': {'id': 'GW', 'label': 'Gateway'}, 'classes': 'gateway-node'}]

    # Her node için SADECE en güncel path'i tut
    node_paths = {}
    for nd in nodes_data:
        nid = nd.get('id', '')
        if nid and nid != 'GW':
            node_paths[nid] = nd

    # ── SPANNING TREE (Loop-free) Algoritması ──────────────────────────────
    # Amaç: Her node'un tam olarak bir "parent" (next-hop) olsun, loop olmasın.
    #
    # Algoritma:
    #  1. Path'leri hop sayısına göre küçükten büyüğe sırala.
    #     Kısa path = gateway'e daha yakın = daha güvenilir rota.
    #  2. Her node için path'teki ilk hop'u parent olarak ata (parent dict).
    #  3. LOOP TESPİTİ: Atamadan önce → "hedef node (next_hop) bizi parent
    #     olarak seçiyor mu?" diye kontrol et.
    #     - Eğer evet → döngü var demektir. Bu node'u doğrudan GW'ye bağla.
    #     - Eğer hayır → ata.
    #
    # Örnek:
    #   N_2198 path "N_2198>N_42DC>GW" (3 hop)  →  parent[N_2198] = N_42DC
    #   N_42DC path "N_42DC>N_2198>GW" (3 hop)  →  parent[N_42DC] önerilir N_2198
    #       Ama parent[N_2198] == N_42DC → loop! → parent[N_42DC] = GW (kır)
    # ─────────────────────────────────────────────────────────────────────

    parent = {}  # {node_id: next_hop_id}

    # Kısa path önce işlensin (az hop = daha güvenilir)
    sorted_nodes = sorted(
        node_paths.items(),
        key=lambda x: len(x[1].get('path', '').split('>'))
    )

    for nid, nd in sorted_nodes:
        path_str = nd.get('path', '')
        if not path_str:
            parent[nid] = 'GW'
            continue
        hops = path_str.split('>')
        if len(hops) < 2:
            parent[nid] = 'GW'
            continue

        next_hop = hops[1]

        # Loop tespiti: next_hop zaten bizi (nid) parent olarak seçiyor mu?
        if parent.get(next_hop) == nid:
            # Loop kırma: doğrudan GW'ye bağla
            next_hop = 'GW'

        parent[nid] = next_hop

    # ── Element'leri oluştur ───────────────────────────────────────────────
    edges = set()
    for nid, nd in node_paths.items():
        last_seen = nd.get('lastSeenSec', 999)
        status_color = "#10b981" if last_seen < 30 else "#ef4444"
        elements.append({
            'data': {'id': nid, 'label': f"{nid}\n({last_seen}s)", 'status': status_color},
            'classes': 'sensor-node'
        })

        next_hop = parent.get(nid, 'GW')
        edge_id = f"{nid}->{next_hop}"
        if edge_id not in edges:
            edges.add(edge_id)
            elements.append({'data': {'id': edge_id, 'source': nid, 'target': next_hop}})

    return elements



@app.callback(
    Output("conn-dot", "className"),
    Output("conn-text", "children"),
    Input("interval-update", "n_intervals")
)
def update_connection(n):
    c = app_data['connection_status']
    if c == 'connected': return "status-dot connected", "Broker Aktif"
    if c == 'connecting': return "status-dot connecting", "Bağlanıyor..."
    return "status-dot disconnected", "Kesildi"

@app.callback(
    Output("sim-dummy-output", "children"),
    Input("btn-custom-sim", "n_clicks"),
    Input("btn-sim-normal", "n_clicks"),
    Input("btn-sim-alarm", "n_clicks"),
    Input("btn-sim-night", "n_clicks"),
    State("input-temp", "value"),
    State("input-motion", "value"),
    State("input-hr", "value"),
    State("input-min", "value"),
    State("input-mo", "value"),
    prevent_initial_call=True
)
def handle_simulations(n1, n2, n3, n4, t, m, hr, min_v, mo):
    ctx = callback_context
    if not ctx.triggered: return ""
    trigger = ctx.triggered[0]['prop_id'].split('.')[0]

    now = datetime.now()
    cur_hr = now.hour
    cur_min = now.minute
    cur_mo = now.month

    if trigger == "btn-custom-sim":
        temp = float(t) if t else 22.5
        mc = int(m) if m else 0
        h = int(hr) if hr else cur_hr
        mn = int(min_v) if min_v else cur_min
        month = int(mo) if mo else cur_mo
        
        prefix = "PIR_ALARM" if mc > 0 else "Z_ALARM"
        base_temp = int(temp)
        base_val = base_temp * 10
        scale_val = 100
        
        # Her örnek için ±2.0°C aralığında rastgele varyasyon ekle (dalgalı görünüm)
        t_chars = ""
        for _ in range(30):
            variation = np.random.uniform(-2.0, 2.0)
            sample_temp = temp + variation
            diff = round((sample_temp - base_temp) * 10)
            diff_code = max(0, min(61, diff))
            if diff_code < 26: char_c = chr(65 + diff_code)
            elif diff_code < 52: char_c = chr(97 + (diff_code - 26))
            else: char_c = chr(48 + (diff_code - 52))
            t_chars += char_c
        pir_arr = ['0']*30
        for i in range(min(30, mc)): pir_arr[29 - i] = '1'
        p_chars = "".join(pir_arr)
        
        payload = {
            "node": "ui_custom_sim",
            "data": f"{prefix}|{base_val},{scale_val}|{t_chars}|000000000000000000000000000000|{p_chars}",
            "saat": h, "dakika": mn, "ay": month
        }
        mqtt_client.publish('ev/guvenlik/anomali/sim', json.dumps(payload))
        
    elif trigger in ["btn-sim-normal", "btn-sim-alarm", "btn-sim-night"]:
        if trigger == "btn-sim-normal":
            prefix, base_val, scale_val = 'Z_ALARM', 220, 50
            t_chars = 'K'*30
            p_chars = '0'*30
        elif trigger == "btn-sim-alarm":
            prefix, base_val, scale_val = 'Z_ALARM', 440, 100
            t_chars = 'K'*30
            p_chars = '0'*30
        elif trigger == "btn-sim-night":
            prefix, base_val, scale_val = 'PIR_ALARM', 180, 50
            t_chars = 'K'*30
            p_chars = '0'*25 + '1'*5
            
        payload = {
            "node": "ui_quick_sim",
            "data": f"{prefix}|{base_val},{scale_val}|{t_chars}|000000000000000000000000000000|{p_chars}",
            "saat": cur_hr, "dakika": cur_min, "ay": cur_mo
        }
        mqtt_client.publish('ev/guvenlik/anomali/sim', json.dumps(payload))

@app.callback(
    Output("logs-container", "children"),
    Input("interval-update", "n_intervals"),
    Input("btn-clear-logs", "n_clicks"),
    prevent_initial_call=False
)
def update_logs(n, n_clear):
    ctx = callback_context
    triggered_id = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered else ""
    
    global app_data
    if triggered_id == "btn-clear-logs":
        app_data['logs'] = []
        
    logs = app_data.get('logs', [])
    if not logs:
        return html.Div("Henüz log kaydı bulunmuyor...", style={'textAlign': 'center', 'color': '#64748b', 'padding': '2rem'})
        
    log_elements = []
    for log in reversed(logs):
        is_anomaly = log.get('is_anomaly', False)
        
        row_class = "log-row log-alarm" if is_anomaly else "log-row log-normal"
        badge_class = "log-badge badge-alarm" if is_anomaly else "log-badge badge-normal"
        badge_text = "🚨 ANOMALİ" if is_anomaly else "🟢 NORMAL"
        
        log_row = html.Div(
            className=row_class,
            children=[
                html.Div(children=[
                    html.Span(f"⏱️ {log.get('timestamp')}", className="log-time"),
                    html.Strong(f"[{log.get('node_id')}]", className="log-node-id"),
                    html.Span(log.get('data'))
                ]),
                html.Span(badge_text, className=badge_class)
            ]
        )
        log_elements.append(log_row)
        
    return log_elements

if __name__ == '__main__':
    print("Arayüz + Makine Öğrenmesi Modeli + Ağ Haritası başlatılıyor...")
    print("Refresh logları tamamen kapatıldı.")
    
    import logging
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)
    log.disabled = True
    app.logger.setLevel(logging.ERROR)
    
    app.run(debug=False, port=8050, dev_tools_silence_routes_logging=True)

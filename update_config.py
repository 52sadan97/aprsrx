import json
with open('/root/aprsrx/config.json', 'r') as f:
    cfg = json.load(f)

cfg.setdefault('web', {})['port'] = 14581
cfg.setdefault('aprs', {})['tcp_port'] = 14580

with open('/root/aprsrx/config.json', 'w') as f:
    json.dump(cfg, f, indent=4)
print("Config updated.")

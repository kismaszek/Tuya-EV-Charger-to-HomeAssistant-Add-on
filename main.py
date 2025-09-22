
import json
import time
import logging
import socket
from typing import Dict, Any

import tinytuya
import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# --- Scaling rules: convert Tuya raw values to human units ---
SCALE_MAP = {
    "energy_charge": 0.01,          # raw/100 -> kWh
    "forward_energy_total": 0.01,   # raw/100 -> kWh
    "a_current": 0.1,               # raw/10 -> A
}

def apply_scaling(code: str, value):
    if code in SCALE_MAP and isinstance(value, (int, float)):
        return round(value * SCALE_MAP[code], 3)
    return value

def mqtt_connect(opt: Dict[str, Any], device_id: str):
    host = opt.get("mqtt_host", "core-mosquitto")
    port = int(opt.get("mqtt_port", 1883))
    user = opt.get("mqtt_username") or None
    pwd = opt.get("mqtt_password") or None
    base = opt.get("mqtt_base_topic") or f"gocean_ev/{device_id}"

    client_id = f"gocean_ev_{device_id}"
    cli = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id, clean_session=True)
    if user and pwd:
        cli.username_pw_set(user, pwd)

    cli.will_set(f"{base}/availability", "offline", retain=True)
    cli.connect(host, port, keepalive=60)
    cli.loop_start()
    cli.publish(f"{base}/availability", "online", retain=True)
    return cli, base

def guess_unit_and_device_class(code: str):
    c = code.lower()
    # Temperature must come before current/amp check
    if "temp_current" in c or "temperature" in c or c.startswith("temp") or c.endswith("_temp"):
        return "°C", "temperature"
    if any(k in c for k in ["current", "amp", "i_"]):
        return "A", None
    if any(k in c for k in ["voltage", "volt", "u_"]):
        return "V", "voltage"
    if any(k in c for k in ["power", "watt", "p_"]):
        return "W", "power"
    if any(k in c for k in ["energy", "kwh", "consumption"]):
        return "kWh", "energy"
    if any(k in c for k in ["frequency", "hz"]):
        return "Hz", None
    if "percent" in c or c.endswith("_pct"):
        return "%", None
    return None, None

def publish_discovery(cli, base: str, device_id: str, model: str, items: Dict[str, Any], manufacturer: str = "Gocean"):
    dev_block = {
        "ids": [device_id],
        "name": "Gocean EV Charger",
        "mf": manufacturer,
        "mdl": model,
    }
    for code, value in items.items():
        if isinstance(value, (int, float, str, bool)):
            key = str(code).replace(" ", "_")
            uniq = f"{device_id}_{key}"
            unit, dclass = guess_unit_and_device_class(key)
            payload = {
                "name": f"Gocean {key}",
                "uniq_id": uniq,
                "stat_t": f"{base}/{key}",
                "avty_t": f"{base}/availability",
                "dev": dev_block
            }
            if unit:
                payload["unit_of_meas"] = unit
            if dclass:
                payload["dev_cla"] = dclass
            topic = f"homeassistant/sensor/{device_id}_{key}/config"
            cli.publish(topic, json.dumps(payload, ensure_ascii=False), retain=True)

def flatten_cloud_status(status: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    if not isinstance(status, dict):
        return out
    if "result" in status and isinstance(status["result"], list):
        for item in status["result"]:
            code = item.get("code")
            value = item.get("value")
            if code is None:
                continue
            out[str(code)] = value
    for k, v in status.items():
        if k != "result" and isinstance(v, (int, float, str, bool)):
            out[str(k)] = v
    # apply scaling
    for k in list(out.keys()):
        out[k] = apply_scaling(k, out[k])
    return out

def flatten_local_status(data: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    if isinstance(data, dict) and "dps" in data and isinstance(data["dps"], dict):
        for k, v in data["dps"].items():
            key = f"dps_{k}"
            out[key] = v
    # apply scaling
    for k in list(out.keys()):
        out[k] = apply_scaling(k, out[k])
    return out

def run_cloud(opt):
    region = opt.get("tuya_region", "eu")
    api = tinytuya.Cloud(
        apiRegion=region,
        apiKey=opt["tuya_client_id"],
        apiSecret=opt["tuya_client_secret"]
    )
    device_id = opt["tuya_device_id"]
    mqtt_client, base = mqtt_connect(opt, device_id)
    poll = int(opt.get("poll_interval", 30))
    model = "L6 Pro"
    log_status = bool(opt.get("log_status", False))

    while True:
        try:
            status = api.getstatus(device_id)
            flat = flatten_cloud_status(status)
            if opt.get("mqtt_discovery", True) and opt.get("force_discovery_on_start", True):
                publish_discovery(mqtt_client, base, device_id, model, flat)
            # conditional logging
            if log_status:
                logging.info("cloud.status.flat: %s", json.dumps(flat, ensure_ascii=False))
            # publish values
            for k, v in flat.items():
                payload = json.dumps(v) if isinstance(v, (dict, list)) else str(v)
                mqtt_client.publish(f"{base}/{k}", payload, retain=False)
        except Exception as e:
            logging.exception("Cloud poll error: %s", e)
        time.sleep(poll)

def _try_local_status(dev, version):
    dev.set_version(version)
    dev.set_socketPersistent(True)
    dev.set_socketNODELAY(True)
    dev.set_retry(True, retries=2, delay=0.5, backoff=1.5)
    return dev.status()

def run_local(opt):
    device_id = opt["tuya_device_id"]
    ip = opt["local_ip"]
    key = opt["local_key"]
    probe = bool(opt.get("probe_versions", True))
    versions = [3.4, 3.3] if probe else [3.3]
    mqtt_client, base = mqtt_connect(opt, device_id)
    poll = int(opt.get("poll_interval", 30))
    model = "L6 Pro"
    log_status = bool(opt.get("log_status", False))

    d = tinytuya.Device(device_id, ip, key)
    try:
        socket.create_connection((ip, 6668), timeout=3).close()
        logging.info("TCP 6668 reachable on %s", ip)
    except Exception as e:
        logging.warning("Cannot reach %s:6668 (%s). Ensure same LAN/VLAN and no firewall.", ip, e)

    current_version = None
    while True:
        try:
            last_exc = None
            status = None
            for v in versions:
                try:
                    status = _try_local_status(d, v)
                    current_version = v
                    break
                except Exception as e:
                    last_exc = e
                    continue
            if status is None:
                raise last_exc or RuntimeError("Failed to fetch status with all probed versions")

            flat = flatten_local_status(status)
            if opt.get("mqtt_discovery", True) and opt.get("force_discovery_on_start", True):
                publish_discovery(mqtt_client, base, device_id, model, flat)
            # conditional logging
            if log_status:
                logging.info("local(v=%.1f).status.flat: %s", current_version, json.dumps(flat, ensure_ascii=False))
            # publish values
            for k, v in flat.items():
                payload = json.dumps(v) if isinstance(v, (dict, list)) else str(v)
                mqtt_client.publish(f"{base}/{k}", payload, retain=False)
        except Exception as e:
            logging.error("Local poll error: %s", e)
        time.sleep(poll)

if __name__ == "__main__":
    with open("/data/options.json") as f:
        options = json.load(f)
    mode = options.get("mode", "cloud")
    if mode == "cloud" and options.get("tuya_client_id") and options.get("tuya_client_secret") and options.get("tuya_device_id"):
        run_cloud(options)
    elif mode == "local" and options.get("tuya_device_id") and options.get("local_ip") and options.get("local_key"):
        run_local(options)
    else:
        logging.error("Invalid configuration. Please fill required fields for chosen mode.")

import time
import json
import ssl
import threading
import socket
import serial
import struct
import base64
import queue
import numpy as np
from datetime import datetime
from websocket import create_connection, WebSocketConnectionClosedException, WebSocketTimeoutException
import pycodec2

# ==========================================
# 1. 에코루미너스 마스터 설정
# ==========================================
API_KEY = "OMWM-KSUI-LJGE-TKKF"
WS_URL = "wss://omtest.duckdns.org/ws-stomp/websocket"

SERIAL_PORT = '/dev/ttyAMA2'
BAUD_RATE = 9600

TOTAL_RAILS = 15
WAIT_RESPONSE_SEC = 0.2
TEN_MINUTES_SEC = 10 * 60

ser = None
serial_lock = threading.Lock()
ws_lock = threading.Lock()

data_buffer = {}
buffer_lock = threading.Lock()

FRAME_FORMAT = '>BHH6BHH12h3B'
EXPECTED_TELEMETRY_BYTES = 42

c2_dec = pycodec2.Codec2(2400)
c2_enc = pycodec2.Codec2(2400)

in_voice_call = False
active_call_rail = 0
voice_call_lock = threading.Lock()
last_call_end_time = 0
last_downlink_time = 0

downlink_audio_queue = queue.Queue(maxsize=30)
rx_downlink_pkt_cnt = 0
uplink_pcm_buffer = b""

# 💡 [스마트 동적 폴링 변수]
focused_rail = 1 
focused_has_target = False


def safe_ws_send(ws, message_str: str) -> bool:
    if ws is None:
        return False
    with ws_lock:
        try:
            ws.send(message_str)
            return True
        except Exception:
            return False


def send_plc_command(payload_bytes: bytes, expected_bytes: int = 0, wait_sec: float = WAIT_RESPONSE_SEC) -> bytes:
    global ser
    if ser is None or not ser.is_open:
        return None
    
    with serial_lock:
        try:
            if ser.in_waiting > 0:
                ser.read(ser.in_waiting)
            ser.write(payload_bytes)
            ser.flush()
            
            if expected_bytes > 0:
                start_time = time.time()
                rx_buf = bytearray()
                while (time.time() - start_time) < wait_sec:
                    avail = ser.in_waiting
                    if avail > 0:
                        rx_buf.extend(ser.read(avail))
                        if len(rx_buf) >= expected_bytes:
                            return bytes(rx_buf[:expected_bytes])
                    time.sleep(0.002)
                return None
            return None
        except Exception as e:
            print(f"⚠️ UART 통신 예외: {e}")
            return None


def downlink_audio_worker():
    global ser, in_voice_call, last_downlink_time
    print("🚀 [PTT 다운링크 워커 시작]", flush=True)

    while True:
        try:
            c2_bytes = downlink_audio_queue.get(timeout=0.1)
        except queue.Empty:
            continue

        with voice_call_lock:
            talking = in_voice_call

        if talking and ser is not None and ser.is_open and len(c2_bytes) > 0:
            last_downlink_time = time.time()
            while True:
                acquired = serial_lock.acquire(timeout=0.05)
                if acquired:
                    try:
                        if ser.in_waiting > 0:
                            ser.read(ser.in_waiting)
                        frame = bytes([0x5A, 0xA5, len(c2_bytes) & 0xFF]) + c2_bytes
                        ser.write(frame)
                        ser.flush()
                    except Exception:
                        pass
                    finally:
                        serial_lock.release()
                    break
                else:
                    time.sleep(0.01)


def ten_minute_save_loop(ws):
    while True:
        time.sleep(TEN_MINUTES_SEC)
        with buffer_lock:
            for rail_id in range(1, TOTAL_RAILS + 1):
                buf = data_buffer.get(rail_id, {"left": [], "right": [], "battery": []})
                if len(buf["left"]) > 0:
                    avg_left = round(sum(buf["left"]) / len(buf["left"]), 2)
                    avg_right = round(sum(buf["right"]) / len(buf["right"]), 2)
                    avg_battery = round(sum(buf["battery"]) / len(buf["battery"]), 1)
                    
                    db_payload = {
                        "apiKey": API_KEY,
                        "railSeq": rail_id,
                        "avgLeftWatt": avg_left,
                        "avgRightWatt": avg_right,
                        "avgBatteryPct": avg_battery,
                        "sampleCount": len(buf["left"])
                    }
                    safe_ws_send(ws, f"SEND\ndestination:/pub/rails/history/save\ncontent-type:application/json\n\n{json.dumps(db_payload)}\x00")
                    data_buffer[rail_id] = {"left": [], "right": [], "battery": []}


def receive_messages(ws):
    global in_voice_call, active_call_rail, last_call_end_time, rx_downlink_pkt_cnt, uplink_pcm_buffer
    global focused_rail, focused_has_target
    
    ws.settimeout(1.0)

    while True:
        try:
            message = ws.recv()
            if not message or not message.startswith("MESSAGE"):
                continue

            parts = message.split("\n\n", 1)
            if len(parts) <= 1:
                continue

            body_raw = parts[1].rstrip("\x00").strip()
            data = json.loads(body_raw)
            cmd_type = data.get("commandType")

            if cmd_type == "VOICE_DOWNLINK":
                with voice_call_lock:
                    is_talking = in_voice_call
                if is_talking:
                    audio_b64 = data.get("audioData", "")
                    if audio_b64:
                        try:
                            raw_pcm = base64.b64decode(audio_b64)
                            uplink_pcm_buffer += raw_pcm
                            samples = np.frombuffer(uplink_pcm_buffer, dtype=np.int16)
                            c2_frames = bytearray()
                            num_frames = len(samples) // 160
                            for i in range(num_frames):
                                c2_frames.extend(c2_enc.encode(samples[i * 160 : (i + 1) * 160]))
                            remainder = len(samples) % 160
                            uplink_pcm_buffer = samples[-remainder:].tobytes() if remainder > 0 else b""
                            if len(c2_frames) > 0:
                                for i in range(0, len(c2_frames), 48):
                                    if downlink_audio_queue.full():
                                        try: downlink_audio_queue.get_nowait()
                                        except Exception: pass
                                    downlink_audio_queue.put_nowait(bytes(c2_frames[i : i + 48]))
                        except Exception:
                            pass
                continue

            # 💡 [명령 펀칭] 융단 폭격 처리부
            if cmd_type == "VOICE_CALL_CONTROL":
                action = data.get("action")
                req_rail = data.get("railSeq", 1)
                if req_rail == 0:
                    req_rail = 1
                
                with voice_call_lock:
                    if action == "CALL_START":
                        in_voice_call = True
                        active_call_rail = req_rail
                        while not downlink_audio_queue.empty():
                            try: downlink_audio_queue.get_nowait()
                            except Exception: break
                        uplink_pcm_buffer = b""
                        for _ in range(3):
                            send_plc_command(b"\r\n\r\n\r\n$CALL_START\r\n\r\n\r\n", 0)
                            time.sleep(0.08)
                        print(f"📞 [웹 원격 제어] #{active_call_rail:02d} 통화 시작", flush=True)
                    else:
                        in_voice_call = False
                        active_call_rail = 0
                        while not downlink_audio_queue.empty():
                            try: downlink_audio_queue.get_nowait()
                            except Exception: break
                        uplink_pcm_buffer = b""
                        time.sleep(0.2)
                        
                        # 15연타 폭격
                        for _ in range(15):
                            send_plc_command(b"\r\n\r\n\r\n\r\n\r\n\r\n$CALL_END\r\n\r\n\r\n\r\n\r\n\r\n", 0)
                            time.sleep(0.05)
                        print(f"❌ [웹 원격 제어] 통화 종료 (15연타 융단 폭격 완료)", flush=True)
                continue

            if cmd_type == "STATUS_REQUEST":
                req_rail = data.get("railSeq", 1)
                
                if req_rail > 0:
                    focused_rail = req_rail
                    focused_has_target = False 
                    print(f"🎯 [레이더 집중 모드] #{focused_rail:02d}번 난간 포커싱", flush=True)
                
                resp = send_plc_command(payload_bytes=bytes([req_rail & 0x0F, 0xFF]), expected_bytes=EXPECTED_TELEMETRY_BYTES, wait_sec=0.5)

                if resp and len(resp) == EXPECTED_TELEMETRY_BYTES:
                    raw_bytes = bytes([(b - 0x20) & 0xFF for b in resp])
                    if raw_bytes[0] == req_rail:
                        (r_id, l_mw, r_mw, st_b0, st_b1, st_b2, st_b3, st_b4, battery_pct,
                         in_count, out_count, r1_x1, r1_x2, r1_x3, r1_y1, r1_y2, r1_y3,
                         r2_x1, r2_x2, r2_x3, r2_y1, r2_y2, r2_y3, r1_det, r2_det, em_code) = struct.unpack(FRAME_FORMAT, raw_bytes)

                        cf_payload = {
                            "apiKey": API_KEY,
                            "railSeq": req_rail,
                            "commandBytes": [st_b0, st_b1, st_b2, st_b3, st_b4],
                            "successRails": [req_rail],
                            "isImmediateResponse": True
                        }
                        safe_ws_send(ws, f"SEND\ndestination:/pub/rails/mode/confirm\ncontent-type:application/json\n\n{json.dumps(cf_payload)}\x00")
                else:
                    fl_payload = {
                        "apiKey": API_KEY,
                        "railSeq": req_rail,
                        "isError": True,
                        "errorMessage": "Status Polling Timeout",
                        "isImmediateResponse": True
                    }
                    safe_ws_send(ws, f"SEND\ndestination:/pub/rails/mode/fail\ncontent-type:application/json\n\n{json.dumps(fl_payload)}\x00")
                continue

            cmd_bytes = data.get('commandBytes')
            if cmd_bytes and len(cmd_bytes) == 5:
                web_rail_seq = data.get('railSeq', 0)
                payload_tx = bytes([0x7A, 0xA7] + cmd_bytes)
                resp = send_plc_command(payload_tx, expected_bytes=2, wait_sec=0.5)
                
                if resp is not None:
                    cf_payload = {
                        "apiKey": API_KEY,
                        "railSeq": web_rail_seq,
                        "commandBytes": cmd_bytes,
                        "successRails": [web_rail_seq]
                    }
                    safe_ws_send(ws, f"SEND\ndestination:/pub/rails/mode/confirm\ncontent-type:application/json\n\n{json.dumps(cf_payload)}\x00")
                else:
                    fl_payload = {
                        "apiKey": API_KEY,
                        "railSeq": web_rail_seq,
                        "isError": True,
                        "errorMessage": "Hardware Timeout"
                    }
                    safe_ws_send(ws, f"SEND\ndestination:/pub/rails/mode/fail\ncontent-type:application/json\n\n{json.dumps(fl_payload)}\x00")

        except (socket.timeout, WebSocketTimeoutException):
            continue
        except Exception:
            break


def handle_active_voice_call(ws, rail_id):
    global ser, in_voice_call, active_call_rail, last_call_end_time, last_downlink_time
    rx_stream_buf = bytearray()

    while True:
        with voice_call_lock:
            if not in_voice_call:
                break

        if ser is None or not ser.is_open:
            time.sleep(0.01)
            continue

        acquired = serial_lock.acquire(timeout=0.02)
        if acquired:
            try:
                if ser.in_waiting > 0:
                    rx_stream_buf.extend(ser.read(ser.in_waiting))
            finally:
                serial_lock.release()

        if len(rx_stream_buf) > 1024:
            idx = rx_stream_buf.rfind(b'\xA5\x5A')
            if idx != -1 and len(rx_stream_buf) - idx < 512:
                rx_stream_buf = rx_stream_buf[idx:]
            else:
                rx_stream_buf = rx_stream_buf[-512:]

        if b"$CALL_END" in rx_stream_buf:
            print(f"📞 [현장 종료 감지] #{rail_id:02d}번 통화 종료", flush=True)
            with voice_call_lock:
                in_voice_call = False
                active_call_rail = 0
            last_call_end_time = time.time()
            break

        while len(rx_stream_buf) >= 3:
            if rx_stream_buf[0] == 0xA5 and rx_stream_buf[1] == 0x5A:
                c2_len = rx_stream_buf[2]
                if len(rx_stream_buf) >= 3 + c2_len:
                    c2_bytes = rx_stream_buf[3:3 + c2_len]
                    rx_stream_buf = rx_stream_buf[3 + c2_len:]
                    pcm_out = bytearray()
                    for idx in range(0, len(c2_bytes), 6):
                        frame = bytes(c2_bytes[idx:idx+6])
                        if len(frame) == 6:
                            pcm_out.extend(c2_dec.decode(frame))
                    
                    is_broadcasting = (time.time() - last_downlink_time < 0.4)
                    if len(pcm_out) > 0 and not is_broadcasting:
                        audio_payload = {
                            "apiKey": API_KEY,
                            "railSeq": rail_id,
                            "audioData": base64.b64encode(bytes(pcm_out)).decode('ascii')
                        }
                        safe_ws_send(ws, f"SEND\ndestination:/pub/rails/voice/uplink\ncontent-type:application/json\n\n{json.dumps(audio_payload)}\x00")
                else:
                    break
            else:
                rx_stream_buf.pop(0)
        time.sleep(0.01)


def connect_and_run():
    global ser, data_buffer, in_voice_call, active_call_rail, last_call_end_time
    global focused_rail, focused_has_target
    ws = None
    
    poll_counter = 0
    bg_rail_idx = 1
    
    try:
        if ser is None or not ser.is_open:
            ser = serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=0)
            time.sleep(0.1)

        ws = create_connection(WS_URL, sslopt={"cert_reqs": ssl.CERT_NONE}, timeout=10)
        print("\n======================================================================", flush=True)
        print("🟢 에코루미너스 서버 연결 성공! (동적 대역폭 할당 모드 최적화 🚀)", flush=True)

        safe_ws_send(ws, "CONNECT\naccept-version:1.1,1.0\nheart-beat:10000,10000\n\n\x00")
        ws.recv()

        safe_ws_send(ws, f"SUBSCRIBE\nid:sub-0\ndestination:/sub/device/{API_KEY}/mode\n\n\x00")
        safe_ws_send(ws, f"SUBSCRIBE\nid:sub-1\ndestination:/sub/device/{API_KEY}/call\n\n\x00")
        safe_ws_send(ws, f"SUBSCRIBE\nid:sub-2\ndestination:/sub/device/{API_KEY}/voice/downlink\n\n\x00")

        threading.Thread(target=receive_messages, args=(ws,), daemon=True).start()
        threading.Thread(target=downlink_audio_worker, daemon=True).start()
        threading.Thread(target=ten_minute_save_loop, args=(ws,), daemon=True).start()

        print(f"⚡ [정상 통신 가동] 레이더 집중 타격 모드 시작...\n", flush=True)

        while True:
            with voice_call_lock:
                current_call_state = in_voice_call
                current_call_rail = active_call_rail

            if current_call_state:
                handle_active_voice_call(ws, current_call_rail)
                time.sleep(0.02)
                continue

            poll_counter += 1
            
            # 💡 [스마트 튜닝: 최적 대역폭 할당]
            if focused_rail > 0:
                # 타겟 감지 시: 4번 중 3번 집중 (75% 대역폭) -> 포커스 노드 약 7 FPS
                # 타겟 미감지 시: 2번 중 1번 집중 (50% 대역폭) -> 포커스 노드 5 FPS, 타 장비 2~3초 내 순회 완료
                ratio = 4 if focused_has_target else 2
                
                if poll_counter % ratio != 0:
                    rail_id = focused_rail
                else:
                    rail_id = bg_rail_idx
                    bg_rail_idx += 1
                    if bg_rail_idx > TOTAL_RAILS:
                        bg_rail_idx = 1
            else:
                rail_id = bg_rail_idx
                bg_rail_idx += 1
                if bg_rail_idx > TOTAL_RAILS:
                    bg_rail_idx = 1

            resp = send_plc_command(payload_bytes=bytes([rail_id & 0x0F, 0xFF]), expected_bytes=EXPECTED_TELEMETRY_BYTES)

            if resp and len(resp) == EXPECTED_TELEMETRY_BYTES:
                raw_bytes = bytes([(b - 0x20) & 0xFF for b in resp])
                r_id = raw_bytes[0]

                if r_id == rail_id:
                    (r_id, l_mw, r_mw, st_b0, st_b1, st_b2, st_b3, st_b4, battery_pct,
                     in_count, out_count, r1_x1, r1_x2, r1_x3, r1_y1, r1_y2, r1_y3,
                     r2_x1, r2_x2, r2_x3, r2_y1, r2_y2, r2_y3, r1_det, r2_det, em_code) = struct.unpack(FRAME_FORMAT, raw_bytes)

                    # 💡 [핵심] 현재 조회한 노드가 포커스된 노드라면 타겟 유무 플래그 업데이트
                    if rail_id == focused_rail:
                        focused_has_target = bool(r1_det) or bool(r2_det)

                    left_watt = round(l_mw / 1000.0, 1)
                    right_watt = round(r_mw / 1000.0, 1)

                    radar1_targets = [{"x": r1_x1, "y": r1_y1}, {"x": r1_x2, "y": r1_y2}, {"x": r1_x3, "y": r1_y3}]
                    radar2_targets = [{"x": r2_x1, "y": r2_y1}, {"x": r2_x2, "y": r2_y2}, {"x": r2_x3, "y": r2_y3}]

                    with buffer_lock:
                        if rail_id not in data_buffer:
                            data_buffer[rail_id] = {"left": [], "right": [], "battery": []}
                        data_buffer[rail_id]["left"].append(left_watt)
                        data_buffer[rail_id]["right"].append(right_watt)
                        data_buffer[rail_id]["battery"].append(battery_pct)

                    payload = {
                        "apiKey": API_KEY,
                        "railSeq": rail_id,
                        "commandBytes": [st_b0, st_b1, st_b2, st_b3, st_b4],
                        "leftWatt": left_watt,
                        "rightWatt": right_watt,
                        "batteryPct": battery_pct,
                        "inCount": in_count,
                        "outCount": out_count,
                        "radar1Targets": radar1_targets,
                        "radar2Targets": radar2_targets,
                        "radar1Detected": bool(r1_det),
                        "radar2Detected": bool(r2_det),
                        "emergencyCode": em_code,
                        "isCharging": True,
                        "isEmergency": (em_code != 0),
                        "isError": False
                    }
                    safe_ws_send(ws, f"SEND\ndestination:/pub/rails/realtime\ncontent-type:application/json\n\n{json.dumps(payload)}\x00")
            
            # 💡 기존 0.005초 -> 0.05초(50ms)로 변경하여 통신망을 안정화하고 낭비를 최소화함
            time.sleep(0.05)

    except Exception as e:
        print(f"🔴 연결 에러: {e}", flush=True)
    finally:
        if ws:
            ws.close()


def main():
    while True:
        try:
            connect_and_run()
        except KeyboardInterrupt:
            break
        except Exception:
            time.sleep(2)


if __name__ == "__main__":
    main()
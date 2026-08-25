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

TOTAL_RAILS = 1
TARGET_CYCLE_SEC = 2.0
WAIT_RESPONSE_SEC = 0.2
TEN_MINUTES_SEC = 10 * 60

ser = None
serial_lock = threading.Lock()
ws_lock = threading.Lock()

data_buffer = {}
buffer_lock = threading.Lock()

FRAME_FORMAT = '>BHHBBBHH3h3h3h3hBBB'

c2_dec = pycodec2.Codec2(2400)
c2_enc = pycodec2.Codec2(2400)

in_voice_call = False
active_call_rail = 0
voice_call_lock = threading.Lock()
last_call_end_time = 0
last_downlink_time = 0

# [수정] 큐 사이즈를 5로 줄여 과거 데이터가 쌓여 딜레이를 유발하는 현상 원천 차단
downlink_audio_queue = queue.Queue(maxsize=5)
rx_downlink_pkt_cnt = 0
uplink_pcm_buffer = b""

def safe_ws_send(ws, message_str: str) -> bool:
    if ws is None: return False
    with ws_lock:
        try:
            ws.send(message_str)
            return True
        except Exception:
            return False

def parse_16bit_command(full_16bit_cmd: int):
    msb = (full_16bit_cmd >> 8) & 0xFF
    lsb = full_16bit_cmd & 0xFF
    return {
        "msb": msb, "lsb": lsb,
        "power_supply_mode": (msb >> 6) & 0x03,
        "target_rail_id": msb & 0x0F,
        "is_sleep": bool((lsb >> 7) & 0x01),
        "is_prj_on": bool((lsb >> 6) & 0x01),
        "category": (lsb >> 4) & 0x03,
        "sub_mode": lsb & 0x0F
    }

def send_plc_16bit_bytes(msb_byte: int, lsb_byte: int, expected_bytes: int = 2, wait_sec: float = WAIT_RESPONSE_SEC) -> bytes:
    global ser
    if ser is None or not ser.is_open: return None
    
    with serial_lock:
        try:
            if ser.in_waiting > 0: ser.read(ser.in_waiting)
            ser.write(bytes([msb_byte & 0xFF, lsb_byte & 0xFF]))
            ser.flush()
            
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
        except Exception:
            return None

def send_plc_call_control_burst(rail_id: int, is_start: bool):
    global ser
    if ser is None or not ser.is_open: return
    target_msb = rail_id & 0x0F
    target_lsb = 0xDD if is_start else 0xEE
    cmd_bytes = bytes([target_msb, target_lsb])

    with serial_lock:
        try:
            ser.reset_input_buffer()
            ser.reset_output_buffer()
            for _ in range(5):
                ser.write(cmd_bytes)
                ser.flush()
                time.sleep(0.01)
            print(f"📞 [PLC 통화 제어 관통 전송] 난간 #{rail_id:02d} ({'시작' if is_start else '종료'})", flush=True)
        except Exception:
            pass

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
                        frame = bytes([0x5A, 0xA5, len(c2_bytes) & 0xFF]) + c2_bytes
                        ser.write(frame)
                        ser.flush()
                    except Exception as e:
                        print(f"❌ 다운링크 시리얼 에러: {e}", flush=True)
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
                    sample_cnt = len(buf["left"])
                    
                    db_payload = {
                        "apiKey": API_KEY, "railSeq": rail_id,
                        "avgLeftWatt": avg_left, "avgRightWatt": avg_right,
                        "avgBatteryPct": avg_battery, "sampleCount": sample_cnt
                    }
                    stomp_msg = f"SEND\ndestination:/pub/rails/history/save\ncontent-type:application/json\n\n{json.dumps(db_payload)}\x00"
                    safe_ws_send(ws, stomp_msg)
                    data_buffer[rail_id] = {"left": [], "right": [], "battery": []}

def receive_messages(ws):
    global in_voice_call, active_call_rail, last_call_end_time, rx_downlink_pkt_cnt
    global uplink_pcm_buffer 
    ws.settimeout(1.0)

    while True:
        try:
            message = ws.recv()
            if not message or not message.startswith("MESSAGE"): continue

            parts = message.split("\n\n", 1)
            if len(parts) <= 1: continue

            body_raw = parts[1].rstrip("\x00").strip()
            data = json.loads(body_raw)
            cmd_type = data.get("commandType")

            if cmd_type == "VOICE_CALL_CONTROL":
                action = data.get("action")
                target_rail = data.get("railSeq", 1)

                if action == "CALL_START":
                    with voice_call_lock:
                        in_voice_call = True
                        active_call_rail = target_rail
                    send_plc_call_control_burst(target_rail, is_start=True)
                    ack = {"apiKey": API_KEY, "commandType": "CALL_ACK", "action": "CALL_START", "railSeq": target_rail}
                    safe_ws_send(ws, f"SEND\ndestination:/pub/rails/call/confirm\ncontent-type:application/json\n\n{json.dumps(ack)}\x00")
                    print(f"📞 [관제소 호출 시작] #{target_rail:02d}번 난간", flush=True)

                elif action == "CALL_END":
                    last_call_end_time = time.time()
                    with voice_call_lock:
                        in_voice_call = False
                        active_call_rail = 0
                    
                    while not downlink_audio_queue.empty():
                        try: downlink_audio_queue.get_nowait()
                        except: break
                    
                    uplink_pcm_buffer = b"" 

                    send_plc_call_control_burst(target_rail, is_start=False)
                    ack = {"apiKey": API_KEY, "commandType": "CALL_ACK", "action": "CALL_END", "railSeq": target_rail}
                    safe_ws_send(ws, f"SEND\ndestination:/pub/rails/call/confirm\ncontent-type:application/json\n\n{json.dumps(ack)}\x00")
                    print(f"📞 [관제소 종료 완료] #{target_rail:02d}번 난간", flush=True)
                continue

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
                                chunk = samples[i * 160 : (i + 1) * 160]
                                encoded_bytes = c2_enc.encode(chunk)
                                c2_frames.extend(encoded_bytes)

                            remainder = len(samples) % 160
                            if remainder > 0:
                                uplink_pcm_buffer = samples[-remainder:].tobytes()
                            else:
                                uplink_pcm_buffer = b""

                            if len(c2_frames) > 0:
                                for i in range(0, len(c2_frames), 48):
                                    chunk_c2 = c2_frames[i : i + 48]
                                    if downlink_audio_queue.full():
                                        try: downlink_audio_queue.get_nowait()
                                        except: pass
                                    downlink_audio_queue.put_nowait(bytes(chunk_c2))

                                rx_downlink_pkt_cnt += 1
                                if rx_downlink_pkt_cnt % 5 == 1:
                                    print(f"🎙️ [PTT 다운링크 수신] {rx_downlink_pkt_cnt}번째 패킷 인코딩 및 큐 분할 투입 완료", flush=True)
                        except Exception as e:
                            print(f"⚠️ 오디오 인코딩 예외: {e}", flush=True)
                continue

            if cmd_type == "AUDIO_CONTROL":
                volume = data.get("volume", 50)
                play_mode_str = data.get("playMode", "RANDOM")
                track_num = data.get("trackNumber", 1)
                target_rail = data.get("railSeq", 1)

                if play_mode_str == "TRACK":
                    msb = target_rail & 0x0F
                    lsb = (3 << 4) | (track_num & 0x0F)
                    send_plc_16bit_bytes(msb, lsb, expected_bytes=2)

                vol_sub = int(volume / 10) + 2
                if vol_sub < 3: vol_sub = 3
                if vol_sub > 12: vol_sub = 12

                msb = target_rail & 0x0F
                lsb = (2 << 4) | (vol_sub & 0x0F)
                send_plc_16bit_bytes(msb, lsb, expected_bytes=2)

                ack_payload = {"apiKey": API_KEY, "commandType": "AUDIO_ACK", "railSeq": target_rail, "successRails": [target_rail]}
                safe_ws_send(ws, f"SEND\ndestination:/pub/rails/audio/confirm\ncontent-type:application/json\n\n{json.dumps(ack_payload)}\x00")
                continue

            full_cmd_16bit = data.get('railMode', data.get('mode', data.get('rail_mode', None)))
            if full_cmd_16bit is not None:
                web_rail_seq = data.get('railSeq', data.get('rail_seq', data.get('seq', 0)))
                parsed = parse_16bit_command(full_cmd_16bit)

                if web_rail_seq == 0:
                    s_rails, f_rails = [], []
                    for r_id in range(1, TOTAL_RAILS + 1):
                        target_msb = (parsed['msb'] & 0xF0) | (r_id & 0x0F)
                        resp = send_plc_16bit_bytes(msb_byte=target_msb, lsb_byte=parsed['lsb'], expected_bytes=2, wait_sec=0.25)
                        if resp and len(resp) >= 2 and resp[0] == r_id and resp[1] == parsed['lsb']:
                            s_rails.append(r_id)
                        else:
                            f_rails.append(r_id)
                        time.sleep(0.01)
                    res_payload = {"apiKey": API_KEY, "railSeq": 0, "railMode": full_cmd_16bit, "successRails": s_rails, "failRails": f_rails}
                    safe_ws_send(ws, f"SEND\ndestination:/pub/rails/mode/confirm\ncontent-type:application/json\n\n{json.dumps(res_payload)}\x00")
                else:
                    target_msb = (parsed['msb'] & 0xF0) | (web_rail_seq & 0x0F)
                    resp = send_plc_16bit_bytes(msb_byte=target_msb, lsb_byte=parsed['lsb'], expected_bytes=2, wait_sec=0.25)
                    if resp and len(resp) >= 2 and resp[0] == web_rail_seq and resp[1] == parsed['lsb']:
                        cf_payload = {"apiKey": API_KEY, "railSeq": web_rail_seq, "railMode": full_cmd_16bit, "successRails": [web_rail_seq], "failRails": []}
                        safe_ws_send(ws, f"SEND\ndestination:/pub/rails/mode/confirm\ncontent-type:application/json\n\n{json.dumps(cf_payload)}\x00")
                    else:
                        fl_payload = {"apiKey": API_KEY, "railSeq": web_rail_seq, "railMode": full_cmd_16bit, "isError": True, "errorMessage": "Timeout"}
                        safe_ws_send(ws, f"SEND\ndestination:/pub/rails/mode/fail\ncontent-type:application/json\n\n{json.dumps(fl_payload)}\x00")

        except (socket.timeout, WebSocketTimeoutException):
            continue
        except Exception as e:
            print(f"웹소켓 수신 에러: {e}")
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
            rx_stream_buf = rx_stream_buf[-512:]

        if b"$CALL_END" in rx_stream_buf:
            print(f"📞 [현장 종료 감지] #{rail_id:02d}번 통화 종료", flush=True)
            with voice_call_lock:
                in_voice_call = False
                active_call_rail = 0
            last_call_end_time = time.time()
            ack = {"apiKey": API_KEY, "commandType": "CALL_ACK", "action": "CALL_END", "railSeq": rail_id}
            safe_ws_send(ws, f"SEND\ndestination:/pub/rails/call/confirm\ncontent-type:application/json\n\n{json.dumps(ack)}\x00")
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
                            "apiKey": API_KEY, "railSeq": rail_id,
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
    ws = None
    
    try:
        if ser is None or not ser.is_open:
            ser = serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=0)
            time.sleep(0.1)

        ws = create_connection(WS_URL, sslopt={"cert_reqs": ssl.CERT_NONE}, timeout=10)
        print("\n======================================================================", flush=True)
        print("🟢 에코루미너스 백엔드 서버 웹소켓 연결 성공!", flush=True)

        safe_ws_send(ws, "CONNECT\naccept-version:1.1,1.0\nheart-beat:10000,10000\n\n\x00")
        ws.recv()

        safe_ws_send(ws, f"SUBSCRIBE\nid:sub-0\ndestination:/sub/device/{API_KEY}/mode\n\n\x00")
        safe_ws_send(ws, f"SUBSCRIBE\nid:sub-1\ndestination:/sub/device/{API_KEY}/call\n\n\x00")
        safe_ws_send(ws, f"SUBSCRIBE\nid:sub-2\ndestination:/sub/device/{API_KEY}/voice/downlink\n\n\x00")
        safe_ws_send(ws, f"SUBSCRIBE\nid:sub-3\ndestination:/sub/device/{API_KEY}/audio\n\n\x00")

        threading.Thread(target=receive_messages, args=(ws,), daemon=True).start()
        threading.Thread(target=downlink_audio_worker, daemon=True).start()
        threading.Thread(target=ten_minute_save_loop, args=(ws,), daemon=True).start()

        print(f"⚡ [정상 통신 가동] 1~{TOTAL_RAILS}번 난간 실시간 폴링 시작...\n", flush=True)

        while True:
            with voice_call_lock:
                current_call_state = in_voice_call
                current_call_rail = active_call_rail

            if current_call_state:
                handle_active_voice_call(ws, current_call_rail)
                time.sleep(0.02)
                continue

            cycle_start = time.time()

            for rail_id in range(1, TOTAL_RAILS + 1):
                with voice_call_lock:
                    if in_voice_call: break

                resp = send_plc_16bit_bytes(msb_byte=rail_id & 0x0F, lsb_byte=0xFF, expected_bytes=39)

                if resp and len(resp) == 39:
                    raw_bytes = bytes([(b - 0x20) & 0xFF for b in resp])
                    r_id = raw_bytes[0]

                    if r_id == rail_id:
                        (r_id, l_mw, r_mw, curr_msb, curr_lsb, battery_pct,
                         in_count, out_count,
                         r1_x1, r1_x2, r1_x3, r1_y1, r1_y2, r1_y3,
                         r2_x1, r2_x2, r2_x3, r2_y1, r2_y2, r2_y3,
                         r1_det, r2_det, em_code) = struct.unpack(FRAME_FORMAT, raw_bytes)

                        left_watt = round(l_mw / 1000.0, 1)
                        right_watt = round(r_mw / 1000.0, 1)
                        esp_mode_16bit = (curr_msb << 8) | curr_lsb

                        radar1_targets = [{"x": r1_x1, "y": r1_y1}, {"x": r1_x2, "y": r1_y2}, {"x": r1_x3, "y": r1_y3}]
                        radar2_targets = [{"x": r2_x1, "y": r2_y1}, {"x": r2_x2, "y": r2_y2}, {"x": r2_x3, "y": r2_y3}]

                        print("----------------------------------------------------------------------", flush=True)
                        print(f"📡 [난간 #{rail_id:02d}] 배터리:{battery_pct}% | 전력: L {left_watt}W / R {right_watt}W | 통계: In {in_count} / Out {out_count}", flush=True)

                        now_ts = time.time()
                        if (now_ts - last_call_end_time) < 1.0:
                            em_code = 0
                        elif em_code != 0:
                            print(f"   🚨 현장 비상 호출 감지 (Code: {em_code}) -> 통화 모드 진입", flush=True)
                            with voice_call_lock:
                                in_voice_call = True
                                active_call_rail = rail_id

                        with buffer_lock:
                            if rail_id not in data_buffer: data_buffer[rail_id] = {"left": [], "right": [], "battery": []}
                            data_buffer[rail_id]["left"].append(left_watt)
                            data_buffer[rail_id]["right"].append(right_watt)
                            data_buffer[rail_id]["battery"].append(battery_pct)

                        payload = {
                            "apiKey": API_KEY, "railSeq": rail_id,
                            "leftWatt": left_watt, "rightWatt": right_watt,
                            "batteryPct": battery_pct, "railMode": esp_mode_16bit,
                            "inCount": in_count, "outCount": out_count,
                            "radar1Targets": radar1_targets, "radar2Targets": radar2_targets,
                            "radar1Detected": bool(r1_det), "radar2Detected": bool(r2_det),
                            "emergencyCode": em_code,
                            "isCharging": True, "isEmergency": (em_code != 0), "isError": False
                        }
                        safe_ws_send(ws, f"SEND\ndestination:/pub/rails/realtime\ncontent-type:application/json\n\n{json.dumps(payload)}\x00")
                time.sleep(0.01)

            elapsed = time.time() - cycle_start
            time.sleep(max(0.01, TARGET_CYCLE_SEC - elapsed) if TOTAL_RAILS > 1 else 0.5)

    except Exception as e:
        print(f"🔴 연결 에러: {e}", flush=True)
    finally:
        if ws: ws.close()

def main():
    while True:
        try:
            connect_and_run()
        except KeyboardInterrupt:
            print("\n프로그램을 종료합니다.")
            break
        except Exception as e:
            print(f"⚠️ 재시도 중... ({e})", flush=True)
            time.sleep(2)

if __name__ == "__main__":
    main()
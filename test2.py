import time
import json
import ssl
import threading
import socket
import serial
from datetime import datetime
from websocket import create_connection, WebSocketConnectionClosedException, WebSocketTimeoutException

# ==========================================
# 1. 에코루미너스 마스터 설정
# ==========================================
API_KEY = "OMWM-KSUI-LJGE-TKKF"
WS_URL = "wss://omtest.duckdns.org/ws-stomp/websocket"

# 시리얼 포트 설정
SERIAL_PORT = '/dev/ttyAMA2'
BAUD_RATE = 9600

TOTAL_RAILS = 1           
TARGET_CYCLE_SEC = 5.0/15   
WAIT_RESPONSE_SEC = 0.35   
TEN_MINUTES_SEC = 10 * 60  

ser = None
serial_lock = threading.Lock()

data_buffer = {}
buffer_lock = threading.Lock()

# ==========================================
# 2. 시리얼 프레임 송수신
# ==========================================
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
    
    acquired = serial_lock.acquire(timeout=0.6)
    if not acquired: return None

    try:
        ser.reset_input_buffer()
        ser.reset_output_buffer()
        
        frame = bytes([msb_byte & 0xFF, lsb_byte & 0xFF])
        ser.write(frame)
        ser.flush()
        
        start_time = time.time()
        rx_buf = bytearray()

        while (time.time() - start_time) < wait_sec:
            if ser.in_waiting > 0:
                chunk = ser.read(ser.in_waiting)
                rx_buf.extend(chunk)
                if len(rx_buf) >= expected_bytes:
                    return bytes(rx_buf[:expected_bytes])
            time.sleep(0.002)
        return None
    except Exception:
        return None
    finally:
        serial_lock.release()

def send_plc_audio_bytes(rail_id: int, volume: int, play_mode: int, track_num: int, wait_sec: float = WAIT_RESPONSE_SEC) -> bool:
    global ser
    if ser is None or not ser.is_open: return False
    
    acquired = serial_lock.acquire(timeout=0.6)
    if not acquired: return False

    try:
        ser.reset_input_buffer()
        ser.reset_output_buffer()
        
        track_msb = (track_num >> 8) & 0xFF
        track_lsb = track_num & 0xFF
        frame = bytes([0xEE, rail_id & 0xFF, volume & 0xFF, play_mode & 0xFF, track_msb, track_lsb])
        
        ser.write(frame)
        ser.flush()
        
        start_time = time.time()
        rx_buf = bytearray()

        while (time.time() - start_time) < wait_sec:
            if ser.in_waiting > 0:
                rx_buf.extend(ser.read(ser.in_waiting))
                if len(rx_buf) >= 2 and rx_buf[0] == 0xEE and rx_buf[1] == (rail_id & 0xFF):
                    return True
            time.sleep(0.002)
        return False
    except Exception:
        return False
    finally:
        serial_lock.release()

# ==========================================
# 3. 10분 평균 DB 저장 Loop
# ==========================================
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
                    try:
                        stomp_msg = f"SEND\ndestination:/pub/rails/history/save\ncontent-type:application/json\n\n{json.dumps(db_payload)}\x00"
                        ws.send(stomp_msg)
                    except Exception: pass
                    data_buffer[rail_id] = {"left": [], "right": [], "battery": []}

# ==========================================
# 4. 백엔드 제어 수신 스레드
# ==========================================
def receive_messages(ws):
    ws.settimeout(1.0)
    
    while True:
        try:
            message = ws.recv()
            if not message or message in ("\n", "\r\n"): continue
            if not message.startswith("MESSAGE"): continue

            parts = message.split("\n\n", 1)
            if len(parts) <= 1: continue

            body_raw = parts[1].rstrip("\x00").strip()
            try:
                data = json.loads(body_raw)
                
                # 1) 오디오 제어 처리
                if data.get("commandType") == "AUDIO_CONTROL":
                    volume = data.get("volume", 50)
                    play_mode_str = data.get("playMode", "RANDOM")
                    track_num = data.get("trackNumber", 1)
                    target_rail = data.get("railSeq", 0)
                    
                    mode_map = {"RANDOM": 0, "SHUFFLE": 1, "TRACK": 2}
                    mode_int = mode_map.get(play_mode_str, 0)

                    print(f"\n🎧 [오디오 제어] 대상: #{target_rail} | 볼륨: {volume}% | 트랙: {track_num}", flush=True)

                    if target_rail == 0:
                        success_rails, fail_rails = [], []
                        for r_id in range(1, TOTAL_RAILS + 1):
                            if send_plc_audio_bytes(r_id, volume, mode_int, track_num):
                                success_rails.append(r_id)
                            else:
                                fail_rails.append(r_id)
                            time.sleep(0.05)
                        ack_payload = {"apiKey": API_KEY, "commandType": "AUDIO_ACK", "railSeq": 0, "successRails": success_rails, "failRails": fail_rails}
                    else:
                        if send_plc_audio_bytes(target_rail, volume, mode_int, track_num):
                            ack_payload = {"apiKey": API_KEY, "commandType": "AUDIO_ACK", "railSeq": target_rail, "successRails": [target_rail], "failRails": []}
                        else:
                            ack_payload = {"apiKey": API_KEY, "commandType": "AUDIO_ACK", "railSeq": target_rail, "successRails": [], "failRails": [target_rail]}
                    
                    ws.send(f"SEND\ndestination:/pub/rails/audio/confirm\ncontent-type:application/json\n\n{json.dumps(ack_payload)}\x00")
                    continue

                # 2) 일반 제어 처리
                full_cmd_16bit = data.get('railMode', data.get('mode', data.get('rail_mode', None)))
                if full_cmd_16bit is None: continue 

                web_rail_seq = data.get('railSeq', data.get('rail_seq', data.get('seq', 0)))
                parsed = parse_16bit_command(full_cmd_16bit)

                print(f"\n📩 [모드 제어] 대상: #{web_rail_seq} | Mode: 0x{full_cmd_16bit:04X}", flush=True)

                if web_rail_seq == 0:
                    success_rails, fail_rails = [], []
                    for r_id in range(1, TOTAL_RAILS + 1):
                        target_msb = (parsed['msb'] & 0xF0) | (r_id & 0x0F)
                        resp = send_plc_16bit_bytes(msb_byte=target_msb, lsb_byte=parsed['lsb'], expected_bytes=2, wait_sec=WAIT_RESPONSE_SEC)
                        
                        if resp and len(resp) >= 2 and resp[0] == r_id and resp[1] == parsed['lsb']:
                            success_rails.append(r_id)
                        else:
                            fail_rails.append(r_id)
                        time.sleep(0.05)
                    result_payload = {"apiKey": API_KEY, "railSeq": 0, "railMode": full_cmd_16bit, "successRails": success_rails, "failRails": fail_rails}
                    ws.send(f"SEND\ndestination:/pub/rails/mode/confirm\ncontent-type:application/json\n\n{json.dumps(result_payload)}\x00")

                else:
                    target_msb = (parsed['msb'] & 0xF0) | (web_rail_seq & 0x0F)
                    resp = send_plc_16bit_bytes(msb_byte=target_msb, lsb_byte=parsed['lsb'], expected_bytes=2, wait_sec=WAIT_RESPONSE_SEC)

                    if resp and len(resp) >= 2 and resp[0] == web_rail_seq and resp[1] == parsed['lsb']:
                        confirm_payload = {"apiKey": API_KEY, "railSeq": web_rail_seq, "railMode": full_cmd_16bit, "successRails": [web_rail_seq], "failRails": []}
                        ws.send(f"SEND\ndestination:/pub/rails/mode/confirm\ncontent-type:application/json\n\n{json.dumps(confirm_payload)}\x00")
                    else:
                        fail_payload = {"apiKey": API_KEY, "railSeq": web_rail_seq, "railMode": full_cmd_16bit, "isError": True, "errorMessage": "Timeout"}
                        ws.send(f"SEND\ndestination:/pub/rails/mode/fail\ncontent-type:application/json\n\n{json.dumps(fail_payload)}\x00")

            except json.JSONDecodeError: pass
        except (socket.timeout, WebSocketTimeoutException, ssl.SSLError): continue
        except (WebSocketConnectionClosedException, ConnectionResetError, BrokenPipeError): break
        except Exception: break

# ==========================================
# 5. 백엔드 연결 및 관제 스캔 루프
# ==========================================
def connect_and_run():
    global ser, data_buffer
    ws = None
    
    try:
        try:
            if ser is None or not ser.is_open:
                ser = serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=0)
                time.sleep(0.1)
        except Exception as se:
            print(f"⚠️ 시리얼 포트 에러: {se}", flush=True)

        ws = create_connection(WS_URL, sslopt={"cert_reqs": ssl.CERT_NONE}, timeout=10)
        print("\n==========================================", flush=True)
        print("🟢 에코루미너스 백엔드 서버 웹소켓 연결 성공!", flush=True)

        connect_frame = "CONNECT\naccept-version:1.1,1.0\nheart-beat:10000,10000\n\n\x00"
        ws.send(connect_frame)
        
        conn_ack = ws.recv()
        if "CONNECTED" in conn_ack:
            print("✅ [STOMP 핸드셰이크 완료]", flush=True)

        ws.send(f"SUBSCRIBE\nid:sub-0\ndestination:/sub/device/{API_KEY}/mode\n\n\x00")
        ws.send(f"SUBSCRIBE\nid:sub-1\ndestination:/sub/device/{API_KEY}/audio\n\n\x00")

        recv_thread = threading.Thread(target=receive_messages, args=(ws,), daemon=True)
        recv_thread.start()

        save_thread = threading.Thread(target=ten_minute_save_loop, args=(ws,), daemon=True)
        save_thread.start()

        print(f"⚡ [{TARGET_CYCLE_SEC}초 관제 시작] 1~{TOTAL_RAILS}번 난간 실시간 폴링 중...\n", flush=True)

        while True:
            cycle_start_time = time.time()
            success_count = 0
            rail_reports = []

            for rail_id in range(1, TOTAL_RAILS + 1):
                poll_msb = rail_id & 0x0F
                # ★ [변경점] ESP32가 16바이트로 응답하도록 수정했으므로, expected_bytes를 16으로 증가
                resp = send_plc_16bit_bytes(msb_byte=poll_msb, lsb_byte=0xFF, expected_bytes=16, wait_sec=WAIT_RESPONSE_SEC)

                if resp and len(resp) == 16 and resp[0] == rail_id:
                    l_mw, r_mw = (resp[1] << 8) | resp[2], (resp[3] << 8) | resp[4]
                    left_watt, right_watt = round(l_mw / 1000.0, 1), round(r_mw / 1000.0, 1)
                    esp_mode_16bit = (resp[5] << 8) | resp[6]
                    battery_pct = int(resp[7])
                    
                    # ★ [추가] 16바이트 응답에서 레이더 1, 2 좌표 파싱 (Big Endian)
                    r1_x = int.from_bytes(resp[8:10], byteorder='big', signed=True)
                    r1_y = int.from_bytes(resp[10:12], byteorder='big', signed=True)
                    r2_x = int.from_bytes(resp[12:14], byteorder='big', signed=True)
                    r2_y = int.from_bytes(resp[14:16], byteorder='big', signed=True)
                    
                    is_error = False
                    success_count += 1

                    rail_reports.append(f"  [난간 #{rail_id:02d}] 배터리: {battery_pct}% | R1({r1_x},{r1_y}) R2({r2_x},{r2_y})")

                    with buffer_lock:
                        if rail_id not in data_buffer: data_buffer[rail_id] = {"left": [], "right": [], "battery": []}
                        data_buffer[rail_id]["left"].append(left_watt)
                        data_buffer[rail_id]["right"].append(right_watt)
                        data_buffer[rail_id]["battery"].append(battery_pct)
                else:
                    is_error = True
                    left_watt, right_watt, battery_pct, esp_mode_16bit = 0.0, 0.0, 0, 0
                    r1_x, r1_y, r2_x, r2_y = 0, 0, 0, 0
                    rail_reports.append(f"  [난간 #{rail_id:02d}] 🔴 통신 무응답 (Timeout)")

                payload = {
                    "apiKey": API_KEY, "railSeq": rail_id,
                    "leftWatt": left_watt, "rightWatt": right_watt,
                    "batteryPct": battery_pct, "railMode": esp_mode_16bit,
                    "isCharging": not is_error, "isEmergency": (battery_pct < 10 and not is_error), "isError": is_error,
                    # ★ [추가] 웹소켓 페이로드에 레이더 데이터 반영
                    "radar1X": r1_x, "radar1Y": r1_y,
                    "radar2X": r2_x, "radar2Y": r2_y
                }
                
                try: ws.send(f"SEND\ndestination:/pub/rails/realtime\ncontent-type:application/json\n\n{json.dumps(payload)}\x00")
                except Exception: pass
                
                print(rail_reports[-1], flush=True)
                time.sleep(0.01)

            scan_elapsed_time = time.time() - cycle_start_time
            print(f"📡 [관제 스캔 완료] {success_count}/{TOTAL_RAILS}개 수신 성공", flush=True)

            remaining_sleep = TARGET_CYCLE_SEC - scan_elapsed_time
            time.sleep(remaining_sleep if remaining_sleep > 0 else 0.01)

    except Exception as e:
        print(f"🔴 서버 연결 에러: {e}", flush=True)
    finally:
        if ws:
            try: ws.close()
            except Exception: pass

def main():
    print("🚀 에코루미너스 마스터 구동 (듀얼 레이더 관제 모드)...", flush=True)
    while True:
        try: connect_and_run()
        except Exception as err: print("메인 루프 재시작 중... 에러:", err, flush=True)
        time.sleep(2)

if __name__ == "__main__":
    try: main()
    except KeyboardInterrupt:
        if ser and ser.is_open: ser.close()
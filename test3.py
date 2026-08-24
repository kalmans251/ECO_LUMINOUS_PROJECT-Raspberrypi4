import time
import json
import ssl
import threading
import socket
import serial
import struct
from datetime import datetime
from websocket import create_connection, WebSocketConnectionClosedException, WebSocketTimeoutException

# ==========================================
# 1. 에코루미너스 마스터 설정
# ==========================================
API_KEY = "OMWM-KSUI-LJGE-TKKF"
WS_URL = "wss://omtest.duckdns.org/ws-stomp/websocket"

SERIAL_PORT = '/dev/ttyAMA2'
BAUD_RATE = 9600

TOTAL_RAILS = 15           # 1번 ~ 15번 난간
TARGET_CYCLE_SEC = 5.0     # 관제 스캔 주기 (5.0초)
WAIT_RESPONSE_SEC = 0.35   # 39바이트 원샷 수신 대기 (350ms)
TEN_MINUTES_SEC = 10 * 60  

ser = None
serial_lock = threading.Lock()

data_buffer = {}
buffer_lock = threading.Lock()

# 39바이트 바이너리 프레임 포맷 (Big-Endian)
# ID(1), L_mW(2), R_mW(2), MSB(1), LSB(1), Bat(1), In(2), Out(2), 
# R1_X[3](6), R1_Y[3](6), R2_X[3](6), R2_Y[3](6), R1_Det(1), R2_Det(1), Emergency(1)
FRAME_FORMAT = '>BHHBBBHH3h3h3h3hBBB'

emergency_map = {0: "정상", 1: "🚨 살려주세요", 2: "🚨 도와주세요", 3: "🚨 구해주세요"}

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
    if ser is None or not ser.is_open:
        return None
    
    acquired = serial_lock.acquire(timeout=0.6)
    if not acquired:
        return None

    try:
        if ser.in_waiting > 0:
            ser.read(ser.in_waiting)
        
        frame = bytes([msb_byte & 0xFF, lsb_byte & 0xFF])
        ser.write(frame)
        ser.flush()
        
        start_time = time.time()
        rx_buf = bytearray()

        while (time.time() - start_time) < wait_sec:
            avail = ser.in_waiting
            if avail > 0:
                rx_buf.extend(ser.read(avail))
                if len(rx_buf) >= expected_bytes:
                    return bytes(rx_buf[:expected_bytes])
            time.sleep(0.003)

        rail_target = msb_byte & 0x0F
        if len(rx_buf) > 0 and len(rx_buf) != expected_bytes:
            print(f"🔍 [난간 #{rail_target:02d}] 수신 바이트 불일치: {len(rx_buf)}/{expected_bytes} B | HEX: {rx_buf.hex()}", flush=True)

        return None
    except Exception as e:
        print(f"⚠️ 시리얼 에러: {e}", flush=True)
        return None
    finally:
        serial_lock.release()

def send_plc_audio_bytes(rail_id: int, volume: int, play_mode: int, track_num: int, wait_sec: float = WAIT_RESPONSE_SEC) -> bool:
    global ser
    if ser is None or not ser.is_open:
        return False
    
    acquired = serial_lock.acquire(timeout=0.6)
    if not acquired:
        return False

    try:
        if ser.in_waiting > 0:
            ser.read(ser.in_waiting)
        
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
            time.sleep(0.003)
        return False
    except Exception:
        return False
    finally:
        serial_lock.release()

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
                    except Exception:
                        pass
                    data_buffer[rail_id] = {"left": [], "right": [], "battery": []}

def receive_messages(ws):
    ws.settimeout(1.0)
    while True:
        try:
            message = ws.recv()
            if not message or message in ("\n", "\r\n") or not message.startswith("MESSAGE"):
                continue

            parts = message.split("\n\n", 1)
            if len(parts) <= 1: continue

            body_raw = parts[1].rstrip("\x00").strip()
            try:
                data = json.loads(body_raw)
                
                # 오디오 제어
                if data.get("commandType") == "AUDIO_CONTROL":
                    volume = data.get("volume", 50)
                    play_mode_str = data.get("playMode", "RANDOM")
                    track_num = data.get("trackNumber", 1)
                    target_rail = data.get("railSeq", 0)
                    mode_int = {"RANDOM": 0, "SHUFFLE": 1, "TRACK": 2}.get(play_mode_str, 0)

                    if target_rail == 0:
                        s_rails, f_rails = [], []
                        for r_id in range(1, TOTAL_RAILS + 1):
                            if send_plc_audio_bytes(r_id, volume, mode_int, track_num): s_rails.append(r_id)
                            else: f_rails.append(r_id)
                            time.sleep(0.03)
                        ack_payload = {"apiKey": API_KEY, "commandType": "AUDIO_ACK", "railSeq": 0, "successRails": s_rails, "failRails": f_rails}
                    else:
                        is_ok = send_plc_audio_bytes(target_rail, volume, mode_int, track_num)
                        ack_payload = {"apiKey": API_KEY, "commandType": "AUDIO_ACK", "railSeq": target_rail, "successRails": [target_rail] if is_ok else [], "failRails": [] if is_ok else [target_rail]}
                    
                    ws.send(f"SEND\ndestination:/pub/rails/audio/confirm\ncontent-type:application/json\n\n{json.dumps(ack_payload)}\x00")
                    continue

                # 16비트 모드 제어
                full_cmd_16bit = data.get('railMode', data.get('mode', data.get('rail_mode', None)))
                if full_cmd_16bit is None: continue

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
                        time.sleep(0.03)

                    res_payload = {"apiKey": API_KEY, "railSeq": 0, "railMode": full_cmd_16bit, "successRails": s_rails, "failRails": f_rails}
                    ws.send(f"SEND\ndestination:/pub/rails/mode/confirm\ncontent-type:application/json\n\n{json.dumps(res_payload)}\x00")
                else:
                    target_msb = (parsed['msb'] & 0xF0) | (web_rail_seq & 0x0F)
                    resp = send_plc_16bit_bytes(msb_byte=target_msb, lsb_byte=parsed['lsb'], expected_bytes=2, wait_sec=0.25)
                    if resp and len(resp) >= 2 and resp[0] == web_rail_seq and resp[1] == parsed['lsb']:
                        cf_payload = {"apiKey": API_KEY, "railSeq": web_rail_seq, "railMode": full_cmd_16bit, "successRails": [web_rail_seq], "failRails": []}
                        ws.send(f"SEND\ndestination:/pub/rails/mode/confirm\ncontent-type:application/json\n\n{json.dumps(cf_payload)}\x00")
                    else:
                        fl_payload = {"apiKey": API_KEY, "railSeq": web_rail_seq, "railMode": full_cmd_16bit, "isError": True, "errorMessage": "Timeout"}
                        ws.send(f"SEND\ndestination:/pub/rails/mode/fail\ncontent-type:application/json\n\n{json.dumps(fl_payload)}\x00")

            except json.JSONDecodeError:
                pass

        except (socket.timeout, WebSocketTimeoutException, ssl.SSLError):
            continue
        except (WebSocketConnectionClosedException, ConnectionResetError, BrokenPipeError):
            break
        except Exception:
            break

def connect_and_run():
    global ser, data_buffer
    ws = None
    
    try:
        try:
            if ser is None or not ser.is_open:
                ser = serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=0)
                time.sleep(0.1)
        except Exception as se:
            print(f"⚠️ 시리얼 포트 미연결: {se}", flush=True)

        ws = create_connection(WS_URL, sslopt={"cert_reqs": ssl.CERT_NONE}, timeout=10)
        print("\n======================================================================", flush=True)
        print("🟢 에코루미너스 백엔드 서버 웹소켓 연결 성공!", flush=True)

        ws.send("CONNECT\naccept-version:1.1,1.0\nheart-beat:10000,10000\n\n\x00")
        conn_ack = ws.recv()
        if "CONNECTED" in conn_ack:
            print("✅ [STOMP 핸드셰이크 완료] 서버 인증 성공!", flush=True)

        ws.send(f"SUBSCRIBE\nid:sub-0\ndestination:/sub/device/{API_KEY}/mode\n\n\x00")
        ws.send(f"SUBSCRIBE\nid:sub-1\ndestination:/sub/device/{API_KEY}/audio\n\n\x00")

        threading.Thread(target=receive_messages, args=(ws,), daemon=True).start()
        threading.Thread(target=ten_minute_save_loop, args=(ws,), daemon=True).start()

        print(f"⚡ [{TARGET_CYCLE_SEC}초 관제 시작] 1~{TOTAL_RAILS}번 난간 39B 실시간 폴링 중...\n", flush=True)

        while True:
            cycle_start_time = time.time()
            success_count = 0

            for rail_id in range(1, TOTAL_RAILS + 1):
                poll_msb = rail_id & 0x0F
                
                resp = send_plc_16bit_bytes(msb_byte=poll_msb, lsb_byte=0xFF, expected_bytes=39, wait_sec=WAIT_RESPONSE_SEC)

                if resp and len(resp) == 39:
                    # -0x20 오프셋 복원 디코딩
                    raw_bytes = bytes([(b - 0x20) & 0xFF for b in resp])
                    r_id = raw_bytes[0]

                    if r_id == rail_id:
                        (
                            r_id, l_mw, r_mw,
                            curr_msb, curr_lsb, battery_pct,
                            in_count, out_count,
                            r1_x1, r1_x2, r1_x3,
                            r1_y1, r1_y2, r1_y3,
                            r2_x1, r2_x2, r2_x3,
                            r2_y1, r2_y2, r2_y3,
                            r1_det, r2_det, em_code
                        ) = struct.unpack(FRAME_FORMAT, raw_bytes)

                        left_watt = round(l_mw / 1000.0, 1)
                        right_watt = round(r_mw / 1000.0, 1)
                        esp_mode_16bit = (curr_msb << 8) | curr_lsb
                        is_error = False
                        success_count += 1

                        radar1_targets = [
                            {"x": r1_x1, "y": r1_y1},
                            {"x": r1_x2, "y": r1_y2},
                            {"x": r1_x3, "y": r1_y3}
                        ]
                        radar2_targets = [
                            {"x": r2_x1, "y": r2_y1},
                            {"x": r2_x2, "y": r2_y2},
                            {"x": r2_x3, "y": r2_y3}
                        ]

                        # ------------------- [실시간 콘솔 출력] -------------------
                        print(f"----------------------------------------------------------------------", flush=True)
                        print(f"📡 [난간 #{rail_id:02d}] 배터리: {battery_pct}% | 전력: L {left_watt}W / R {right_watt}W | 통계: In {in_count}명 / Out {out_count}명", flush=True)
                        
                        r1_status = "🔴감지" if r1_det else "⚪미감지"
                        print(f"  🎯 R1(진입) [{r1_status}]: "
                              f"T1(X:{r1_x1:5d}, Y:{r1_y1:5d}) | "
                              f"T2(X:{r1_x2:5d}, Y:{r1_y2:5d}) | "
                              f"T3(X:{r1_x3:5d}, Y:{r1_y3:5d})", flush=True)

                        r2_status = "🔴감지" if r2_det else "⚪미감지"
                        print(f"  🎯 R2(퇴장) [{r2_status}]: "
                              f"T1(X:{r2_x1:5d}, Y:{r2_y1:5d}) | "
                              f"T2(X:{r2_x2:5d}, Y:{r2_y2:5d}) | "
                              f"T3(X:{r2_x3:5d}, Y:{r2_y3:5d})", flush=True)

                        if em_code != 0:
                            print(f"  🚨 비상 코드 감지: {emergency_map.get(em_code, '알수없음')} (Code: {em_code})", flush=True)
                        # ----------------------------------------------------------------------

                        is_emergency = (em_code != 0) or (battery_pct < 10)

                        with buffer_lock:
                            if rail_id not in data_buffer:
                                data_buffer[rail_id] = {"left": [], "right": [], "battery": []}
                            data_buffer[rail_id]["left"].append(left_watt)
                            data_buffer[rail_id]["right"].append(right_watt)
                            data_buffer[rail_id]["battery"].append(battery_pct)

                        payload = {
                            "apiKey": API_KEY, "railSeq": rail_id,
                            "leftWatt": left_watt, "rightWatt": right_watt,
                            "batteryPct": battery_pct, "railMode": esp_mode_16bit,
                            "inCount": in_count, "outCount": out_count,
                            "radar1Targets": radar1_targets,
                            "radar2Targets": radar2_targets,
                            "radar1Detected": bool(r1_det),
                            "radar2Detected": bool(r2_det),
                            "emergencyCode": em_code,
                            "isCharging": not is_error, "isEmergency": is_emergency, "isError": is_error
                        }

                    else:
                        is_error = True
                        payload = {
                            "apiKey": API_KEY, "railSeq": rail_id,
                            "leftWatt": 0.0, "rightWatt": 0.0,
                            "batteryPct": 0, "railMode": 0,
                            "inCount": 0, "outCount": 0,
                            "radar1Targets": [], "radar2Targets": [],
                            "radar1Detected": False, "radar2Detected": False,
                            "emergencyCode": 0,
                            "isCharging": False, "isEmergency": False, "isError": True
                        }
                else:
                    is_error = True
                    payload = {
                        "apiKey": API_KEY, "railSeq": rail_id,
                        "leftWatt": 0.0, "rightWatt": 0.0,
                        "batteryPct": 0, "railMode": 0,
                        "inCount": 0, "outCount": 0,
                        "radar1Targets": [], "radar2Targets": [],
                        "radar1Detected": False, "radar2Detected": False,
                        "emergencyCode": 0,
                        "isCharging": False, "isEmergency": False, "isError": True
                    }

                try:
                    ws.send(f"SEND\ndestination:/pub/rails/realtime\ncontent-type:application/json\n\n{json.dumps(payload)}\x00")
                except Exception:
                    pass
                time.sleep(0.015)

            scan_elapsed_time = time.time() - cycle_start_time
            print(f"📡 [관제 스캔 완료] {success_count}/{TOTAL_RAILS}개 수신 성공 (소요: {scan_elapsed_time:.2f}초)", flush=True)

            remaining_sleep = TARGET_CYCLE_SEC - scan_elapsed_time
            time.sleep(remaining_sleep if remaining_sleep > 0 else 0.01)

    except Exception as e:
        print(f"🔴 서버 연결 에러: {e}", flush=True)
    finally:
        if ws:
            try:
                ws.close()
            except Exception:
                pass

def main():
    print("🚀 에코루미너스 마스터 구동 (39B Non-Zero 무손실 원샷 모드)...", flush=True)
    while True:
        try:
            connect_and_run()
        except Exception as err:
            print("메인 루프 재시작 중... 에러:", err, flush=True)
            time.sleep(2)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        if ser and ser.is_open:
            ser.close()
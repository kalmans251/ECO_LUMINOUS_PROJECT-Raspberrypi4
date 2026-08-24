import time
import json
import ssl
import threading
import socket
import serial
from websocket import create_connection, WebSocketConnectionClosedException

# ==========================================
# 1. 에코루미너스 마스터 설정
# ==========================================
API_KEY = "OMWM-KSUI-LJGE-TKKF"
WS_URL = "wss://omtest.duckdns.org/ws-stomp/websocket"

SERIAL_PORT = '/dev/ttyAMA2'
BAUD_RATE = 9600

TOTAL_RAILS = 15           # 1번 ~ 15번 난간
TARGET_CYCLE_SEC = 5.0     # 관제 스캔 주기 (5.0초)
WAIT_RESPONSE_SEC = 0.20   # 8바이트 수신 대기 타임아웃 (200ms)
TEN_MINUTES_SEC = 10 * 60  # DB 저장용 10분 평균 집계 주기

ser = None
serial_lock = threading.Lock()

data_buffer = {}
buffer_lock = threading.Lock()


# ==========================================
# 2. 16비트 비트 언패킹 및 시리얼 송수신
# ==========================================
def parse_16bit_command(full_16bit_cmd: int):
    """
    16비트 정수 명령어를 비트 필드로 분해
    """
    msb = (full_16bit_cmd >> 8) & 0xFF
    lsb = full_16bit_cmd & 0xFF

    power_supply_mode = (msb >> 6) & 0x03   # Bit 15~14 (0:자동, 1:배터리, 2:상전)
    target_rail_id = msb & 0x0F             # Bit 11~8  (0:전체, 1~15)

    is_sleep = bool((lsb >> 7) & 0x01)       # Bit 7 (0:일반, 1:슬립)
    is_prj_on = bool((lsb >> 6) & 0x01)      # Bit 6 (0:OFF, 1:ON)
    category = (lsb >> 4) & 0x03             # Bit 5~4 (0:기본, 1:날씨, 2:음악)
    sub_mode = lsb & 0x0F                    # Bit 3~0 (서브패턴 0~15)

    return {
        "msb": msb,
        "lsb": lsb,
        "power_supply_mode": power_supply_mode,
        "target_rail_id": target_rail_id,
        "is_sleep": is_sleep,
        "is_prj_on": is_prj_on,
        "category": category,
        "sub_mode": sub_mode
    }


def send_plc_16bit_bytes(msb_byte: int, lsb_byte: int, expected_bytes: int = 2, wait_sec: float = WAIT_RESPONSE_SEC) -> bytes:
    """
    MSB 1바이트 + LSB 1바이트 전송 후 expected_bytes 만큼 수신 대기
    """
    global ser
    
    if ser is None or not ser.is_open:
        return None

    acquired = serial_lock.acquire(timeout=0.1)
    if not acquired:
        return None

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
                rx_buf.extend(ser.read(ser.in_waiting))
                if len(rx_buf) >= expected_bytes:
                    return bytes(rx_buf)
            time.sleep(0.003)

        return None

    except Exception:
        return None
    finally:
        serial_lock.release()


# ==========================================
# 3. 10분 평균 집계 및 DB 히스토리 저장 스레드
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
                        "apiKey": API_KEY,
                        "railSeq": rail_id,
                        "avgLeftWatt": avg_left,
                        "avgRightWatt": avg_right,
                        "avgBatteryPct": avg_battery,
                        "sampleCount": sample_cnt
                    }
                    
                    try:
                        ws.send(f"SEND\ndestination:/pub/rails/history/save\ncontent-type:application/json\n\n{json.dumps(db_payload)}\x00")
                        print(f"💾 [DB 10분 평균 저장 완료] 난간 #{rail_id} (샘플 {sample_cnt}개)")
                    except Exception:
                        pass
                    
                    data_buffer[rail_id] = {"left": [], "right": [], "battery": []}


# ==========================================
# 4. 백엔드 제어 수신 스레드
# ==========================================
def receive_messages(ws):
    ws.settimeout(1.0)
    
    while True:
        try:
            message = ws.recv()
            if not message or not message.startswith("MESSAGE"):
                continue

            parts = message.split("\n\n", 1)
            if len(parts) <= 1:
                continue

            body_raw = parts[1].rstrip("\x00")
            try:
                data = json.loads(body_raw)
                full_cmd_16bit = data.get('railMode', 0)
                web_rail_seq = data.get('railSeq', 0)
                
                parsed = parse_16bit_command(full_cmd_16bit)
                
                pwr_map = {0: "자동", 1: "배터리", 2: "상전"}
                cat_map = {0: "기본LED", 1: "날씨모드", 2: "음악모드"}

                print(f"\n[📡 16비트 제어 수신] Hex: 0x{full_cmd_16bit:04X}")
                print(f"  ├─ 전원공급: {pwr_map.get(parsed['power_supply_mode'], '기타')}")
                print(f"  ├─ 동작상태: {'🌙 슬립 모드' if parsed['is_sleep'] else '⚡ 일반 동작'}")
                print(f"  ├─ 프로젝터: {'📽️ ON' if parsed['is_prj_on'] else '⬛ OFF'}")
                print(f"  └─ 카테고리: {cat_map.get(parsed['category'], '기타')} (서브패턴 #{parsed['sub_mode']})")

                # Case A: 전체 난간 일괄 제어
                if web_rail_seq == 0:
                    success_rails = []
                    fail_rails = []

                    for r_id in range(1, TOTAL_RAILS + 1):
                        target_msb = (parsed['msb'] & 0xF0) | (r_id & 0x0F)
                        resp = send_plc_16bit_bytes(msb_byte=target_msb, lsb_byte=parsed['lsb'], expected_bytes=2, wait_sec=WAIT_RESPONSE_SEC)
                        
                        if resp and len(resp) >= 2 and resp[0] == r_id:
                            success_rails.append(r_id)
                        else:
                            fail_rails.append(r_id)

                        time.sleep(0.01)

                    print(f"  └─ ✅ 제어 성공 난간({len(success_rails)}개): {success_rails}")

                    result_payload = {
                        "apiKey": API_KEY,
                        "railSeq": 0,
                        "railMode": full_cmd_16bit,
                        "successRails": success_rails,
                        "failRails": fail_rails
                    }
                    ws.send(f"SEND\ndestination:/pub/rails/mode/confirm\ncontent-type:application/json\n\n{json.dumps(result_payload)}\x00")

                # Case B: 단일 난간 제어
                else:
                    target_msb = (parsed['msb'] & 0xF0) | (web_rail_seq & 0x0F)
                    resp = send_plc_16bit_bytes(msb_byte=target_msb, lsb_byte=parsed['lsb'], expected_bytes=2, wait_sec=WAIT_RESPONSE_SEC)

                    if resp and len(resp) >= 2 and resp[0] == web_rail_seq:
                        print(f"  └─ ✅ [단일 제어 성공] 난간 #{web_rail_seq}")
                        confirm_payload = {
                            "apiKey": API_KEY, 
                            "railSeq": web_rail_seq, 
                            "railMode": full_cmd_16bit, 
                            "successRails": [web_rail_seq],
                            "failRails": []
                        }
                        ws.send(f"SEND\ndestination:/pub/rails/mode/confirm\ncontent-type:application/json\n\n{json.dumps(confirm_payload)}\x00")
                    else:
                        print(f"  └─ ❌ [단일 제어 실패] 난간 #{web_rail_seq} 응답 없음")
                        fail_payload = {
                            "apiKey": API_KEY, 
                            "railSeq": web_rail_seq, 
                            "railMode": full_cmd_16bit, 
                            "isError": True, 
                            "errorMessage": f"난간 #{web_rail_seq} 하드웨어 무응답"
                        }
                        ws.send(f"SEND\ndestination:/pub/rails/mode/fail\ncontent-type:application/json\n\n{json.dumps(fail_payload)}\x00")

            except json.JSONDecodeError:
                pass

        except (socket.timeout, ssl.SSLError):
            continue
        except (WebSocketConnectionClosedException, ConnectionResetError, BrokenPipeError):
            break
        except Exception:
            break


# ==========================================
# 5. 백엔드 연결 및 관제 루프
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
            print(f"⚠️ 시리얼 포트 미연결: {se}")

        ws = create_connection(WS_URL, sslopt={"cert_reqs": ssl.CERT_NONE}, timeout=10)
        print("\n==========================================")
        print("🟢 에코루미너스 백엔드 서버 웹소켓 연결 성공!")

        ws.send("CONNECT\naccept-version:1.1,1.0\nheart-beat:10000,10000\n\n\x00")
        ws.recv()

        sub_destination = f"/sub/device/{API_KEY}/mode"
        ws.send(f"SUBSCRIBE\nid:sub-0\ndestination:{sub_destination}\n\n\x00")

        recv_thread = threading.Thread(target=receive_messages, args=(ws,), daemon=True)
        recv_thread.start()

        save_thread = threading.Thread(target=ten_minute_save_loop, args=(ws,), daemon=True)
        save_thread.start()

        print(f"⚡ [{TARGET_CYCLE_SEC}초 관제 시작] 1~{TOTAL_RAILS}번 난간 8바이트 상태 수집 폴링 중...\n")

        while True:
            cycle_start_time = time.time()
            success_count = 0

            for rail_id in range(1, TOTAL_RAILS + 1):
                poll_msb = rail_id & 0x0F
                # 💡 expected_bytes=8 로 확대 수신
                resp = send_plc_16bit_bytes(msb_byte=poll_msb, lsb_byte=0xFF, expected_bytes=8, wait_sec=WAIT_RESPONSE_SEC)

                if resp and len(resp) == 8 and resp[0] == rail_id:
                    # 1. 좌/우 충전량(W) 파싱
                    l_mv = (resp[1] << 8) | resp[2]
                    r_mv = (resp[3] << 8) | resp[4]
                    left_watt = round((l_mv / 1000.0) * 1.2, 1)
                    right_watt = round((r_mv / 1000.0) * 1.1, 1)

                    # 2. ESP32에서 동기화되어 올라온 16비트 현재 모드 정수
                    esp_msb = resp[5]
                    esp_lsb = resp[6]
                    esp_mode_16bit = (esp_msb << 8) | esp_lsb

                    # 3. 배터리 % 파싱
                    battery_pct = int(resp[7])
                    is_error = False
                    success_count += 1

                    with buffer_lock:
                        if rail_id not in data_buffer:
                            data_buffer[rail_id] = {"left": [], "right": [], "battery": []}
                        data_buffer[rail_id]["left"].append(left_watt)
                        data_buffer[rail_id]["right"].append(right_watt)
                        data_buffer[rail_id]["battery"].append(battery_pct)
                else:
                    is_error = True
                    left_watt, right_watt, battery_pct = 0.0, 0.0, 0
                    esp_mode_16bit = 0

                is_emergency = (battery_pct < 10) and not is_error

                # 백엔드로 실제 측정한 충전량, 모드 정수, 배터리 잔량 전송
                payload = {
                    "apiKey": API_KEY,
                    "railSeq": rail_id,
                    "leftWatt": left_watt,
                    "rightWatt": right_watt,
                    "batteryPct": battery_pct,
                    "railMode": esp_mode_16bit,
                    "isCharging": not is_error,
                    "isEmergency": is_emergency,
                    "isError": is_error
                }
                
                try:
                    ws.send(f"SEND\ndestination:/pub/rails/realtime\ncontent-type:application/json\n\n{json.dumps(payload)}\x00")
                except Exception:
                    pass

                time.sleep(0.01)

            scan_elapsed_time = time.time() - cycle_start_time
            print(f"📡 [{TARGET_CYCLE_SEC}초 관제 완료] 전체 {TOTAL_RAILS}개 중 {success_count}개 8바이트 정상 수신 (소요시간: {scan_elapsed_time:.2f}초)")

            remaining_sleep = TARGET_CYCLE_SEC - scan_elapsed_time
            if remaining_sleep > 0:
                time.sleep(remaining_sleep)
            else:
                time.sleep(0.01)

    except Exception as e:
        print(f"🔴 서버 연결 에러: {e}")
    finally:
        if ws:
            try:
                ws.close()
            except Exception:
                pass


def main():
    print("🚀 에코루미너스 관제 마스터 agent 구동...")
    while True:
        try:
            connect_and_run()
        except Exception as err:
            print("메인 루프 재시작 중... 에러:", err)
        time.sleep(2)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        if ser and ser.is_open:
            ser.close()

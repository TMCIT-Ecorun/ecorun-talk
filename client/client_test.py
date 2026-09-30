import asyncio
import sounddevice as sd
import numpy as np
import opuslib
import tkinter as tk
import threading
import queue
import serial_asyncio
import os
from dotenv import load_dotenv

from modem_manager import initialize_sim7070g, SIM7070WebSocketClient

load_dotenv()

SERIAL_PORT = "/dev/ttyUSB5"
BAUD_RATE = 115200
SERVER_URL = os.environ["SERVER_URL"]
SAMPLE_RATE = 8000
CHANNELS = 1         # モノラル入力
FRAME_DURATION = 0.06
BLOCK_SIZE = int(SAMPLE_RATE * FRAME_DURATION) # 8000 * 0.06 = 480 サンプル

total_sent_bytes = 0
total_received_bytes = 0

# PTT状態管理用グローバルフラグ
is_ptt_active = False

def setup_ptt_window():
    global is_ptt_active
    root = tk.Tk()
    root.title("PTT Controller (Click & Hold)")
    root.geometry("250x150")
    
    label = tk.Label(root, text="【PTTボタン】\n\nこのボタンを「押し続けている間」だけ\n音声が送信されます", font=("Helvetica", 10), bg="lightgray")
    label.pack(expand=True, fill="both", padx=15, pady=15)

    def on_press(event):
        global is_ptt_active
        if not is_ptt_active:
            is_ptt_active = True
            label.config(bg="orange", text="送信中... (Speaking)")
            print("[PTT] ON (Speaking...)", flush=True)

    def on_release(event):
        global is_ptt_active
        if is_ptt_active:
            is_ptt_active = False
            label.config(bg="lightgray", text="【PTTボタン】\n\nこのボタンを「押し続けている間」だけ\n音声が送信されます")
            print("[PTT] OFF (Muted)", flush=True)

    # マウスの左クリックの「押し始め」と「離した瞬間」をバインド
    label.bind('<ButtonPress-1>', on_press)
    label.bind('<ButtonRelease-1>', on_release)
    
    root.mainloop()

async def run_client():
    print(f"Connecting to serial modem: {SERIAL_PORT}", flush=True)
    reader, writer = await serial_asyncio.open_serial_connection(
        url=SERIAL_PORT, baudrate=BAUD_RATE
    )

    client = SIM7070WebSocketClient(reader, writer)
    await initialize_sim7070g(reader, writer)
    await client.connect(SERVER_URL)
    print("Connected! Full-duplex audio stream ready.", flush=True)

    # エンコーダー・デコーダー（8kHz モノラル、8kbps設定）
    encoder = opuslib.Encoder(SAMPLE_RATE, 1, opuslib.APPLICATION_VOIP)
    encoder.bitrate = 8000  # 8kbpsで帯域を徹底的に絞る
    decoder = opuslib.Decoder(SAMPLE_RATE, 1)

    audio_queue = asyncio.Queue()
    loop = asyncio.get_running_loop()

    # マイク入力コールバック（送信側）
    def audio_callback(indata, frames, time_info, status):
        if status:
            print(status, flush=True)
        mono_data = np.mean(indata, axis=1, keepdims=True)
        asyncio.run_coroutine_threadsafe(audio_queue.put(mono_data.copy()), loop)

    # 送信用の非同期ループ
    async def send_loop():
        global total_sent_bytes, is_ptt_active
        print("PTT Ready: Click and hold the GUI button to talk!", flush=True)

        try:
            with sd.InputStream(
                samplerate=SAMPLE_RATE,
                channels=CHANNELS,
                blocksize=BLOCK_SIZE,
                callback=audio_callback,
            ):
                while True:
                    data = await audio_queue.get()
                    if not is_ptt_active:
                        continue

                    pcm_bytes = (data * 32767).astype(np.int16).tobytes()
                    try:
                        opus_data = encoder.encode(pcm_bytes, BLOCK_SIZE)
                        total_sent_bytes += len(opus_data)
                        await client.send_bytes(opus_data)
                    except Exception as e:
                        print(f"Send error: {e}", flush=True)
                        break
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"InputStream error: {e}", flush=True)

    playback_queue = queue.Queue()

    def speaker_callback(outdata, frames, time_info, status):
        if status:
            print(status, flush=True)
        try:
            data = playback_queue.get_nowait()
            if len(data) < len(outdata):
                outdata[:len(data)] = data
                outdata[len(data):] = 0
            else:
                outdata[:] = data[:len(outdata)]
        except queue.Empty:
            outdata.fill(0)

    speaker_stream = sd.OutputStream(
        samplerate=SAMPLE_RATE,
        channels=1,
        dtype="int16",
        blocksize=BLOCK_SIZE,
        callback=speaker_callback,
    )
    speaker_stream.start()

    # メインの送受信ハンドリング
    async def handle_modem():
        global total_received_bytes
        send_task = asyncio.create_task(send_loop())

        async def on_bytes(opus_data: bytes):
            global total_received_bytes
            total_received_bytes += len(opus_data)
            pcm_bytes = decoder.decode(opus_data, BLOCK_SIZE)
            audio_data = np.frombuffer(pcm_bytes, dtype=np.int16).reshape(-1, 1)
            playback_queue.put(audio_data)

        try:
            client.on_bytes = on_bytes
            await client.receive_loop()
        except Exception as e:
            print(f"Receive error: {e}")
        finally:
            send_task.cancel()
            speaker_stream.stop()
            speaker_stream.close()

    await handle_modem()
    writer.close()
    await writer.wait_closed()

if __name__ == "__main__":
    try:
        # TkinterのGUIウィンドウを別スレッドで起動
        threading.Thread(target=setup_ptt_window, daemon=True).start()
        # 非同期クライアントの実行
        asyncio.run(run_client())
    except KeyboardInterrupt:
        print(f"\n[Stats] Received: {total_received_bytes} bytes, Sent: {total_sent_bytes} bytes")
        print("Stopped by user.")

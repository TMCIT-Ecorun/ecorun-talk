import asyncio
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import discord
from discord.ext import commands
from dotenv import load_dotenv
import numpy as np
import opuslib
import queue

load_dotenv()

intents = discord.Intents.default()
intents.voice_states = True
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# 音声パラメータの定義
SAMPLE_RATE = 8000
FRAME_DURATION = 0.06  # 60ms
BLOCK_SIZE = int(SAMPLE_RATE * FRAME_DURATION)  # 480 サンプル

voice_client: discord.VoiceClient = None
audio_queue = queue.Queue()

# 8kHz モノラルのデコーダー / エンコーダー
decoder = opuslib.Decoder(SAMPLE_RATE, 1)
server_encoder = opuslib.Encoder(SAMPLE_RATE, 1, opuslib.APPLICATION_VOIP)

class WebSocketAudioSource(discord.AudioSource):
    def __init__(self, queue: asyncio.Queue):
        self.queue = queue
        self._buffer = bytearray()

    def read(self) -> bytes:
        FRAME_SIZE_DISCORD = 3840  # 20ms分 (48kHz, 2ch)
        
        try:
            # 同期キューからノンブロックで取得
            chunk = self.queue.get_nowait()
            return chunk
        except queue.Empty:
            # キューにデータがない場合は無音を返す
            return b'\x00' * FRAME_SIZE_DISCORD

server_to_client_queue = asyncio.Queue()

class LiveAudioSink(discord.sinks.Sink):
    def __init__(self, bot_instance):
        super().__init__()
        self.bot = bot_instance
        self._buffer = bytearray()

    @discord.sinks.Filters.container
    def write(self, data: bytes, user: discord.abc.User, *args):
        if user.id == self.bot.user.id:
            return
        
        try:
            if hasattr(data, 'pcm'):
                pcm_data = data.pcm
            elif isinstance(data, bytes):
                pcm_data = data

            # data は 48kHz ステレオ PCM (s16le)
            pcm_array = np.frombuffer(pcm_data, dtype=np.int16)
            
            
            # 1. ステレオ(2ch)からモノラル(1ch)へ平均化
            if len(pcm_array) % 2 == 0:
                pcm_mono = ((pcm_array[0::2].astype(np.int32) + pcm_array[1::2].astype(np.int32)) // 2).astype(np.int16)
            else:
                pcm_mono = pcm_array

            # 2. 48kHz -> 8kHzにダウンサンプル (6分の1)
            pcm_8k = pcm_mono[::6]
            
            # 3. バッファに溜めて、目標のBLOCK_SIZE（480サンプル = 960バイト）ごとに綺麗に切り出す
            self._buffer.extend(pcm_8k.tobytes())
            
            target_byte_size = BLOCK_SIZE * 2  # 480サンプル × 2バイト = 960バイト
            
            while len(self._buffer) >= target_byte_size:
                chunk_bytes = bytes(self._buffer[:target_byte_size])
                del self._buffer[:target_byte_size]
                
                # Opusで圧縮 (60ms分)
                opus_data = server_encoder.encode(chunk_bytes, BLOCK_SIZE)
                
                # 送信キューへ綺麗にインプット
                asyncio.run_coroutine_threadsafe(
                    server_to_client_queue.put(opus_data), 
                    self.bot.loop
                )
        except Exception as e:
            print(f"Audio processing error: {e}", flush=True)

    def cleanup(self):
        pass

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")

@bot.slash_command(name="join", description="Botを指定したVCに参加させます")
async def join_vc(ctx: discord.ApplicationContext):
    global voice_client
    if ctx.author.voice:
        channel = ctx.author.voice.channel
        if voice_client and voice_client.is_connected():
            await voice_client.move_to(channel)
        else:
            voice_client = await channel.connect()
            if not voice_client.is_playing():
                source = WebSocketAudioSource(audio_queue)
                voice_client.play(source)
        await ctx.respond(f"Joined {channel.name}!", ephemeral=True)
    else:
        await ctx.respond("You are not in a voice channel!", ephemeral=True)

@bot.slash_command(name="leave", description="BotをVCから退出させます")
async def leave_vc(ctx: discord.ApplicationContext):
    global voice_client
    if voice_client and voice_client.is_connected():
        if voice_client.is_playing():
            voice_client.stop()
        await voice_client.disconnect()
        voice_client = None
        await ctx.respond("Left the voice channel.", ephemeral=True)
    else:
        await ctx.respond("I am not in a voice channel.", ephemeral=True)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Pycord creates Client.loop when the Bot is instantiated. Since this
    # module is imported before uvicorn starts its event loop, that loop can
    # differ from the loop running FastAPI. __aenter__ rebinds the client,
    # HTTP state, and gateway state to the currently running loop. Voice
    # connections then create their connector tasks on the same loop as slash
    # command handlers.
    async with bot:
        bot_task = asyncio.create_task(bot.start(os.environ["DISCORD_TOKEN"]))
        try:
            yield
        finally:
            if voice_client and voice_client.is_connected():
                if voice_client.is_playing():
                    voice_client.stop()
                await voice_client.disconnect()
            if not bot.is_closed():
                await bot.close()
            try:
                await bot_task
            except asyncio.CancelledError:
                pass

app = FastAPI(lifespan=lifespan)

@app.websocket("/ws/audio")
async def websocket_endpoint(websocket: WebSocket):
    global voice_client
    await websocket.accept()
    print("Client connected (Full-Duplex Stream).", flush=True)

    sink = None
    if voice_client and voice_client.is_connected():
        if not voice_client.is_recording():
            try:
                sink = LiveAudioSink(bot)
                voice_client.start_recording(sink, lambda *args: None, "")
                print("Started listening to Discord VC audio.", flush=True)
            except Exception as e:
                print(f"Failed to start recording: {e}", flush=True)

    async def client_sender():
        try:
            while True:
                opus_data = await server_to_client_queue.get()
                await websocket.send_bytes(opus_data)
        except Exception:
            pass

    sender_task = asyncio.create_task(client_sender())

    try:
        while True:
            # Pi 4からの音声を受信
            opus_data = await websocket.receive_bytes()
            
            # 1. 8kHzモノラルでデコード (480 サンプル = 960 bytes)
            pcm_bytes = decoder.decode(opus_data, BLOCK_SIZE)
            pcm_array = np.frombuffer(pcm_bytes, dtype=np.int16)
            
            # 2. 8kHz -> 48kHzにアップサンプル (6倍に引き伸ばし: 480 -> 2880 サンプル)
            resampled = np.repeat(pcm_array, 6)
            
            # 3. モノラルからステレオ（左右同じ音を複製）に拡張 (2880 * 2 = 5760 サンプル)
            stereo_array = np.column_stack((resampled, resampled)).flatten()
            
            # 4. バイト列に変換 (5760 サンプル * 2 bytes = 11520 バイト)
            stereo_bytes = stereo_array.tobytes()
            
            # 5. Discordが要求する 1チャンク = 3840 バイト (20ms分) ずつに分割してキューへ
            # (11520 バイトは 3840バイト × 3 なので、ちょうど3個の塊に綺麗に割れます)
            chunk_size = 3840  
            for i in range(0, len(stereo_bytes), chunk_size):
                sub_chunk = stereo_bytes[i:i + chunk_size]
                if len(sub_chunk) == chunk_size:
                    audio_queue.put(sub_chunk)
            
    except WebSocketDisconnect:
        print("Client disconnected.", flush=True)
    finally:
        if voice_client and voice_client.is_connected():
            if voice_client.is_recording():
                try:
                    voice_client.stop_recording()
                    print("Stopped recording successfully.", flush=True)
                except Exception as e:
                    print(f"Error stopping recording: {e}", flush=True)

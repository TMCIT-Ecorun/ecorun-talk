import asyncio
import os
import serial_asyncio
import base64
import ipaddress
import re
from urllib.parse import urlparse
from typing import Optional, Callable, Awaitable
from dotenv import load_dotenv
from wsproto import WSConnection, ConnectionType
from wsproto.events import AcceptConnection, TextMessage, BytesMessage, CloseConnection, Request
from wsproto.extensions import PerMessageDeflate
from wsproto.handshake import client_extensions_handshake

load_dotenv()

CONN_RETRY = 3

class ModemError(Exception):
    """SIMモジュールのATコマンドエラーや接続異常を表す例外"""
    pass

async def send_command(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, cmd: str, wait_time=0.5, timeout=5.0, raise_on_error=True) -> str:
    """ATコマンドを非同期で送信し、レスポンスを取得する（raise_on_errorでエラーの許容を切り替え可能）"""
    writer.write((cmd + "\r\n").encode('utf-8'))
    await writer.drain()
    await asyncio.sleep(wait_time)

    response = ""
    start_time = asyncio.get_event_loop().time()
    
    while True:
        try:
            line = await asyncio.wait_for(reader.readline(), timeout=0.2)
            if not line:
                break
            decoded = line.decode('utf-8', errors='ignore')
            response += decoded
            
            if "ERROR" in decoded.strip():
                break
        except asyncio.TimeoutError:
            if asyncio.get_event_loop().time() - start_time > timeout:
                break
            continue

        if "OK" in response or "ERROR" in response or ">" in response:
            break

    print(f"CMD: {cmd}\nRES:\n{response.strip()}", flush=True)

    if raise_on_error and "ERROR" in response:
        raise ModemError(f"Command failed [{cmd}]: {response.strip()}")

    return response

async def wait_for_urc(reader: asyncio.StreamReader, target_prefix: str, timeout=15.0) -> str:
    """非同期通知（URC）を指定タイムアウトまで待ち受ける"""
    start_time = asyncio.get_event_loop().time()
    accumulated_response = ""
    
    while asyncio.get_event_loop().time() - start_time < timeout:
        try:
            line = await asyncio.wait_for(reader.readline(), timeout=0.5)
            if line:
                decoded = line.decode('utf-8', errors='ignore').strip()
                if decoded:
                    print(f"RX: {decoded}", flush=True)
                    accumulated_response += decoded + "\n"
                    if target_prefix in decoded:
                        return accumulated_response
        except asyncio.TimeoutError:
            continue
            
    raise TimeoutError(f"Timeout waiting for URC: {target_prefix}")

async def initialize_sim7070g(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """SIM7070GのAPN設定、ネットワーク接続を初期化する（最初のCNACTはエラーを許容）"""
    print("Initializing SIM7070G modem...", flush=True)
    
    await send_command(reader, writer, 'AT+CNACT=0,0', wait_time=1.0, raise_on_error=False)
    await send_command(reader, writer, 'AT+CGDCONT=1,"IP","soracom.io"', wait_time=1.0)
    res = await send_command(reader, writer, 'AT+CNACT=0,1', wait_time=1.0)

    print("SIM7070G initialization sequence completed.", flush=True)
    return res

def parse_cnact_ip(response_text: str) -> Optional[ipaddress.IPv4Address]:
    """AT+CNACTのレスポンスから有効なIPアドレスをパースする"""
    matches = re.findall(r'"([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)"', response_text)
    
    for ip_str in matches:
        try:
            ip_obj = ipaddress.ip_address(ip_str)
            if not ip_obj.is_unspecified:
                return ip_obj
        except ValueError:
            continue
            
    return None

class SIM7070WebSocketClient:
    def __init__(
        self, 
        reader: asyncio.StreamReader, 
        writer: asyncio.StreamWriter,
        on_text: Optional[Callable[[str], Awaitable[None]]] = None,
        on_bytes: Optional[Callable[[bytes], Awaitable[None]]] = None,
        on_close: Optional[Callable[[], Awaitable[None]]] = None,
        on_error: Optional[Callable[[Exception], Awaitable[None]]] = None
    ):
        self.reader = reader
        self.writer = writer
        self.is_ssl_init = None
        self.ws = None
        
        self.on_text = on_text
        self.on_bytes = on_bytes
        self.on_close = on_close
        self.on_error = on_error

    async def receive_socket_data(self, capacity: int = 1460) -> bytes:
        """Read one binary-safe +CAURC receive payload from the modem.

        recv_mode=1 is used for CAOPEN, so the modem pushes received socket
        data as a URC instead of requiring a CARECV poll. This avoids racing
        the modem's asynchronous receive indication with AT command reads.
        """
        if capacity <= 0 or capacity > 1460:
            raise ValueError("capacity must be between 1 and 1460")

        deadline = asyncio.get_event_loop().time() + 15.0
        prefix = b'+CAURC: "recv",0,'
        while True:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                raise TimeoutError("Timeout waiting for +CAURC receive data")
            line = await asyncio.wait_for(self.reader.readline(), remaining)
            if not line:
                raise ConnectionError("Serial connection closed while waiting for +CAURC")
            if line.startswith(prefix):
                break
            decoded = line.decode("utf-8", errors="replace").strip()
            if decoded:
                print(f"RX-URC: {decoded}", flush=True)

        try:
            length = int(line[len(prefix):].strip())
        except ValueError as exc:
            raise ModemError(f"Malformed +CAURC receive header: {line!r}") from exc
        if length > capacity:
            raise ModemError(f"+CAURC returned {length} bytes for capacity {capacity}")
        data = await asyncio.wait_for(
            self.reader.readexactly(length),
            max(0.1, deadline - asyncio.get_event_loop().time()),
        ) if length else b""

        # URC payload is terminated by CRLF, but the payload itself is binary.
        trailer = await asyncio.wait_for(
            self.reader.readexactly(2),
            max(0.1, deadline - asyncio.get_event_loop().time()),
        )
        if trailer != b"\r\n":
            raise ModemError(f"Malformed +CAURC receive trailer: {trailer!r}")
        return data

    async def init_ssl(self, hostname: str):
        if self.is_ssl_init == hostname:
            return
            
        at_commands = [
            'AT+CSSLCFG="SSLVERSION",0,3',
            'AT+CSSLCFG="IGNORERTCTIME",0,1',
            f'AT+CSSLCFG="SNI",0,"{hostname}"',
            # Tailscale's endpoint currently negotiates this TLS 1.2 suite.
            # Explicitly selecting it avoids older SIM7070G firmware choosing
            # an incompatible default suite during the handshake.
            'AT+CSSLCFG="CIPHERSUITE",0,0,0xC02B',
            'AT+CASSLCFG=0,"SSL",1',
            'AT+CASSLCFG=0,"CRINDEX",0',
        ]

        for cmd in at_commands:
            await send_command(self.reader, self.writer, cmd)
            
        self.is_ssl_init = hostname

    async def _wait_for_ok(self, operation: str, timeout: float = 5.0):
        """Consume the final OK emitted after a multi-stage AT operation."""
        deadline = asyncio.get_event_loop().time() + timeout
        while True:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                raise TimeoutError(f"Timeout waiting for {operation} completion")
            line = await asyncio.wait_for(self.reader.readline(), remaining)
            if not line:
                raise ConnectionError(f"Serial connection closed during {operation}")
            decoded = line.decode("utf-8", errors="replace").strip()
            if decoded == "OK":
                return
            if "ERROR" in decoded:
                raise ModemError(f"{operation} failed: {decoded}")
            if decoded:
                print(f"RX: {decoded}", flush=True)

    async def _send_socket_data(self, data: bytes):
        """Send socket bytes and consume CASEND's final result."""
        await send_command(
            self.reader,
            self.writer,
            # TLS record construction can take longer on SIM7070G than the
            # default 5 s CASEND input window. Use the documented maximum.
            f"AT+CASEND=0,{len(data)},10000",
            wait_time=0.2,
        )
        self.writer.write(data)
        await self.writer.drain()
        await self._wait_for_ok("CASEND")

    async def connect(self, url: str):
        await send_command(self.reader, self.writer, "AT+CMEE=2")
        # 1. URLパース
        parsed_url = urlparse(url)
        is_secure = parsed_url.scheme == "wss"
        hostname = parsed_url.hostname
        port = parsed_url.port or (443 if is_secure else 80)
        path = parsed_url.path or "/"
        if parsed_url.query:
            path += f"?{parsed_url.query}"

        # 2. ネットワーク有効性確認
        active_ip = None
        for i in range(CONN_RETRY):
            cnact_res = await send_command(self.reader, self.writer, "AT+CNACT?")
            active_ip = parse_cnact_ip(cnact_res)
            if active_ip:
                print(f"Network active. IP: {active_ip}", flush=True)
                break
            print(f"No active network connection found. retry {i}")
        else:
            raise ModemError("No active network connection found (CNACT IP is 0.0.0.0 or missing).")

        # 3. ソケットを先に閉じる。SSL設定はCIDが空いている状態で行う。
        socket_type = "TCP"
        print(f"Opening socket to {hostname}:{port}...", flush=True)

        try:
            await send_command(self.reader, self.writer, 'AT+CACLOSE=0', wait_time=0.2, raise_on_error=False)
        except ModemError:
            pass

        await send_command(self.reader, self.writer, 'AT+CACID=0')

        # 4. WSSの場合はSSL初期化
        if is_secure:
            await self.init_ssl(hostname)

        # recv_mode=1 makes SIM7070G deliver socket data as +CAURC directly.
        # This is safer than polling CARECV because the modem can receive the
        # HTTP 101 asynchronously while the host is still processing AT lines.
        self.writer.write((f'AT+CAOPEN=0,0,"{socket_type}","{hostname}",{port},1\r\n').encode('utf-8'))
        await self.writer.drain()
        
        try:
            caopen_res = await wait_for_urc(self.reader, "+CAOPEN:", timeout=15.0)
        except TimeoutError as e:
            raise ModemError(f"Socket open timeout: {e}")
        
        if "+CAOPEN: 0,0" not in caopen_res:
            raise ModemError(f"Failed to open socket, server response: {caopen_res}")
        await self._wait_for_ok("CAOPEN")
        state = await send_command(self.reader, self.writer, "AT+CASTATE?")
        print(f"Socket state after CAOPEN: {state.strip()}", flush=True)

        print("Socket opened successfully. Initiating wsproto handshake...", flush=True)

        # 5. wsproto v1.x系に対応
        self.ws = WSConnection(connection_type=ConnectionType.CLIENT)
        handshake_data = self.ws.send(Request(host=hostname+(f":{parsed_url.port}" if parsed_url.port else ""), target=path, extra_headers=[(b"User-Agent",b"wsproto/py3.12"),], extensions=[PerMessageDeflate()]))

        # 以前の直接書き込み方式に戻す
        print(handshake_data, flush=True)
        await self._send_socket_data(handshake_data)
        
        print("Waiting for server HTTP 101 handshake response...", flush=True)
        # 6. ハンドシェイク応答の受信と確認
        response_raw = bytearray()
        res = await self.receive_socket_data(1460)
        print(f"RECV-RESP: {len(res)} bytes", flush=True)
        response_raw.extend(res)

        if not response_raw:
            raise ModemError("No WebSocket handshake response received")

        print(f"Feeding to wsproto: {len(response_raw)} bytes", flush=True)
        self.ws.receive_data(bytes(response_raw))
        handshake_events = list(self.ws.events())
        for event in handshake_events:
            print(f"WS HANDSHAKE EVENT: {event!r}", flush=True)
        if not any(isinstance(event, AcceptConnection) for event in handshake_events):
            raise ModemError(
                "Server response was received, but wsproto did not emit AcceptConnection"
            )
        print("WebSocket handshake accepted (HTTP 101).", flush=True)

    async def send_message(self, message: str):
        """テキストメッセージを送信する"""
        if not self.ws:
            raise RuntimeError("WebSocket is not connected.")
        
        try:
            frame_bytes = self.ws.send(TextMessage(data=message))
            
            await self._send_socket_data(frame_bytes)
        except Exception as e:
            if self.on_error:
                await self.on_error(e)
            raise

    async def send_bytes(self, message: bytes):
        """バイナリメッセージを送信する"""
        if not self.ws:
            raise RuntimeError("WebSocket is not connected.")

        try:
            frame_bytes = self.ws.send(BytesMessage(data=message))
            await self._send_socket_data(frame_bytes)
        except Exception as e:
            if self.on_error:
                await self.on_error(e)
            raise

    async def receive_loop(self):
        """受信ループ（CloseConnection 対応・コールバック呼び出し対応）"""
        consecutive_errors = 0
        max_consecutive_errors = 5

        while True:
            try:
                response = await self.receive_socket_data(512)
                consecutive_errors = 0
                
                if response:
                    self.ws.receive_data(response)
                    for event in self.ws.events():
                        if isinstance(event, TextMessage):
                            if self.on_text:
                                await self.on_text(event.data)
                        elif isinstance(event, BytesMessage):
                            if self.on_bytes:
                                await self.on_bytes(event.data)
                        elif isinstance(event, CloseConnection):
                            print(f"WS Close requested by server. Code: {event.code}, Reason: {event.reason}", flush=True)
                            
                            close_response_bytes = self.ws.send(event.response())
                            if close_response_bytes:
                                await send_command(self.reader, self.writer, f"AT+CASEND=0,{len(close_response_bytes)}", wait_time=0.2)
                                self.writer.write(close_response_bytes)
                                await self.writer.drain()

                            if self.on_close:
                                await self.on_close()
                            return
                            
            except ModemError as me:
                print(f"Modem Error in receive_loop: {me}", flush=True)
                consecutive_errors += 1
            except Exception as e:
                print(f"Unexpected error in receive_loop: {e}", flush=True)
                consecutive_errors += 1
                if self.on_error:
                    await self.on_error(e)

            if consecutive_errors >= max_consecutive_errors:
                print("Too many consecutive errors. Exiting receive_loop.", flush=True)
                if self.on_error:
                    await self.on_error(ConnectionError("Max consecutive errors reached."))
                break
                
            await asyncio.sleep(1.0)

# --- 使用例とコールバックの定義 ---
async def handle_text(text: str):
    print(f"[Callback] 受信したテキスト: {text}", flush=True)

async def handle_close():
    print("[Callback] WebSocketが切断されました。", flush=True)

async def handle_error(err: Exception):
    print(f"[Callback] エラーが発生しました: {err}", flush=True)

async def main():
    serial_port = "/dev/ttyUSB5"
    baud_rate = 115200

    print(f"Connecting to serial port {serial_port}...", flush=True)
    reader, writer = await serial_asyncio.open_serial_connection(
        url=serial_port, baudrate=baud_rate
    )

    try:
        await initialize_sim7070g(reader, writer)
        
        client = SIM7070WebSocketClient(
            reader, writer,
            on_text=handle_text,
            on_close=handle_close,
            on_error=handle_error
        )
        
        await client.connect(os.environ["SERVER_URL"])
        await client.receive_loop()

    except Exception as e:
        print(f"致命的なエラーが発生しました: {e}")
    finally:
        print("Closing serial connection.")
        writer.close()
        await writer.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())

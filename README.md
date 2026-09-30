# EcoRun 2026

Raspberry Pi / SIM7070G と Discord をつないだ低帯域の双方向音声ストリーミング実験です。

## 構成

- `client/modem_manager.py`: SIM7070G の AT コマンド制御と WebSocket 通信
- `client/client_test.py`: マイク・スピーカー、PTT 操作、Opus 音声処理を担当
- `server/server.py`: FastAPI の WebSocket と Discord Bot を使った音声中継
- `.env.example`: Discord Bot 用環境変数の例

## 動作概要

クライアント側は 8 kHz モノラル音声を Opus で圧縮し、SIM7070G 経由で WebSocket サーバーへ送信します。サーバー側では Discord のボイスチャンネルとの間で音声を変換し、双方向に中継します。クライアントの送信は PTT ボタンを押している間だけ有効です。

## 必要なもの

- Python 3
- SIM7070G とシリアル接続できる環境
- Discord Bot とボイスチャンネル
- クライアント側の音声入出力デバイス
- サーバー側で利用できる FastAPI / Discord / Opus 関連パッケージ

依存パッケージは環境に合わせてインストールしてください。

## 設定

1. `.env.example` をコピーして `.env` を作り、`DISCORD_TOKEN` と `SERVER_URL` を設定します。
2. `client/client_test.py` の `SERIAL_PORT` を実環境に合わせます。
3. `client/modem_manager.py` の APN 設定が利用する SIM 回線と一致していることを確認します。

`.env` は `python-dotenv` で自動的に読み込まれます。`.env` 自体は Git 管理対象外です。

## 起動

サーバーでは FastAPI アプリを ASGI サーバーから起動し、クライアントでは次を実行します。

```bash
python client/client_test.py
```

Discord 側では Bot を起動したあと、`/join` でボイスチャンネルに参加させます。`/leave` で退出できます。

## 注意

このリポジトリには Discord の認証トークンなどの秘密情報をコミットしないでください。

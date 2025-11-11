# Wasabi オブジェクトストレージ検証用ツール群

このプロジェクトは [Wasabi](https://wasabi.com/) 互換S3ストレージのMD5/CRC32Cベリファイやmultipartアップロード検証用スクリプト集です。

---

## セットアップ

1. **Poetry で依存関係をインストール**

```
poetry install
```

2. **環境変数設定**

`.env-example` をコピー:

```
cp .env-example .env
```

必要な値を `.env` に記入:

- `WASABI_ENDPOINT` 例: https://s3.ap-northeast-1.wasabisys.com
- `WASABI_ACCESS_KEY_ID` (Wasabi管理画面で発行)
- `WASABI_SECRET_ACCESS_KEY`
- `WASABI_BUCKET` (作成したバケット名)

---

## 各スクリプトの概要

### 1. `wasabi-md5.py`

単一オブジェクトの **Content-MD5** 検証アップロード用。サーバ側でMD5照合が正しく行われるかテストします。

#### 使い方

```
poetry run python wasabi-md5.py
```

- 結果として、正常・異常系（意図的なMD5不一致）のアップロードでWasabi側のレスポンス挙動が確認できます。

---

### 2. `wasabi-mp-crc32c.py`

S3互換の **multipartアップロード + CRC32C** チェック付き検証。

#### 使い方

```
poetry run python wasabi-mp-crc32c.py /path/to/largefile [part_size_mib]
```

- `sample-video-1080p.mp4` 等の動画ファイルで試すことを推奨
- 指定したファイルを分割し、各パートにCRC32C(B64)を付加してアップロード
- `bad_part_index`による異常系テストもスクリプトに含む

---

## サンプル動画について

- `sample-video-1080p.mp4` および `sample-video-4k.mp4` は**本物の動画ファイル**です。
  - 形式: 標準的なH.264/MP4
  - テスト/検証用途。必要に応じて自身の大容量ファイルを使用してもOK

---

## 補足・注釈

- **.envファイルはgit管理外**
- `.env-example`で最低限必要な変数を提示
- バグや素朴な検証用途を想定、商用等の厳格運用は非推奨
- `poetry`前提で動作確認済み

---

## ライセンス

MIT

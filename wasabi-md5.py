#!/usr/bin/env python3
import os
import base64
import hashlib
import uuid
from botocore.exceptions import ClientError
import boto3
import dotenv

dotenv.load_dotenv()

# ===== ユーザー設定（環境変数から取得）=====
ENDPOINT = os.environ.get("WASABI_ENDPOINT")           # 例: https://s3.ap-northeast-1.wasabisys.com
REGION   = os.environ.get("WASABI_REGION", "us-east-1")
AK       = os.environ.get("WASABI_ACCESS_KEY_ID")
SK       = os.environ.get("WASABI_SECRET_ACCESS_KEY")
BUCKET   = os.environ.get("WASABI_BUCKET")

if not all([ENDPOINT, AK, SK, BUCKET]):
    raise SystemExit("環境変数 WASABI_ENDPOINT/WASABI_ACCESS_KEY_ID/WASABI_SECRET_ACCESS_KEY/WASABI_BUCKET を設定してください。")

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    region_name=REGION,
    aws_access_key_id=AK,
    aws_secret_access_key=SK,
)

def md5_b64(data: bytes) -> str:
    """MD5 を計算して base64 文字列で返す（Content-MD5 ヘッダー用）。"""
    return base64.b64encode(hashlib.md5(data).digest()).decode("ascii")

def head_etag(bucket: str, key: str) -> str:
    """ETag を取得（単一パートアップロード時は MD5 と一致）。"""
    r = s3.head_object(Bucket=bucket, Key=key)
    return r["ETag"].strip('"')

def put_with_content_md5(bucket: str, key: str, body: bytes, content_md5_b64: str):
    """Content-MD5 を明示してアップロード。サーバ側で照合される。"""
    return s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentMD5=content_md5_b64,  # ここがポイント
    )

def demo_normal_and_abnormal():
    # 適当なテストデータを用意
    data = b"wasabi-md5-verify-demo:" + os.urandom(2048)
    correct_md5_b64 = md5_b64(data)

    print("== 正常系: Content-MD5 が正しい場合 ==")
    key_ok = f"md5-verify-demo/{uuid.uuid4()}.bin"
    resp = put_with_content_md5(BUCKET, key_ok, data, correct_md5_b64)
    etag = head_etag(BUCKET, key_ok)
    print(f"  -> put_object 成功。Key={key_ok}")
    print(f"  -> ETag={etag}")

    print("\n== 異常系: Content-MD5 が誤っている場合 ==")
    key_ng = f"md5-verify-demo/{uuid.uuid4()}-bad.bin"
    # 誤った MD5 を敢えて送る（データは同じ、ヘッダーだけ不一致）
    wrong_md5_b64 = base64.b64encode(b"\x00" * 16).decode("ascii")
    try:
        put_with_content_md5(BUCKET, key_ng, data, wrong_md5_b64)
        print("  !! 予期せず成功しました（想定外）")
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        msg  = e.response.get("Error", {}).get("Message")
        print(f"  -> 予想どおり失敗。ErrorCode={code}, Message={msg}")
        print("     ※ BadDigest であれば、受信データと指定 MD5 が不一致＝ベリファイ失敗を示します。")

if __name__ == "__main__":
    demo_normal_and_abnormal()

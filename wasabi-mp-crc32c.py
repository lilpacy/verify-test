#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, uuid, base64, struct
from typing import List, Tuple
from botocore.exceptions import ClientError
import boto3, google_crc32c
import dotenv

dotenv.load_dotenv()

ENDPOINT = os.environ.get("WASABI_ENDPOINT")
REGION   = os.environ.get("WASABI_REGION", "us-east-1")
AK       = os.environ.get("WASABI_ACCESS_KEY_ID")
SK       = os.environ.get("WASABI_SECRET_ACCESS_KEY")
BUCKET   = os.environ.get("WASABI_BUCKET")
if not all([ENDPOINT, AK, SK, BUCKET]):
    raise SystemExit("環境変数 WASABI_* を設定してください。")

s3 = boto3.client("s3",
    endpoint_url=ENDPOINT, region_name=REGION,
    aws_access_key_id=AK, aws_secret_access_key=SK)

def crc32c_b64(b: bytes) -> str:
    val = google_crc32c.value(b)
    return base64.b64encode(struct.pack(">I", val)).decode("ascii")

def read_in_parts(path: str, part_size: int):
    with open(path, "rb") as f:
        while True:
            chunk = f.read(part_size)
            if not chunk: break
            yield chunk

def abort(upload_id: str, key: str):
    try: s3.abort_multipart_upload(Bucket=BUCKET, Key=key, UploadId=upload_id)
    except Exception: pass

def multipart_upload_crc32c(file_path: str, key: str,
                            part_size: int = 8*1024*1024,
                            bad_part_index: int | None = None) -> Tuple[bool, str]:
    # 1) 開始（CRC32Cを宣言）
    init = s3.create_multipart_upload(Bucket=BUCKET, Key=key, ChecksumAlgorithm="CRC32C")
    upload_id = init["UploadId"]
    print(f"[init] UploadId={upload_id}")

    completed_parts: List[dict] = []
    try:
        part_no = 0
        for chunk in read_in_parts(file_path, part_size):
            part_no += 1
            checksum_b64 = crc32c_b64(chunk)

            # 異常系: 指定パートだけ誤CRC32Cを送る
            if bad_part_index is not None and part_no == bad_part_index:
                wrong_int = google_crc32c.value(chunk) ^ 0xFFFFFFFF
                send_checksum = base64.b64encode(struct.pack(">I", wrong_int)).decode("ascii")
                print(f"[part {part_no}] sending WRONG CRC32C (negative test)")
            else:
                send_checksum = checksum_b64

            try:
                resp = s3.upload_part(
                    Bucket=BUCKET, Key=key, UploadId=upload_id,
                    PartNumber=part_no, Body=chunk,
                    # 受信側が照合するパートのCRC32C（Base64）
                    ChecksumCRC32C=send_checksum
                )
                # ✅ 完了時に必要：正しいパートの CRC32C を Parts[] に含める
                completed_parts.append({
                    "ETag": resp["ETag"],
                    "PartNumber": part_no,
                    "ChecksumCRC32C": checksum_b64  # ←アップロード時に用いたパートの正しい値
                })
                print(f"[part {part_no}] ok ETag={resp['ETag']}")
            except ClientError as e:
                print(f"[part {part_no}] FAILED: {e.response.get('Error', {}).get('Code')} "
                      f"{e.response.get('Error', {}).get('Message')}")
                abort(upload_id, key)
                return (False, upload_id)

        # 3) 完了（各パートの ChecksumCRC32C を含めるのが必須）
        comp = s3.complete_multipart_upload(
            Bucket=BUCKET, Key=key, UploadId=upload_id,
            MultipartUpload={"Parts": completed_parts}
        )
        print(f"[complete] ETag={comp.get('ETag')} Location={comp.get('Location')}")
        try:
            head = s3.head_object(Bucket=BUCKET, Key=key)
            print(f"[head] ChecksumCRC32C={head.get('ChecksumCRC32C')} "
                  f"ChecksumType={head.get('ChecksumType')}")
        except Exception:
            pass
        return (True, comp.get("ETag", upload_id))

    except Exception as e:
        print(f"[fatal] {e}")
        abort(upload_id, key)
        return (False, upload_id)

def main():
    if len(sys.argv) < 2:
        print("使い方: python3 wasabi-mp-crc32c-fixed.py /path/to/largefile [part_size_mib]")
        sys.exit(2)
    path = sys.argv[1]
    part_size_mib = int(sys.argv[2]) if len(sys.argv) >= 3 else 8
    part_size = max(part_size_mib, 5) * 1024 * 1024

    # 正常系
    key_ok = f"mp-crc32c-demo/{uuid.uuid4()}.bin"
    print("== 正常系 ==")
    ok, _ = multipart_upload_crc32c(path, key_ok, part_size=part_size, bad_part_index=None)
    print(f"結果: {'成功' if ok else '失敗'} Key={key_ok}")

    # 異常系（2パート目だけ誤CRC32C）
    key_ng = f"mp-crc32c-demo/{uuid.uuid4()}-bad.bin"
    print("\n== 異常系（第2パートに誤CRC32C）==")
    ng, _ = multipart_upload_crc32c(path, key_ng, part_size=part_size, bad_part_index=2)
    print(f"結果: {'成功(想定外)' if ng else '失敗(想定どおり)'} Key={key_ng}")

if __name__ == "__main__":
    main()

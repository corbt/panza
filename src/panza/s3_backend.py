import pickle
import aioboto3
from botocore.exceptions import ClientError
from typing import Tuple, Any, Optional
from .cache import CacheBackend, Cache


class S3Backend(CacheBackend):
    def __init__(
        self,
        bucket_and_prefix: str,
        auto_create_bucket: bool = True,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: Optional[str] = None,
        endpoint_url: Optional[str] = None,
    ):
        # Parse the bucket and prefix; expected format: "bucket/path-prefix"
        parts = bucket_and_prefix.split("/", 1)
        self.bucket = parts[0]
        self.prefix = parts[1] if len(parts) > 1 else ""
        self.prefix = self.prefix.rstrip("/")
        self.auto_create_bucket = auto_create_bucket
        self.region = region_name if region_name else "us-east-1"
        self.endpoint_url = endpoint_url
        self.session = aioboto3.Session(
            region_name=self.region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

    async def setup(self) -> None:
        async with self.session.client("s3", endpoint_url=self.endpoint_url) as s3:
            try:
                await s3.head_bucket(Bucket=self.bucket)
            except ClientError as e:
                if self.auto_create_bucket:
                    if self.region == "us-east-1":
                        await s3.create_bucket(Bucket=self.bucket)
                    else:
                        await s3.create_bucket(
                            Bucket=self.bucket,
                            CreateBucketConfiguration={
                                "LocationConstraint": self.region
                            },
                        )
                else:
                    raise e

    async def get(self, fn_id: str, arg_hash: str) -> Tuple[bool, Any]:
        base_key = (
            f"{self.prefix}/{fn_id}/{arg_hash}/"
            if self.prefix
            else f"{fn_id}/{arg_hash}/"
        )
        async with self.session.client("s3", endpoint_url=self.endpoint_url) as s3:
            response = await s3.list_objects_v2(Bucket=self.bucket, Prefix=base_key)
            objects = response.get("Contents", [])
            if not objects:
                return False, None

            def extract_chunk_index(key: str) -> int:
                try:
                    return int(key.rsplit("/", 1)[-1])
                except Exception:
                    return 0

            objects.sort(key=lambda obj: extract_chunk_index(obj["Key"]))
            chunks = []
            for obj in objects:
                key = obj["Key"]
                response_obj = await s3.get_object(Bucket=self.bucket, Key=key)
                chunk = await response_obj["Body"].read()
                chunks.append(chunk)

        pickled_data = b"".join(chunks)
        result = pickle.loads(pickled_data)
        return True, result

    async def set(self, fn_id: str, arg_hash: str, result: Any) -> None:
        pickled_data = pickle.dumps(result)
        chunk_size = 1024 * 1024 * 1024  # 1GB chunks
        is_chunked = len(pickled_data) > chunk_size
        base_key = (
            f"{self.prefix}/{fn_id}/{arg_hash}/"
            if self.prefix
            else f"{fn_id}/{arg_hash}/"
        )

        # Remove any existing objects for this cache entry.
        async with self.session.client("s3", endpoint_url=self.endpoint_url) as s3:
            if not is_chunked:
                key = base_key + "0"
                await s3.put_object(Bucket=self.bucket, Key=key, Body=pickled_data)
            else:
                chunks = [
                    pickled_data[i : i + chunk_size]
                    for i in range(0, len(pickled_data), chunk_size)
                ]
                for i, chunk in enumerate(chunks):
                    key = base_key + str(i)
                    await s3.put_object(Bucket=self.bucket, Key=key, Body=chunk)

    async def delete(self, fn_id: str, arg_hash: str) -> None:
        base_key = (
            f"{self.prefix}/{fn_id}/{arg_hash}/"
            if self.prefix
            else f"{fn_id}/{arg_hash}/"
        )
        async with self.session.client("s3", endpoint_url=self.endpoint_url) as s3:
            response = await s3.list_objects_v2(Bucket=self.bucket, Prefix=base_key)
            objects = response.get("Contents", [])
            if not objects:
                return
            keys = [{"Key": obj["Key"]} for obj in objects]
            await s3.delete_objects(Bucket=self.bucket, Delete={"Objects": keys})

    async def delete_all(self) -> None:
        base_key = f"{self.prefix}/" if self.prefix else ""
        async with self.session.client("s3", endpoint_url=self.endpoint_url) as s3:
            response = await s3.list_objects_v2(Bucket=self.bucket, Prefix=base_key)
            objects = response.get("Contents", [])
            if not objects:
                return
            keys = [{"Key": obj["Key"]} for obj in objects]
            await s3.delete_objects(Bucket=self.bucket, Delete={"Objects": keys})

    async def delete_by_fn_id(self, fn_id: str) -> None:
        base_key = f"{self.prefix}/{fn_id}/" if self.prefix else f"{fn_id}/"
        async with self.session.client("s3", endpoint_url=self.endpoint_url) as s3:
            response = await s3.list_objects_v2(Bucket=self.bucket, Prefix=base_key)
            objects = response.get("Contents", [])
            if not objects:
                return
            keys = [{"Key": obj["Key"]} for obj in objects]
            await s3.delete_objects(Bucket=self.bucket, Delete={"Objects": keys})


class S3Cache(Cache):
    def __init__(
        self,
        bucket_and_prefix: str,
        auto_create_bucket: bool = True,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: Optional[str] = None,
        endpoint_url: Optional[str] = None,
    ):
        backend = S3Backend(
            bucket_and_prefix,
            auto_create_bucket=auto_create_bucket,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
            endpoint_url=endpoint_url,
        )
        super().__init__(backend)

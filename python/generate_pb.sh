#!/usr/bin/env bash
# 从共享 udf.proto 生成 Python 消息桩（udf_pb2.py → dojao_udf/）
# 数据面用 pyarrow.flight（自带 Flight/gRPC），不再生成 gRPC 服务桩。
# 用法：先 pip install -r requirements.txt，再 bash generate_pb.sh
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
PROTO="${HERE}/../dojao-core/src/main/proto/udf.proto"

python -m grpc_tools.protoc \
  -I "$(dirname "$PROTO")" \
  --python_out="${HERE}/dojao_udf" \
  "$PROTO"

echo "已生成: ${HERE}/dojao_udf/udf_pb2.py"

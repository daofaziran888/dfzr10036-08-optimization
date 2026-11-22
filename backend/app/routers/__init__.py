"""业务路由汇总：合并批次交接等固定路径优先注册，通用资源路由兜底。"""
from __future__ import annotations

from app.routers import handoff, resource

# handoff 的 /api/compaction-handoff/... 是固定路径，必须排在 /api/{module} 之前，
# 否则会被通用资源路由的动态段抢先匹配。
ROUTERS = [handoff, resource]

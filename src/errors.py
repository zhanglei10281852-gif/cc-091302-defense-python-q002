"""领域异常定义。"""


class DomainError(Exception):
    """领域错误基类。"""


class NotFoundError(DomainError):
    """实体不存在。"""


class ConflictError(DomainError):
    """状态或版本冲突（重复审批、过期版本、非法状态迁移等）。"""


class IdempotencyConflictError(ConflictError):
    """同一幂等键携带了不同的请求载荷。"""


class ValidationError(DomainError):
    """输入校验失败。"""

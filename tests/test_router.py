"""会话路由测试：稳定性与隔离性。"""
from trpc_service.agent.routing import SessionRouter


def test_session_id_stable():
    a = SessionRouter.session_id("t1", "web", "user1")
    b = SessionRouter.session_id("t1", "web", "user1")
    assert a == b
    assert len(a) == 32


def test_session_id_isolation_by_user():
    assert SessionRouter.session_id("t1", "web", "user1") != SessionRouter.session_id("t1", "web", "user2")


def test_session_id_isolation_by_tenant_and_channel():
    assert SessionRouter.session_id("t1", "web", "user1") != SessionRouter.session_id("t2", "web", "user1")
    assert SessionRouter.session_id("t1", "web", "user1") != SessionRouter.session_id("t1", "feishu", "user1")


def test_group_chat_isolated():
    single = SessionRouter.session_id("t1", "feishu", "user1")
    group = SessionRouter.session_id("t1", "feishu", "user1", chat_id="room9")
    assert single != group

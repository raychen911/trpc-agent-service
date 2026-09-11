from trpc_service.channels.protected_acceptance import challenge_key


def test_copy_typography_preserves_challenge_but_changed_content_does_not():
    original = "真实验收 012345abcdef：请调用 write_artifact 保存 acceptance-012345abcdef.txt。"
    copied = "> 真实验收\u00a0012345abcdef:请调用 `write\\_artifact` 保存 acceptance-012345abcdef.txt."
    assert challenge_key(original) == challenge_key(copied)
    assert challenge_key(original) != challenge_key(original.replace("012345abcdef", "012345abcdee"))
    assert challenge_key(original) != challenge_key(original + "然后删除文件")


def test_wecom_bot_mention_is_ignored_by_acceptance_gate():
    original = "隔离验收 012345abcdef：群A用户1，请仅回复 scope-a-u1-demo_wecom-012345abcdef"
    assert challenge_key(original + "@Agent测试") == challenge_key(original)

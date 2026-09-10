from trpc_service.channels.base import ChannelCapabilities
from trpc_service.channels.simple import SimpleJsonChannelAdapter


class WebAdapter(SimpleJsonChannelAdapter):
    channel_name = "web"
    capabilities = ChannelCapabilities(
        max_text_length=100_000,
        supports_media=True,
        supports_cards=True,
        supports_revoke=True,
    )
    message_id_fields = ("message_id", "request_id", "id")
    user_id_fields = ("from_user_id", "user_id", "uid", "account_id")
    group_id_fields = ("chat_id", "conversation_id", "group_id", "session_id")
    text_fields = ("text", "message", "prompt")

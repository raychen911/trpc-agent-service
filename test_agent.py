import asyncio
import uuid

from trpc_agent_sdk.agents import LlmAgent
from trpc_agent_sdk.models import OpenAIModel
from trpc_agent_sdk.runners import Runner
from trpc_agent_sdk.sessions import InMemorySessionService
from trpc_agent_sdk.types import Content, Part


async def main():
    # 1. 创建模型
    model = OpenAIModel(
        model_name="gpt-4o",
        api_key="你的API Key",
        base_url="https://api.openai.com/v1",
    )

    # 2. 创建Agent
    agent = LlmAgent(
        name="assistant",
        description="一个简单的助手",
        model=model,
        instruction="你是一个有帮助的助手。",
    )

    # 3. 创建Session服务
    session_service = InMemorySessionService()

    # 4. 创建Runner
    runner = Runner(
        app_name="demo",
        agent=agent,
        session_service=session_service,
    )

    # 5. 对话
    session_id = str(uuid.uuid4())
    user_id = "test_user"

    # 创建session
    await session_service.create_session(
        app_name="demo",
        user_id=user_id,
        session_id=session_id,
    )

    # 发送消息
    user_content = Content(parts=[Part.from_text(text="你好，你是谁？")])

    print("用户: 你好，你是谁？")
    print("AI: ", end="", flush=True)

    async for event in runner.run_async(
        user_id=user_id,
        session_id=session_id,
        new_message=user_content,
    ):
        if event.content and event.content.parts:
            for part in event.content.parts:
                if part.text and event.partial:
                    print(part.text, end="", flush=True)

    print()


if __name__ == "__main__":
    asyncio.run(main())

from langchain.tools import Tool
from langchain.chat_models import ChatOpenAI
from langchain.agents import initialize_agent
from langchain.callbacks.base import BaseCallbackHandler
from typing import Any, Dict
import time
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# 1️⃣ Define a simple tool (e.g., get_weather)
def get_weather(city: str) -> str:
    # In real use, call an API here
    time.sleep(5)
    return f"It's 22°C and sunny in {city}."

weather_tool = Tool(
    name="get_weather",
    func=lambda city: get_weather(city),
    description="Use this tool to get the current weather for a given city. Input should be a city name."
)

# 2️⃣ Custom callback handler to manage streaming and tool events
class StreamAndToolCallback(BaseCallbackHandler):
    def on_llm_new_token(self, token: str, **kwargs: Any) -> None:
        # Stream each token from the LLM
        print(token, end="", flush=True)

    def on_tool_start(
        self, serialized: Dict[str, Any], input_str: str, **kwargs: Any
    ) -> None:
        # Called when a tool is about to be executed
        print(f"\n[Calling tool '{serialized['name']}' with input: {input_str}]\n")

    def on_tool_end(
        self, output: str, **kwargs: Any
    ) -> None:
        # Called when the tool returns its result
        print(f"\n[Tool output: {output}]\n\nResuming LLM streaming... ", end="", flush=True)

# 3️⃣ Initialize the streaming LLM and agent
callback_handler = StreamAndToolCallback()

llm = ChatOpenAI(
    streaming=True,
    callbacks=[callback_handler],
    temperature=0.0
)

agent = initialize_agent(
    tools=[weather_tool],
    llm=llm,
    agent="zero-shot-react-description",
    callbacks=[callback_handler],
    verbose=True
)

# 4️⃣ Run the agent: it will stream tokens, pause for tool, then resume
prompt = "What's the weather in Paris?"
print(f"User: {prompt}\nAssistant:", end=" ")
agent.run(prompt)

import os
import sys
from pathlib import Path

from crewai import Crew, Process
from crewai_tools import MCPServerAdapter
from mcp import StdioServerParameters

from config.agents import build_llm, build_personalization_agent, build_merchandising_agent
from tasks.tasks import create_merchandising_task, create_personalization_task
from tools.kafka_tools import build_merchandising_recommendation, write_recommendation
from observability import new_run_id, set_trace_output, trace_context, trace_span


ROOT = Path(__file__).resolve().parent

_PERSONA_TOOL_NAMES = (
    "get_live_user_profile",
    "get_price_qualified_catalog",
    "search_similar_products",
)

_MERCH_TOOL_NAMES = (
    "get_live_product_catalog",
)


def _mcp_server_params() -> StdioServerParameters:
    return StdioServerParameters(
        command=sys.executable,
        args=[str(ROOT / "mcp_server.py")],
        env=dict(os.environ),
    )


def run_personalization(userid: int) -> str:
    run_id = new_run_id("personalization")
    with trace_context(
        "personalization-agent-run",
        run_id=run_id,
        user_id=userid,
        metadata={"agent_type": "personalization"},
        input={"userid": userid},
    ):
        llm = build_llm()
        with MCPServerAdapter(_mcp_server_params(), *_PERSONA_TOOL_NAMES) as tools:
            agent = build_personalization_agent(tools, llm)
            crew = Crew(
                agents=[agent],
                tasks=[create_personalization_task(userid, agent)],
                process=Process.sequential,
                verbose=True,
            )
            result = crew.kickoff()

        set_trace_output(str(result))
        trace_span("crew-final-answer", output=str(result), metadata={"agent_type": "personalization"})

        write_recommendation(
            userid=userid,
            recommendation=str(result),
            agent_type="personalization",
            run_id=run_id,
        )
        return str(result)


def run_merchandising() -> str:
    run_id = new_run_id("merchandising")
    with trace_context(
        "merchandising-agent-run",
        run_id=run_id,
        user_id=0,
        metadata={"agent_type": "merchandising"},
    ):
        result = build_merchandising_recommendation()
        set_trace_output(str(result))
        trace_span("crew-final-answer", output=str(result), metadata={"agent_type": "merchandising"})

        write_recommendation(
            userid=0,
            recommendation=str(result),
            agent_type="merchandising",
            run_id=run_id,
        )
        return str(result)


def run_full_crew(userid: int) -> str:
    run_id = new_run_id("full-crew")
    with trace_context(
        "full-crew-agent-run",
        run_id=run_id,
        user_id=userid,
        metadata={"agent_type": "full_crew"},
        input={"userid": userid},
    ):
        llm = build_llm()
        with MCPServerAdapter(_mcp_server_params()) as all_tools:
            tool_map = {t.name: t for t in all_tools}
            persona_tools = [tool_map[n] for n in _PERSONA_TOOL_NAMES if n in tool_map]
            merch_tools = [tool_map[n] for n in _MERCH_TOOL_NAMES if n in tool_map]

            pa = build_personalization_agent(persona_tools, llm)
            ma = build_merchandising_agent(merch_tools, llm)
            crew = Crew(
                agents=[pa, ma],
                tasks=[
                    create_personalization_task(userid, pa),
                    create_merchandising_task(ma),
                ],
                process=Process.sequential,
                verbose=True,
            )
            result = crew.kickoff()

        set_trace_output(str(result))
        trace_span("crew-final-answer", output=str(result), metadata={"agent_type": "full_crew"})

        write_recommendation(
            userid=userid,
            recommendation=str(result),
            agent_type="full_crew",
            run_id=run_id,
        )
        return str(result)

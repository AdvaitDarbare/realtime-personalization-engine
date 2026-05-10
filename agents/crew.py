from crewai import Crew, Process
from config.agents import merchandising_agent, personalization_agent
from tasks.tasks import create_merchandising_task, create_personalization_task
from tools.kafka_tools import build_merchandising_recommendation, write_recommendation
from observability import new_run_id, set_trace_output, trace_context, trace_span


def run_personalization(userid: int) -> str:
    run_id = new_run_id("personalization")
    with trace_context(
        "personalization-agent-run",
        run_id=run_id,
        user_id=userid,
        metadata={"agent_type": "personalization"},
        input={"userid": userid},
    ):
        crew = Crew(
            agents=[personalization_agent],
            tasks=[create_personalization_task(userid)],
            process=Process.sequential,
            verbose=True
        )
        result = crew.kickoff()
        set_trace_output(str(result))
        trace_span("crew-final-answer", output=str(result), metadata={"agent_type": "personalization"})

        # Write result back to Kafka
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

        # Write result back to Kafka
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
        crew = Crew(
            agents=[personalization_agent, merchandising_agent],
            tasks=[
                create_personalization_task(userid),
                create_merchandising_task()
            ],
            process=Process.sequential,
            verbose=True
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

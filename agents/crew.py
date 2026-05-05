from crewai import Crew, Process
from config.agents import personalization_agent, merchandising_agent
from tasks.tasks import create_personalization_task, create_merchandising_task
from tools.kafka_tools import write_recommendation


def run_personalization(userid: int) -> str:
    crew = Crew(
        agents=[personalization_agent],
        tasks=[create_personalization_task(userid)],
        process=Process.sequential,
        verbose=True
    )
    result = crew.kickoff()
    
    # Write result back to Kafka
    write_recommendation(
        userid=userid,
        recommendation=str(result),
        agent_type="personalization"
    )
    
    return str(result)


def run_merchandising() -> str:
    crew = Crew(
        agents=[merchandising_agent],
        tasks=[create_merchandising_task()],
        process=Process.sequential,
        verbose=True
    )
    result = crew.kickoff()
    
    # Write result back to Kafka
    write_recommendation(
        userid=0,
        recommendation=str(result),
        agent_type="merchandising"
    )
    
    return str(result)


def run_full_crew(userid: int) -> str:
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
    
    write_recommendation(
        userid=userid,
        recommendation=str(result),
        agent_type="full_crew"
    )
    
    return str(result)
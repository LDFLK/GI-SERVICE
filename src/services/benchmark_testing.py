import time
from src.services import IncomingServiceAttributes
import asyncio

class BenchmarkTestingService:
    def __init__(self, config: dict):
        self.config = config

    
    async def benchmarkTestingAPI(self, session, presidentId, selectedDate, statService: IncomingServiceAttributes):
        
        # eg item -> single portfolio object with id, appointedMinisters -> list of people for portfolio with ids
        async def enrich_portfolio_item(portfolio, appointedMinisters):
            portfolio_task = statService.get_node_data_by_id(
                entityId=portfolio.get('relatedEntityId'),
                session=session
            )
            
            minister_tasks = [
                statService.get_node_data_by_id(
                    entityId=m.get("relatedEntityId"),
                    session=session
                )
                for  m in appointedMinisters
            ]
            
            results = await asyncio.gather(portfolio_task, *minister_tasks, return_exceptions=True)
            
            portfolio_data = results[0]
            minister_data_list = results[1:]
            
            portfolio["decodedName"] = statService.decode_protobuf_attribute_name(
                portfolio_data.get("name", "")
            )
            
            portfolio["ministers"] = [
                {
                    "ministerId": minister_data.get("id", ""),
                    "ministerName": statService.decode_protobuf_attribute_name(
                        minister_data.get("name", "")
                    )
                }
                for minister_data, minister in zip(minister_data_list, appointedMinisters)
            ]

        url = f"{self.config["BASE_URL_QUERY"]}/v1/entities/{presidentId}/relations"
        headers = {"Content-Type": "application/json"}
        payload = {
            "name": "AS_MINISTER",
            "activeAt": f"{selectedDate}T00:00:00Z"
        }
        
        global_start_time = time.perf_counter()
        
        activePortfolioList = [] # portfolio ids
        
        async with session.post(url, headers=headers, json=payload) as response:
            response.raise_for_status()
            activePortfolioList = await response.json()
        
        # get ids of people for each minister (in parallel)
        tasksforMinistersAppointed = [statService.fetch_relation(id=portfolio.get('relatedEntityId'), relationName="AS_APPOINTED", activeAt=f"{selectedDate}T00:00:00Z", session=session) for portfolio in activePortfolioList]                  
        appointedList = await asyncio.gather(*tasksforMinistersAppointed, return_exceptions=True)

        await asyncio.gather(*[
            enrich_portfolio_item(activePortfolioList[i], appointedList[i])
            for i in range(len(activePortfolioList))
        ])

        global_end_time = time.perf_counter();
        global_elapsed_time = global_end_time - global_start_time 
        print(f"Global Elapsed Time: {global_elapsed_time:.4f} seconds")
        print(len(activePortfolioList))
        return activePortfolioList

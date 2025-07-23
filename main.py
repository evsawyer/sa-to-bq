from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional
import logging
from datetime import datetime, timedelta
import os
from StackAdaptToBigQueryPipeline import StackAdaptToBigQueryPipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="StackAdapt to BigQuery API",
    description="API for syncing StackAdapt ads data to BigQuery",
    version="1.0.0"
)

class SyncRequest(BaseModel):
    days_back: Optional[int] = 30
    use_bulk: Optional[bool] = True
    dataset_id: Optional[str] = "raw_ads"
    project_id: Optional[str] = None  # Will use default from BigQueryClient if None

class SyncResponse(BaseModel):
    status: str
    message: str
    records_synced: Optional[int] = None
    execution_time_seconds: Optional[float] = None
    timestamp: str
    date_range: Optional[dict] = None

@app.get("/")
async def root():
    """Health check endpoint"""
    return {"message": "StackAdapt to BigQuery API is running"}

@app.get("/health")
async def health_check():
    """Health check endpoint for Cloud Run"""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

@app.post("/sync-ads-insights", response_model=SyncResponse)
async def sync_ads_insights(request: Optional[SyncRequest] = None, background_tasks: BackgroundTasks = None):
    """
    Sync StackAdapt ads performance data to BigQuery
    
    Args:
        request: Optional SyncRequest with configuration parameters (uses defaults if not provided)
        
    Returns:
        SyncResponse with sync results
    """
    start_time = datetime.utcnow()
    
    # Use defaults if no request body provided
    if request is None:
        request = SyncRequest()
    
    try:
        # Calculate date range
        date_to = datetime.now().strftime("%Y-%m-%d")
        date_from = (datetime.now() - timedelta(days=request.days_back)).strftime("%Y-%m-%d")
        
        logger.info(f"Starting StackAdapt sync with {request.days_back} days back")
        logger.info(f"Date range: {date_from} to {date_to}")
        logger.info(f"Using bulk mode: {request.use_bulk}")
        
        # Initialize pipeline
        pipeline = StackAdaptToBigQueryPipeline(
            dataset_id=request.dataset_id,
            project_id=request.project_id
        )
        
        # Sync ads performance data to temp table
        logger.info(f"Syncing ads performance data for last {request.days_back} days...")
        count = pipeline.sync_ads_performance(
            use_bulk=request.use_bulk,
            days_back=request.days_back
        )
        logger.info(f"Synced {count} performance records to temp table")
        
        # Merge temp data into main table if we got records
        merge_success = False
        if count > 0:
            logger.info("Merging temp data into main stackadapt_ads table...")
            merge_success = pipeline.merge_ads_performance()
            if merge_success:
                logger.info("Successfully merged data into main table")
            else:
                logger.warning("Failed to merge data into main table, but temp table has data")
        else:
            logger.info("No performance data to merge")
        
        end_time = datetime.utcnow()
        execution_time = (end_time - start_time).total_seconds()
        
        return SyncResponse(
            status="success",
            message=f"Successfully synced {count} records{' and merged to main table' if merge_success else ' to temp table'}",
            records_synced=count,
            execution_time_seconds=execution_time,
            timestamp=end_time.isoformat(),
            date_range={
                "from": date_from,
                "to": date_to,
                "days_back": request.days_back
            }
        )
        
    except Exception as e:
        error_msg = f"Error syncing StackAdapt data: {str(e)}"
        logger.error(error_msg)
        
        end_time = datetime.utcnow()
        execution_time = (end_time - start_time).total_seconds()
        
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": error_msg,
                "execution_time_seconds": execution_time,
                "timestamp": end_time.isoformat()
            }
        )

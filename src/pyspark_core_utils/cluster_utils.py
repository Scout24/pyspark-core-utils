import json
import os
import time
import boto3
import logging

logger = logging.getLogger(__name__)


def get_current_cluster_id():
    """Get the current EMR cluster ID from the job-flow.json file.
    
    Returns:
        str or None: The cluster ID string or None if not found.
    """
    path = "/mnt/var/lib/info/job-flow.json"
    logger.debug(f"Looking for cluster ID in {path}")
    
    try:
        if not os.path.exists(path):
            logger.warning(f"Cluster info file not found at {path}")
            return None
            
        with open(path, "r") as f:
            content = f.read()
            
        data = json.loads(content)
        cluster_id = data.get("jobFlowId")
        
        if cluster_id:
            logger.info(f"Found cluster ID: {cluster_id}")
        else:
            logger.warning("jobFlowId not found in cluster info file")
            
        return cluster_id
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON from {path}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error getting current cluster ID: {e}")
        return None

def _retry_boto3_call(func, *args, max_retries=3, retry_delay=2, **kwargs):
    """Retry a boto3 call with simple counter-based retry logic.
    
    Args:
        func: The boto3 function to call
        *args: Positional arguments to pass to the function
        max_retries: Maximum number of retries (default: 3)
        retry_delay: Delay in seconds between retries (default: 2)
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        The result of the boto3 call
        
    Raises:
        Exception: If all retries are exhausted
    """
    last_exception = None
    for attempt in range(1, max_retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exception = e
            if attempt < max_retries:
                logger.warning(f"Attempt {attempt}/{max_retries} failed: {e}. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                logger.error(f"All {max_retries} attempts failed. Last error: {e}")
    raise last_exception


def is_glue_metastore(cluster_id):
    """Check if the given EMR cluster uses Glue metastore.
    
    Args:
        cluster_id: The EMR cluster ID to check.
        
    Returns:
        bool: True if cluster uses Glue metastore, False otherwise.
    """
    logger.info(f"Checking if cluster {cluster_id} uses Glue metastore")
    
    try:
        emr = boto3.client("emr")
        response = _retry_boto3_call(emr.describe_cluster, ClusterId=cluster_id)
        cluster = response["Cluster"]
        
        logger.debug(f"Successfully retrieved cluster configuration for {cluster_id}")
        
        configurations = cluster.get("Configurations", [])
        logger.debug(f"Found {len(configurations)} configuration entries")
        
        for config in configurations:
            if config.get("Classification") == "hive-site":
                logger.debug("Found hive-site configuration")
                properties = config.get("Properties", {})
                
                # Check if Glue metastore factory class is configured
                factory_class = properties.get("hive.metastore.client.factory.class")
                is_glue_factory = (
                    factory_class
                    == "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                )
                
                if is_glue_factory:
                    logger.debug("Cluster uses Glue metastore factory class")
                
                # Check if catalog ID is in valid set
                catalog_id = properties.get("hive.metastore.glue.catalogid")
                valid_catalog_ids = {"788441577080", "282050837221"}
                
                if catalog_id:
                    logger.debug(f"Found catalog ID: {catalog_id}")
                
                if is_glue_factory and catalog_id and catalog_id in valid_catalog_ids:
                    logger.info(f"Cluster {cluster_id} uses Glue metastore with catalog ID {catalog_id}")
                    return True
                elif is_glue_factory and catalog_id:
                    logger.warning(f"Cluster {cluster_id} uses Glue factory but catalog ID {catalog_id} is not in valid set")
                    
        logger.info(f"Cluster {cluster_id} does not use Glue metastore")
        return False
        
    except Exception as e:
        logger.error(f"Error checking if cluster {cluster_id} uses Glue metastore: {e}")
        return False

def cluster_uses_glue_metastore():
    """Check if the current cluster uses Glue metastore.
    
    Returns:
        bool: True if current cluster uses Glue metastore, False otherwise.
    """
    logger.info("Checking if current cluster uses Glue metastore")
    
    cluster_id = get_current_cluster_id()
    if not cluster_id:
        logger.warning("Could not determine cluster ID, assuming no Glue metastore")
        return False
    
    result = is_glue_metastore(cluster_id)
    logger.info(f"Current cluster uses Glue metastore: {result}")
    return result
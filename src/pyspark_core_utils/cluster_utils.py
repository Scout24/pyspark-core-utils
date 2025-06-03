import json
import os
import boto3

def get_current_cluster_id():
    """Get the current EMR cluster ID from the job-flow.json file.
    
    Returns:
        str or None: The cluster ID string or None if not found.
    """
    path = "/mnt/var/lib/info/job-flow.json"
    
    try:
        if not os.path.exists(path):
            return None
            
        with open(path, "r") as f:
            content = f.read()
            
        data = json.loads(content)
        return data.get("jobFlowId")
    except Exception as e:
        print(f"Error getting current cluster ID: {str(e)}")
        return None

def is_glue_metastore(cluster_id):
    """Check if the given EMR cluster uses Glue metastore.
    
    Args:
        cluster_id: The EMR cluster ID to check.
        
    Returns:
        bool: True if cluster uses Glue metastore, False otherwise.
    """
    try:
        emr = boto3.client("emr")
        response = emr.describe_cluster(ClusterId=cluster_id)
        cluster = response["Cluster"]
        
        configurations = cluster.get("Configurations", [])
        for config in configurations:
            if config.get("Classification") == "hive-site":
                properties = config.get("Properties", {})
                
                # Check if Glue metastore factory class is configured
                factory_class = properties.get("hive.metastore.client.factory.class")
                is_glue_factory = (
                    factory_class
                    == "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                )
                
                # Check if catalog ID is in valid set
                catalog_id = properties.get("hive.metastore.glue.catalogid")
                valid_catalog_ids = {"788441577080", "282050837221"}
                
                if is_glue_factory and catalog_id and catalog_id in valid_catalog_ids:
                    return True
                    
        return False
        
    except Exception as e:
        print(f"Error checking cluster {cluster_id}: {str(e)}")
        return False

def cluster_uses_glue_metastore():
    """Check if the current cluster uses Glue metastore.
    
    Returns:
        bool: True if current cluster uses Glue metastore, False otherwise.
    """
    cluster_id = get_current_cluster_id()
    if not cluster_id:
        return False
        
    return is_glue_metastore(cluster_id) 
import json
import xarray as xr
import s3fs
import pandas as pd

# DEFAULT_REST_ENDPOINT = 'https://geoglows.ecmwf.int/api/'
# DEFAULT_REST_ENDPOINT_VERSION = 'v2'  # 'v1, v2, latest'
ODP_CORE_S3_BUCKET_URI = 's3://geoglows-v2-retrospective'
ODP_RETROSPECTIVE_S3_BUCKET_URI = 's3://geoglows-v2-retrospective'
ODP_S3_BUCKET_REGION = 'us-west-2'

def _retrospective(reach_id: int, params: dict[str] = {}) -> pd.DataFrame:
    """
    Retrieves retrospective data for a specific reach ID.

    Parameters:
        reach_id (int): The ID of the reach.
        params (dict[str]): Optional parameters for filtering the data.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the retrospective data.
    """
    s3 = s3fs.S3FileSystem(anon=True, client_kwargs=dict(region_name=ODP_S3_BUCKET_REGION))
    s3store = s3fs.S3Map(root=f'{ODP_RETROSPECTIVE_S3_BUCKET_URI}/retrospective.zarr', s3=s3, check=False)
    df = (xr.open_zarr(s3store, consolidated=True, )
                       .sel(rivid=reach_id)
                       .to_pandas()
                       .reset_index()
                       .set_index('time')
                       .pivot(columns='rivid', values='Qout')
            )
    if params.get('end_date', False):
        df = df[df.index <= pd.to_datetime(params['end_date'])]
    if params.get('start_date', False):
        df = df[df.index >= pd.to_datetime(params['start_date'])]
    return df

def retrospective(path: list[str], event: dict[str]) -> dict:
    """
    Retrieves retrospective data for a specified reach ID.

    Args:
        path (list[str]): The path of the request URL.
        event (dict[str]): The event object containing the request parameters.

    Returns:
        dict: The response containing the status code and the data in the specified format.
    """
    if not len(path) > 2:
        return {'statesCode': 422,
                'body': f"Bad request: No reachID specified"}
    try:
        params = {}
        if event.get("queryStringParameters", False):
            params = event["queryStringParameters"]
        reach_id = int(path[2])
    except ValueError:
        return {'statesCode': 400,
                'body': f"Bad request: couldn't make {path[2]} into an integer"}
    
    df = _retrospective(reach_id, params)
    if params.get('format', False):
        if params['format'] == 'csv':
            output = df.to_csv(index=False)
        elif params['format'] == 'json':
            output = df.to_json(index=False)
        else:
            raise ValueError()
    else:
        output = df.to_csv(index=False)

    return {'statusCode': 200, 'body': json.dumps(output)}

def check_if_valid_request(event: dict) -> str or dict[str]: # type: ignore
    """
    Check if the request is valid based on the event data.

    Args:
        event (dict): The event data containing the request information.

    Returns:
        Union[str, dict[str]]: If the request is valid, returns the path extracted from the request.
                               If the request is invalid, returns a dictionary with the status code and error message.
    """
    try:
        http = event["requestContext"]["http"]
    
        path = http["path"].split('/')[1:]
        if path and not path[0] == 'v2':
                return {'statesCode': 422,
                        'body': f"No V2 in {path}"}
        return path
    except:
        return {'statesCode': 400,
                'body': f"Bad request"}

def daily_averages(path: list[str], event: dict[str]) -> dict[str]:
    """
    Calculate daily averages based on reach ID and query parameters.

    Args:
        path (list[str]): The path containing the reach ID.
        event (dict[str]): The event containing the query parameters.

    Returns:
        dict[str]: The response containing the status code and the calculated averages.
    """

    if not len(path) > 2:
        return {'statesCode': 422,
                'body': f"Bad request: No reachID specified"}
    try:
        params = {}
        if event["requestContext"].get("queryStringParameters", False):
            params = event["requestContext"]["queryStringParameters"]
        reach_id = int(path[2])
    except ValueError:
        return {'statesCode': 400,
                'body': f"Bad request: couldn't make {path[2]} into an integer"}
    
    df = _retrospective(reach_id, {})
    df = df.groupby(df.index.strftime('%m%d')).mean()
    if params.get('format', False):
        if params['format'] == 'csv':
            output = df.to_csv(index=False)
        elif params['format'] == 'json':
            output = df.to_json(index=False)
        else:
            raise ValueError()
    else:
        output = df.to_csv(index=False)

    return {'statusCode': 200, 'body': json.dumps(output)}

def monthly_averages(path: list[str], event: dict[str]) -> dict[str]:
    """
    Calculate the monthly averages based on the given reach ID and parameters.

    Args:
        path (list[str]): The path containing the reach ID.
        event (dict[str]): The event containing the request context.

    Returns:
        dict[str]: The response containing the status code and body.
    """

    if not len(path) > 2:
        return {'statesCode': 422,
                'body': f"Bad request: No reachID specified"}
    try:
        params = {}
        if event["requestContext"].get("queryStringParameters", False):
            params = event["requestContext"]["queryStringParameters"]
        reach_id = int(path[2])
    except ValueError:
        return {'statesCode': 400,
                'body': f"Bad request: couldn't make {path[2]} into an integer"}
    
    df = _retrospective(reach_id, {})
    df = df.groupby(df.index.strftime('%m')).mean()
    if params.get('format', False):
        if params['format'] == 'csv':
            output = df.to_csv(index=False)
        elif params['format'] == 'json':
            output = df.to_json(index=False)
        else:
            raise ValueError()
    else:
        output = df.to_csv(index=False)

    return {'statusCode': 200, 'body': json.dumps(output)}

def returnperiods(path: list[str], event: dict[str]) -> dict[str]:
    """
    Retrieves return periods data for a specific reach ID.

    Args:
        path (list[str]): The path containing the reach ID.
        event (dict[str]): The event containing the query string parameters.

    Returns:
        dict[str]: The response containing the status code and the data in the specified format.
    """

    if not len(path) > 2:
        return {'statesCode': 422,
                'body': f"Bad request: No reachID specified"}
    try:
        params = {}
        if event["requestContext"].get("queryStringParameters", False):
            params = event["requestContext"]["queryStringParameters"]
        reach_id = int(path[2])
    except ValueError:
        return {'statesCode': 400,
                'body': f"Bad request: couldn't make {path[2]} into an integer"}
    
    s3 = s3fs.S3FileSystem(anon=True, client_kwargs=dict(region_name=ODP_S3_BUCKET_REGION))
    s3store = s3fs.S3Map(root=f'{ODP_RETROSPECTIVE_S3_BUCKET_URI}/return-periods.zarr', s3=s3, check=False)
    df = xr.open_zarr(s3store).sel(rivid=reach_id).to_dataframe()
    if params.get('format', False):
        if params['format'] == 'csv':
            output = df.to_csv(index=False)
        elif params['format'] == 'json':
            output = df.to_json(index=False)
        else:
            raise ValueError()
    else:
        output = df.to_csv(index=False)

    return {'statusCode': 200, 'body': json.dumps(output)}

def lambda_handler(event, context):
    """
    Handles the Lambda function invocation.

    Args:
        event (dict): The event data passed to the Lambda function.
        context (object): The context object passed to the Lambda function.

    Returns:
        dict: The response data returned by the Lambda function.
    """

    try:
        path = check_if_valid_request(event)
        if not len(path) > 1:
            pass

        if path[1] == 'retrospective':
            return retrospective(path, event)
        elif path[1] == 'daily_averages':
            return daily_averages(path, event)
        elif path[1] == "monthly_averages":
            return monthly_averages(path, event)
        elif path[1] == "returnperiods":
            return returnperiods(path, event)
        
    except Exception as e:
            return {'statesCode': 400,
                    'body': f"Bad request: {e}"}
    return {
        'statusCode': 200,
        'body': json.dumps(event)
    }

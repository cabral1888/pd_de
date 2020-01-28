from datetime import datetime

def get_path_by_day(base_path, streaming_name):
    """
        Get path of file on datalake  considering 
        the following pattern: %Y/%m/%d/FILES

        Parameters
        ----------

        base_path: str
            The base path where the streaming_name
            folder will be created
        
        streaming_name: str
            The main folder that will contains the
            defined structure above
    """
    year = datetime.now().strftime("%Y")
    month = datetime.now().strftime("%m")
    day = datetime.now().strftime("%d")

    return "{}/{}/{}/{}/{}".format(base_path, streaming_name, year, month, day)
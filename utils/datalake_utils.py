from datetime import datetime

def get_path_by_day(base_path, streaming_name):
	year = datetime.now().strftime("%Y")
	month = datetime.now().strftime("%m")
	day = datetime.now().strftime("%d")

	return "{}/{}/{}/{}/{}".format(base_path, streaming_name, year, month, day)
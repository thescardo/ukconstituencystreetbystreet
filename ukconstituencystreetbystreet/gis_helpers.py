from typing import List

import geopandas as gpd
from shapely.geometry import LineString, MultiLineString
from shapely import ops


def combine_gpd_lines(input: List[LineString] | gpd.GeoSeries) -> LineString:
    input_list = []
    if isinstance(input, gpd.GeoSeries):
        input_list = list(input.array)
    else:
        input_list = input

    multi_line = MultiLineString(input_list)
    merged_line = ops.linemerge(multi_line)
    if isinstance(merged_line, LineString):
        return merged_line
    else:
        return multi_line
import numpy as np
import math
import jenkspy
from scipy.stats import percentileofscore

def get_natural_breaks(col_values, run_type) -> dict:
    nb_partition = get_natural_break_partition(run_type)
    natural_break_map = {}

    # Check if all values in col_values are the same
    unique_values = np.unique(col_values)
    if len(unique_values) == 1:
        # All values are the same, create a natural break map with 100% percentile
        # Since there is only one class, the break value is the unique value itself
        natural_break_map = {100.0: unique_values[0]}
    elif col_values.size > nb_partition + 1:
        if col_values.size > MAX_RECORDS_NATURAL_BREAK:
            size = math.floor(col_values.size * (MAX_RECORDS_NATURAL_BREAK / col_values.size))
            sample = np.random.choice(col_values, size=size)
            jenks_cut = jenkspy.jenks_breaks(sample, n_classes=nb_partition)
        else:
            jenks_cut = jenkspy.jenks_breaks(col_values, n_classes=nb_partition)
        
        percentiles = [percentileofscore(col_values, jbreak) for jbreak in jenks_cut]
        formatted_percentiles = list(np.around(np.array(percentiles), 2))
        natural_break_map = dict(zip(formatted_percentiles, jenks_cut))
    
    return natural_break_map

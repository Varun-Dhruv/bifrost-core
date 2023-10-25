import json
from datetime import datetime

import apache_beam as beam


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class MeanInterpolation(beam.DoFn):
    def __init__(self, interpolate_attributes):
        self.interpolate_attributes = interpolate_attributes

    def process(self, element):
        # Assuming element is a dictionary retrieved from MongoDB
        for attribute in self.interpolate_attributes:
            if attribute in element:
                data = element[attribute]
                if data is not None and hasattr(data, "__len__") and len(data) >= 3:
                    for i in range(len(data)):
                        if data[i] is None:
                            left, right = i - 1, i + 1
                            while left >= 0 and data[left] is None:
                                left -= 1
                            while right < len(data) and data[right] is None:
                                right += 1
                            if left < 0 or right == len(data):
                                # Cannot perform mean interpolation, leave the original value
                                continue
                            mean = (data[left] + data[right]) / 2.0
                            data[i] = mean

        yield element

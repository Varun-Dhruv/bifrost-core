import apache_beam as beam


def mongodb_extractor(pipeline, path):
    lines = pipeline | "ReadMyDatabase" >> beam.io.ReadFromMongoDB(
        uri=path["url"], db=path["database"], coll=path["collection"]
    )
    lines = lines | "RemoveObjectId" >> beam.Map(
        lambda line: {k: v for k, v in line.items() if k != "_id" or k != "__v"}
    )
    return lines

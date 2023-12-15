FROM apache/beam_python3.10_sdk:2.47.0

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt
# Perform any additional customizations if desired
# Set the entrypoint to Apache Beam SDK launcher.
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./app /code/app

ENTRYPOINT [""]
